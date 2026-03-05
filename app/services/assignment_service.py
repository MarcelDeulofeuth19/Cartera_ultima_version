"""
Servicio principal de asignaciÃ³n de contratos.
Implementa la lÃ³gica de contratos fijos, limpieza y balanceo configurable.
"""
import logging
import math
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Set, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import bindparam, text
from app.core.config import settings
from app.core.dpd import ASSIGNMENT_DPD_ORDER, get_assignment_dpd_range, get_dpd_range
from app.database.models import ContractAdvisor
from app.runtime_config.service import AssignmentRuntimeConfig, RuntimeConfigService
from app.services.blacklist_service import blacklist_service
from app.services.contract_service import ContractService
from app.services.history_service import HistoryService
from app.services.manual_fixed_service import ManualFixedService

logger = logging.getLogger(__name__)


class AssignmentService:
    """
    Servicio de asignaciÃ³n de contratos a asesores.
    Implementa la lÃ³gica de contratos fijos, limpieza y balanceo.
    """
    
    def __init__(
        self,
        mysql_session: Optional[Session],
        postgres_session: Session,
    ):
        """
        Args:
            mysql_session: SesiÃ³n MySQL para consultar contratos
            postgres_session: SesiÃ³n PostgreSQL para gestionar asignaciones
        """
        self.mysql_session = mysql_session
        self.postgres_session = postgres_session
        self.contract_service: Optional[ContractService] = (
            ContractService(mysql_session) if mysql_session is not None else None
        )
        self.history_service = HistoryService(postgres_session)
        self.manual_fixed_service = ManualFixedService(postgres_session)
        self.runtime_config_service = RuntimeConfigService()
        self._estado_actual_column_ready = False
        self._history_dpd_actual_column_ready = False
        
        # Variables de control para balanceo
        self._last_assigned_user = settings.USER_IDS[1]  # Empieza con 81

    def _require_contract_service(self) -> ContractService:
        """Devuelve ContractService o falla si no hay sesion MySQL."""
        if self.contract_service is None:
            raise RuntimeError(
                "No hay sesion MySQL configurada para esta operacion."
            )
        return self.contract_service

    def _ensure_estado_actual_column(self) -> bool:
        """
        Garantiza que contract_advisors tenga la columna estado_actual.
        """
        if self._estado_actual_column_ready:
            return True

        try:
            exists_row = self.postgres_session.execute(
                text(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM information_schema.columns
                    WHERE table_schema = 'alocreditindicators'
                      AND table_name = 'contract_advisors'
                      AND column_name = 'estado_actual'
                    """
                )
            ).mappings().first()
            column_exists = int(exists_row["cnt"] or 0) > 0
            if column_exists:
                self._estado_actual_column_ready = True
                return True

            # Evita esperas largas por lock en horario operativo.
            self.postgres_session.execute(text("SET LOCAL lock_timeout = '2s'"))
            self.postgres_session.execute(text("SET LOCAL statement_timeout = '10s'"))
            self.postgres_session.execute(
                text(
                    """
                    ALTER TABLE alocreditindicators.contract_advisors
                    ADD COLUMN IF NOT EXISTS estado_actual VARCHAR(100)
                    """
                )
            )
            self.postgres_session.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_contract_advisors_estado_actual
                    ON alocreditindicators.contract_advisors (estado_actual)
                    """
                )
            )
            self.postgres_session.commit()
            self._estado_actual_column_ready = True
            logger.info("Columna contract_advisors.estado_actual lista")
            return True
        except Exception as error:
            self.postgres_session.rollback()
            logger.warning(
                "No se pudo asegurar columna estado_actual sin bloqueo. "
                "Se omite actualizacion de estado_actual en esta corrida: %s",
                error,
            )
            return False

    def _ensure_history_dpd_actual_column(self) -> bool:
        """
        Garantiza que contract_advisors_history tenga la columna dpd_actual.
        """
        if self._history_dpd_actual_column_ready:
            return True

        try:
            exists_row = self.postgres_session.execute(
                text(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM information_schema.columns
                    WHERE table_schema = 'alocreditindicators'
                      AND table_name = 'contract_advisors_history'
                      AND column_name = 'dpd_actual'
                    """
                )
            ).mappings().first()
            column_exists = int(exists_row["cnt"] or 0) > 0
            if column_exists:
                self._history_dpd_actual_column_ready = True
                return True

            self.postgres_session.execute(text("SET LOCAL lock_timeout = '2s'"))
            self.postgres_session.execute(text("SET LOCAL statement_timeout = '10s'"))
            self.postgres_session.execute(
                text(
                    """
                    ALTER TABLE alocreditindicators.contract_advisors_history
                    ADD COLUMN IF NOT EXISTS dpd_actual VARCHAR(20)
                    """
                )
            )
            self.postgres_session.commit()
            self._history_dpd_actual_column_ready = True
            logger.info("Columna contract_advisors_history.dpd_actual lista")
            return True
        except Exception as error:
            self.postgres_session.rollback()
            logger.warning(
                "No se pudo asegurar columna dpd_actual en historial. "
                "Se continuara sin sincronizar dpd_actual en esta corrida: %s",
                error,
            )
            return False

    def refresh_estado_actual_for_assignments(
        self,
        current_assignments: Dict[int, Set[int]],
    ) -> Dict[str, int]:
        """
        Actualiza estado_actual para contratos actualmente asignados.
        """
        stats = {
            "contracts_considered": 0,
            "states_loaded": 0,
            "rows_updated": 0,
            "history_rows_updated": 0,
            "lookup_failed": 0,
            "dpd_loaded": 0,
            "dpd_lookup_failed": 0,
            "sync_failed": 0,
        }

        all_assigned_contracts: Set[int] = set()
        for user_id in settings.USER_IDS:
            all_assigned_contracts.update(
                int(contract_id) for contract_id in current_assignments.get(user_id, set())
            )

        stats["contracts_considered"] = len(all_assigned_contracts)
        if not all_assigned_contracts:
            return stats

        if not self._ensure_estado_actual_column():
            return stats
        sync_history_dpd = self._ensure_history_dpd_actual_column()

        sorted_contract_ids = sorted(all_assigned_contracts)

        try:
            state_map = self._require_contract_service().get_current_state_for_contracts(
                sorted_contract_ids
            )
        except Exception as estado_error:
            stats["lookup_failed"] = len(all_assigned_contracts)
            logger.warning(
                "No se pudo sincronizar estado_actual en esta corrida: %s",
                estado_error,
            )
            return stats
        stats["states_loaded"] = len(state_map)

        dpd_map: Dict[int, Optional[str]] = {}
        try:
            days_map = self._require_contract_service().get_days_overdue_for_contracts(
                sorted_contract_ids
            )
            for contract_id in sorted_contract_ids:
                days = days_map.get(contract_id)
                dpd_map[contract_id] = get_dpd_range(
                    int(days) if days is not None else None
                )
        except Exception as dpd_error:
            stats["dpd_lookup_failed"] = len(sorted_contract_ids)
            logger.warning(
                "No se pudo sincronizar dpd_actual en esta corrida: %s",
                dpd_error,
            )
        stats["dpd_loaded"] = sum(1 for value in dpd_map.values() if value)

        params = [
            {
                "contract_id": int(contract_id),
                "estado_actual": str(
                    state_map.get(int(contract_id), "SIN_ESTADO") or "SIN_ESTADO"
                ),
                "dpd_actual": str(dpd_map.get(int(contract_id)))
                if dpd_map.get(int(contract_id))
                else None,
            }
            for contract_id in sorted_contract_ids
        ]

        try:
            # Sincronizacion en bloque:
            # 1) cargar estados a tabla temporal
            # 2) actualizar contract_advisors con un solo UPDATE ... FROM
            self.postgres_session.execute(text("SET LOCAL lock_timeout = '3s'"))
            self.postgres_session.execute(text("SET LOCAL statement_timeout = '15min'"))
            self.postgres_session.execute(
                text(
                    """
                    CREATE TEMP TABLE IF NOT EXISTS tmp_estado_actual_sync (
                        contract_id INTEGER PRIMARY KEY,
                        estado_actual VARCHAR(100),
                        dpd_actual VARCHAR(20)
                    ) ON COMMIT DROP
                    """
                )
            )
            self.postgres_session.execute(text("TRUNCATE tmp_estado_actual_sync"))

            batch_size = 1000
            for index in range(0, len(params), batch_size):
                chunk = params[index : index + batch_size]
                self.postgres_session.execute(
                    text(
                        """
                        INSERT INTO tmp_estado_actual_sync (contract_id, estado_actual, dpd_actual)
                        VALUES (:contract_id, :estado_actual, :dpd_actual)
                        ON CONFLICT (contract_id)
                        DO UPDATE SET
                            estado_actual = EXCLUDED.estado_actual,
                            dpd_actual = EXCLUDED.dpd_actual
                        """
                    ),
                    chunk,
                )

            result = self.postgres_session.execute(
                text(
                    """
                    UPDATE alocreditindicators.contract_advisors ca
                    SET estado_actual = t.estado_actual
                    FROM tmp_estado_actual_sync t
                    WHERE ca.contract_id = t.contract_id
                      AND ca.user_id IN (45, 81)
                      AND ca.estado_actual IS DISTINCT FROM t.estado_actual
                    """
                )
            )
            if sync_history_dpd:
                history_result = self.postgres_session.execute(
                    text(
                        """
                        UPDATE alocreditindicators.contract_advisors_history h
                        SET
                            estado_actual = t.estado_actual,
                            dpd_actual = COALESCE(t.dpd_actual, h.dpd_actual)
                        FROM tmp_estado_actual_sync t
                        WHERE h.contract_id = t.contract_id
                          AND h."Fecha Terminal" IS NULL
                          AND (
                              h.estado_actual IS DISTINCT FROM t.estado_actual
                              OR (
                                  t.dpd_actual IS NOT NULL
                                  AND h.dpd_actual IS DISTINCT FROM t.dpd_actual
                              )
                          )
                        """
                    )
                )
            else:
                history_result = self.postgres_session.execute(
                    text(
                        """
                        UPDATE alocreditindicators.contract_advisors_history h
                        SET estado_actual = t.estado_actual
                        FROM tmp_estado_actual_sync t
                        WHERE h.contract_id = t.contract_id
                          AND h."Fecha Terminal" IS NULL
                          AND h.estado_actual IS DISTINCT FROM t.estado_actual
                        """
                    )
                )
            self.postgres_session.commit()
            stats["rows_updated"] = int(result.rowcount or 0)
            stats["history_rows_updated"] = int(history_result.rowcount or 0)
            logger.info(
                "estado_actual/dpd_actual actualizado: contratos=%s, contract_advisors=%s, history=%s, dpd_cargados=%s",
                stats["contracts_considered"],
                stats["rows_updated"],
                stats["history_rows_updated"],
                stats["dpd_loaded"],
            )
            return stats
        except Exception as error:
            self.postgres_session.rollback()
            logger.error("Error actualizando estado_actual/dpd_actual: %s", error)
            stats["sync_failed"] = len(params)
            return stats

    @staticmethod
    def _build_weighted_sequence(
        total: int,
        serlefin_ratio: float = 0.6,
        initial_count_81: int = 0,
        initial_count_45: int = 0,
    ) -> List[int]:
        """
        Genera una secuencia balanceada por peso para repartir contratos.

        Ejemplo con total=3 y ratio 0.6 => [81, 45, 81]
        """
        sequence: List[int] = []
        count_81 = int(initial_count_81)
        count_45 = int(initial_count_45)

        for index in range(total):
            expected_total = initial_count_81 + initial_count_45 + index + 1
            expected_81 = expected_total * serlefin_ratio
            expected_45 = expected_total * (1 - serlefin_ratio)

            deficit_81 = expected_81 - count_81
            deficit_45 = expected_45 - count_45

            if deficit_81 >= deficit_45:
                sequence.append(81)
                count_81 += 1
            else:
                sequence.append(45)
                count_45 += 1

        return sequence

    @staticmethod
    def _compute_house_quotas(total: int, serlefin_ratio: float) -> Dict[int, int]:
        """
        Calcula cuotas finales por casa para mantener proporcion 60/40 (o configurable).
        """
        total_int = max(0, int(total))
        ratio = max(0.0, min(1.0, float(serlefin_ratio)))
        cobyser_ratio = 1.0 - ratio

        exact_81 = total_int * ratio
        exact_45 = total_int * cobyser_ratio
        quota_81 = int(math.floor(exact_81))
        quota_45 = int(math.floor(exact_45))

        remainder = total_int - (quota_81 + quota_45)
        if remainder > 0:
            frac_81 = exact_81 - quota_81
            frac_45 = exact_45 - quota_45
            if frac_81 >= frac_45:
                quota_81 += remainder
            else:
                quota_45 += remainder

        return {81: quota_81, 45: quota_45}

    @staticmethod
    def _build_alternating_user_sequence(
        total: int,
        quotas: Dict[int, int],
        first_user: int = 81,
    ) -> List[int]:
        """
        Crea secuencia alternada 81/45 respetando cuotas finales configuradas.
        """
        total_int = max(0, int(total))
        if total_int == 0:
            return []

        sequence: List[int] = []
        assigned = {81: 0, 45: 0}
        next_user = 81 if first_user not in {45, 81} else first_user

        while len(sequence) < total_int:
            preferred = next_user
            alternate = 45 if preferred == 81 else 81

            if assigned[preferred] < int(quotas.get(preferred, 0)):
                chosen = preferred
            elif assigned[alternate] < int(quotas.get(alternate, 0)):
                chosen = alternate
            else:
                break

            sequence.append(chosen)
            assigned[chosen] += 1
            next_user = alternate

        return sequence

    def _load_contract_blacklist(self) -> Set[int]:
        """
        Compatibilidad legacy.
        En modo operativo actual (append-only) no se excluyen contratos por lista negra.
        """
        logger.info(
            "Modo append-only activo: lista negra ignorada (sin exclusiones ni eliminaciones)."
        )
        return set()

    def enforce_blacklist_on_active_assignments(
        self,
        blocked_contract_ids: Set[int],
    ) -> Dict[str, int]:
        """
        Compatibilidad legacy.
        En modo operativo actual NO se eliminan asignaciones activas.
        """
        logger.info(
            "Modo append-only activo: no se ejecuta limpieza por lista negra en asignaciones activas."
        )
        return {
            "blocked_found_active": 0,
            "removed_from_contract_advisors": 0,
            "history_closed": 0,
        }

    def _load_runtime_assignment_config(self) -> AssignmentRuntimeConfig:
        """
        Carga configuracion dinamica activa para asignacion.
        Si falla, usa valores por defecto de settings.
        """
        try:
            runtime_config = self.runtime_config_service.get_assignment_config()
            logger.info(
                "Configuracion activa de asignacion: Serlefin %.2f%% | Cobyser %.2f%% | rango %s-%s",
                runtime_config.serlefin_percent,
                runtime_config.cobyser_percent,
                runtime_config.min_days,
                runtime_config.max_days,
            )
            return runtime_config
        except Exception as error:
            logger.error(
                "No se pudo cargar configuracion dinamica. Se usaran defaults: %s",
                error,
            )

            return AssignmentRuntimeConfig(
                serlefin_percent=float(settings.DEFAULT_SERLEFIN_PERCENT),
                cobyser_percent=float(settings.DEFAULT_COBYSER_PERCENT),
                min_days=int(settings.DEFAULT_ASSIGNMENT_MIN_DAYS),
                max_days=int(settings.DEFAULT_ASSIGNMENT_MAX_DAYS),
                updated_by=settings.ADMIN_DEFAULT_AUDIT_ACTOR,
                updated_at=datetime.utcnow(),
            )

    @staticmethod
    def _build_history_metadata_from_days(
        contracts_days_map: Dict[int, int],
        tipo: str = "ASIGNACION",
        states_map: Optional[Dict[int, str]] = None,
    ) -> Dict[int, Dict[str, Any]]:
        """
        Construye metadatos de historial por contrato usando dias de atraso.
        """
        metadata: Dict[int, Dict[str, Any]] = {}
        states_map = states_map or {}
        for contract_id, days in contracts_days_map.items():
            days_int = int(days) if days is not None else None
            metadata[int(contract_id)] = {
                "tipo": tipo,
                "dias_atraso_inicial": days_int,
                "dpd_inicial": get_dpd_range(days_int),
                "dpd_actual": get_dpd_range(days_int),
                "estado_actual": str(states_map.get(int(contract_id), "SIN_ESTADO")),
            }
        return metadata
    
    def process_manual_fixed_contracts(self, manual_contracts: Dict[int, List[int]]) -> Dict[str, any]:
        """
        Procesa contratos fijos manuales con validaciones por lotes.
        
        Args:
            manual_contracts: Diccionario {user_id: [contract_ids]}
        
        Returns:
            EstadÃ­sticas de procesamiento
        """
        logger.info("Procesando contratos fijos manuales...")
        return self.manual_fixed_service.validate_and_insert_manual_fixed(manual_contracts)
    
    def get_fixed_contracts(self) -> Dict[int, Set[int]]:
        """
        Obtiene contratos fijos SOLO por promesas activas.

        Regla operativa:
        - effect = acuerdo_de_pago
        - promise_date >= CURRENT_DATE

        Para evitar multiples consultas, resuelve todo en una sola query SQL:
        - filtra promesas activas
        - mapea usuario origen a casa principal (45 / 81)
        - deduplica por contrato dejando la gestion mas reciente
        """
        logger.info("Consultando contratos fijos por promesas activas...")

        fixed_contracts = {45: set(), 81: set()}

        try:
            cobyser_users = [int(user_id) for user_id in settings.COBYSER_USERS]
            serlefin_users = [int(user_id) for user_id in settings.SERLEFIN_USERS]
            eligible_users = sorted(set(cobyser_users + serlefin_users))

            if not eligible_users:
                logger.info("No hay usuarios configurados para promesas activas")
                return fixed_contracts

            statement = text(
                """
                WITH ranked AS (
                    SELECT
                        m.contract_id::BIGINT AS contract_id,
                        CASE
                            WHEN m.user_id IN :cobyser_users THEN 45
                            WHEN m.user_id IN :serlefin_users THEN 81
                            ELSE NULL
                        END AS target_user,
                        ROW_NUMBER() OVER (
                            PARTITION BY m.contract_id
                            ORDER BY COALESCE(m.management_date, TIMESTAMP '1900-01-01') DESC, m.id DESC
                        ) AS rn
                    FROM alocreditindicators.managements m
                    WHERE m.user_id IN :eligible_users
                      AND m.effect = :effect_acuerdo
                      AND m.promise_date IS NOT NULL
                      AND m.promise_date >= CURRENT_DATE
                )
                SELECT
                    target_user,
                    contract_id
                FROM ranked
                WHERE rn = 1
                  AND target_user IS NOT NULL
                ORDER BY contract_id
                """
            ).bindparams(
                bindparam("cobyser_users", expanding=True),
                bindparam("serlefin_users", expanding=True),
                bindparam("eligible_users", expanding=True),
            )

            rows = self.postgres_session.execute(
                statement,
                {
                    "cobyser_users": cobyser_users,
                    "serlefin_users": serlefin_users,
                    "eligible_users": eligible_users,
                    "effect_acuerdo": settings.EFFECT_ACUERDO_PAGO,
                },
            ).mappings().all()

            for row in rows:
                target_user = int(row["target_user"])
                contract_id = int(row["contract_id"])
                if target_user in fixed_contracts:
                    fixed_contracts[target_user].add(contract_id)

            logger.info(
                "Promesas activas detectadas: total=%s | COBYSER=%s | SERLEFIN=%s",
                len(rows),
                len(fixed_contracts[45]),
                len(fixed_contracts[81]),
            )
            return fixed_contracts

        except Exception as e:
            logger.error("Error al consultar contratos fijos por promesa activa: %s", e)
            self.postgres_session.rollback()
            raise

    def get_current_assignments(self) -> Dict[int, Set[int]]:
        """
        Obtiene las asignaciones actuales desde contract_advisors.
        
        Returns:
            Diccionario {user_id: set(contract_ids)}
        """
        logger.info("Consultando asignaciones actuales...")
        
        current_assignments = {user_id: set() for user_id in settings.USER_IDS}
        
        try:
            assignments = self.postgres_session.query(
                ContractAdvisor.user_id,
                ContractAdvisor.contract_id,
            ).filter(
                ContractAdvisor.user_id.in_(settings.USER_IDS)
            ).all()

            for user_id, contract_id in assignments:
                user_id_int = int(user_id)
                if user_id_int in current_assignments:
                    current_assignments[user_id_int].add(int(contract_id))
            
            logger.info(f"âœ“ Asignaciones actuales: Usuario 45: {len(current_assignments[45])}, Usuario 81: {len(current_assignments[81])}")
            
            return current_assignments
        
        except Exception as e:
            logger.error(f"âœ— Error al consultar asignaciones actuales: {e}")
            raise
    
    def clean_assignments(
        self,
        fixed_contracts: Dict[int, Set[int]],
        current_assignments: Dict[int, Set[int]],
        max_days_threshold: int,
    ) -> Dict[str, int]:
        """
        Compatibilidad legacy.
        En modo operativo actual (append-only) no se elimina ningun contrato.
        """
        logger.info(
            "Modo append-only activo: clean_assignments deshabilitado (no se eliminan contratos)."
        )
        return {
            "deleted_total": 0,
            "deleted_cobyser": 0,
            "deleted_serlefin": 0,
            "deleted_gt_threshold": 0,
            "deleted_gt_209": 0,
            "deleted_zero_no_effect": 0,
            "protected_fixed": 0,
            "history_closed": 0,
            "days_threshold_applied": max_days_threshold,
        }

    def balance_assignments(
        self,
        contracts_with_days: List[Dict],
        current_assignments: Dict[int, Set[int]],
        serlefin_ratio: float,
        blocked_contract_ids: Optional[Set[int]] = None,
    ) -> tuple[Dict[int, List[int]], Dict[int, int]]:
        """
        Balancea contratos nuevos por bucket DPD.
        En cada bucket aplica cuota configurable 60/40 (o valor activo).
        """
        serlefin_ratio = max(0.0, min(1.0, float(serlefin_ratio)))
        cobyser_ratio = 1.0 - serlefin_ratio
        logger.info(
            "Iniciando balanceo de contratos nuevos por bucket DPD (SERLEFIN %.2f%% | COBYSER %.2f%%)...",
            serlefin_ratio * 100,
            cobyser_ratio * 100,
        )

        new_assignments: Dict[int, List[int]] = {45: [], 81: []}
        blocked_ids = blocked_contract_ids or set()
        contracts_days_map: Dict[int, int] = {
            int(contract["contract_id"]): int(contract["days_overdue"])
            for contract in contracts_with_days
            if int(contract["contract_id"]) not in blocked_ids
        }

        valid_contract_ids = set(contracts_days_map.keys())

        all_currently_assigned_any: Set[int] = set()
        for user_id in settings.USER_IDS:
            all_currently_assigned_any.update(current_assignments.get(user_id, set()))

        contracts_new: List[Dict[str, int]] = []
        for contract in contracts_with_days:
            contract_id = int(contract["contract_id"])
            if contract_id in blocked_ids:
                continue
            if contract_id in all_currently_assigned_any:
                continue
            contracts_new.append(
                {
                    "contract_id": contract_id,
                    "days_overdue": int(contract["days_overdue"]),
                }
            )

        logger.info(
            "Contratos candidatos: total=%s, ya_asignados=%s, bloqueados=%s, nuevos=%s",
            len(contracts_with_days),
            len(all_currently_assigned_any & valid_contract_ids),
            len(blocked_ids & set(int(c["contract_id"]) for c in contracts_with_days)),
            len(contracts_new),
        )

        if not contracts_new:
            logger.info("No hay contratos nuevos para balancear")
            return new_assignments, contracts_days_map

        contracts_new.sort(key=lambda item: (int(item["days_overdue"]), int(item["contract_id"])))
        contracts_by_bucket: Dict[str, List[Dict[str, int]]] = {}
        skipped_without_bucket = 0
        for contract in contracts_new:
            days_overdue = int(contract["days_overdue"])
            dpd_range = get_assignment_dpd_range(days_overdue)
            if not dpd_range:
                skipped_without_bucket += 1
                continue
            contracts_by_bucket.setdefault(dpd_range, []).append(contract)

        range_stats: Dict[str, Dict[str, int]] = {
            dpd_range: {
                "total": 0,
                "target_81": 0,
                "target_45": 0,
                "assigned_81": 0,
                "assigned_45": 0,
            }
            for dpd_range in ASSIGNMENT_DPD_ORDER
        }

        # Se reparte por bucket en orden de menor atraso primero.
        for dpd_range in reversed(ASSIGNMENT_DPD_ORDER):
            bucket_contracts = contracts_by_bucket.get(dpd_range, [])
            bucket_total = len(bucket_contracts)
            if bucket_total <= 0:
                continue

            quotas = self._compute_house_quotas(bucket_total, serlefin_ratio)
            user_sequence = self._build_alternating_user_sequence(
                total=bucket_total,
                quotas=quotas,
                first_user=81,
            )

            if len(user_sequence) != bucket_total:
                logger.warning(
                    "Secuencia incompleta para bucket %s (esperado=%s, real=%s)",
                    dpd_range,
                    bucket_total,
                    len(user_sequence),
                )

            range_stats[dpd_range]["total"] = bucket_total
            range_stats[dpd_range]["target_81"] = int(quotas.get(81, 0))
            range_stats[dpd_range]["target_45"] = int(quotas.get(45, 0))

            for contract, user_id in zip(bucket_contracts, user_sequence):
                contract_id = int(contract["contract_id"])
                new_assignments[user_id].append(contract_id)
                if user_id == 81:
                    range_stats[dpd_range]["assigned_81"] += 1
                else:
                    range_stats[dpd_range]["assigned_45"] += 1

        total_45 = len(new_assignments[45])
        total_81 = len(new_assignments[81])
        total_assigned_now = total_45 + total_81
        pct_45 = (total_45 / total_assigned_now * 100) if total_assigned_now > 0 else 0.0
        pct_81 = (total_81 / total_assigned_now * 100) if total_assigned_now > 0 else 0.0

        logger.info("Balanceo completado:")
        logger.info(
            "  Nuevos -> SERLEFIN: %s (%.1f%%), COBYSER: %s (%.1f%%)",
            total_81,
            pct_81,
            total_45,
            pct_45,
        )
        logger.info(
            "  Total insertables en esta corrida: %s",
            total_assigned_now,
        )
        if skipped_without_bucket > 0:
            logger.warning(
                "  Contratos omitidos sin bucket DPD valido: %s",
                skipped_without_bucket,
            )

        for dpd_range in ASSIGNMENT_DPD_ORDER:
            stats = range_stats.get(dpd_range, {})
            total_bucket = int(stats.get("total", 0))
            if total_bucket <= 0:
                continue
            logger.info(
                "  Bucket %s -> total:%s | objetivo S:%s C:%s | asignado S:%s C:%s",
                dpd_range,
                total_bucket,
                int(stats.get("target_81", 0)),
                int(stats.get("target_45", 0)),
                int(stats.get("assigned_81", 0)),
                int(stats.get("assigned_45", 0)),
            )

        return new_assignments, contracts_days_map

    def save_assignments(
        self,
        assignments: Dict[int, List[int]],
        contracts_days_map: Optional[Dict[int, int]] = None,
        excluded_contract_ids: Optional[Set[int]] = None,
    ) -> Dict[str, int]:
        """
        Guarda nuevas asignaciones en contract_advisors y las registra en historial.
        """
        logger.info("Guardando nuevas asignaciones...")

        stats = {
            "inserted_total": 0,
            "inserted_cobyser": 0,
            "inserted_serlefin": 0,
            "skipped_blacklist": 0,
            "estado_lookup_failed": 0,
        }
        new_assignments: Dict[int, List[int]] = {}
        blocked_ids = excluded_contract_ids or set()
        days_cache = contracts_days_map or {}

        try:
            all_contract_ids: Set[int] = set()
            for contract_ids in assignments.values():
                all_contract_ids.update(contract_ids)

            if not all_contract_ids:
                logger.info("No hay contratos para insertar")
                return stats

            eligible_contract_ids: Set[int] = {
                int(contract_id)
                for contract_id in all_contract_ids
                if int(contract_id) not in blocked_ids
            }
            stats["skipped_blacklist"] = len(all_contract_ids) - len(eligible_contract_ids)
            if stats["skipped_blacklist"] > 0:
                logger.warning(
                    "Se omitieron %s contratos por lista negra",
                    stats["skipped_blacklist"],
                )
            if not eligible_contract_ids:
                logger.info("No hay contratos elegibles para insertar")
                return stats

            logger.info(
                f"Verificando duplicados para {len(eligible_contract_ids)} contratos unicos..."
            )
            existing_assignments = self.postgres_session.query(
                ContractAdvisor.contract_id
            ).filter(
                ContractAdvisor.contract_id.in_(eligible_contract_ids)
            ).all()

            existing_contract_ids = set(int(row[0]) for row in existing_assignments)
            rows_to_insert: List[Dict[str, Any]] = []
            states_cache: Dict[int, str] = {}

            if eligible_contract_ids:
                try:
                    states_cache = self._require_contract_service().get_current_state_for_contracts(
                        sorted(eligible_contract_ids)
                    )
                except Exception as estado_error:
                    stats["estado_lookup_failed"] = len(eligible_contract_ids)
                    logger.warning(
                        "No se pudo consultar estado_actual para insercion masiva. "
                        "Se insertara con valor por defecto. Error: %s",
                        estado_error,
                    )

            for user_id, contract_ids in assignments.items():
                new_assignments[user_id] = []

                for contract_id in contract_ids:
                    contract_id = int(contract_id)
                    if contract_id not in eligible_contract_ids:
                        continue
                    if contract_id in existing_contract_ids:
                        continue

                    rows_to_insert.append(
                        {
                            "contract_id": contract_id,
                            "user_id": int(user_id),
                            "estado_actual": str(
                                states_cache.get(contract_id, "PENDIENTE")
                            ),
                        }
                    )
                    new_assignments[user_id].append(contract_id)
                    existing_contract_ids.add(contract_id)

                    stats["inserted_total"] += 1
                    if user_id in settings.COBYSER_USERS:
                        stats["inserted_cobyser"] += 1
                    elif user_id in settings.SERLEFIN_USERS:
                        stats["inserted_serlefin"] += 1

            if rows_to_insert:
                self.postgres_session.bulk_insert_mappings(
                    ContractAdvisor,
                    rows_to_insert,
                )

            self.postgres_session.commit()

            inserted_contract_ids: List[int] = []
            for contract_ids in new_assignments.values():
                inserted_contract_ids.extend(contract_ids)

            history_stats = {"total_registered": 0}
            if inserted_contract_ids:
                missing_days = [
                    contract_id
                    for contract_id in inserted_contract_ids
                    if int(contract_id) not in days_cache
                ]
                if missing_days:
                    fetched_days = self._require_contract_service().get_days_overdue_for_contracts(
                        missing_days
                    )
                    days_cache.update(fetched_days)

                days_map = {
                    int(contract_id): int(days_cache.get(contract_id, 0))
                    for contract_id in inserted_contract_ids
                }
                states_for_history = {
                    int(contract_id): str(states_cache.get(contract_id, "PENDIENTE"))
                    for contract_id in inserted_contract_ids
                }
                assignment_metadata = self._build_history_metadata_from_days(
                    days_map,
                    tipo="ASIGNACION",
                    states_map=states_for_history,
                )

                logger.info("Registrando asignaciones en historial...")
                history_stats = self.history_service.register_assignments(
                    new_assignments,
                    assignment_metadata=assignment_metadata,
                    default_tipo="ASIGNACION",
                )

            logger.info("Asignaciones guardadas:")
            logger.info(f"  Total: {stats['inserted_total']}")
            logger.info(f"  COBYSER: {stats['inserted_cobyser']}")
            logger.info(f"  SERLEFIN: {stats['inserted_serlefin']}")
            logger.info(
                f"  Historial registrado: {history_stats.get('total_registered', 0)}"
            )

            return stats

        except Exception as e:
            logger.error(f"Error al guardar asignaciones: {e}")
            self.postgres_session.rollback()
            raise

    def ensure_fixed_contracts_assigned(
        self,
        fixed_contracts: Dict[int, Set[int]],
        max_days_threshold: int,
    ) -> Dict[str, int]:
        """
        Asegura que todos los contratos fijos esten en contract_advisors.
        """
        logger.info("Verificando que todos los contratos fijos esten asignados...")

        stats = {
            "inserted_total": 0,
            "inserted_cobyser": 0,
            "inserted_serlefin": 0,
            "already_assigned": 0,
            "skipped_gt_209": 0,
            "skipped_gt_threshold": 0,
            "estado_lookup_failed": 0,
            "days_threshold_applied": max_days_threshold,
        }
        new_fixed_assignments: Dict[int, List[int]] = {}

        try:
            assigned_rows = self.postgres_session.query(
                ContractAdvisor.user_id,
                ContractAdvisor.contract_id,
            ).filter(
                ContractAdvisor.user_id.in_(settings.USER_IDS)
            ).all()
            assigned_user_by_contract = {
                int(contract_id): int(user_id)
                for user_id, contract_id in assigned_rows
            }

            missing_by_user: Dict[int, Set[int]] = {45: set(), 81: set()}
            missing_all: Set[int] = set()

            for user_id in settings.USER_IDS:
                user_fixed_contracts = {
                    int(contract_id)
                    for contract_id in fixed_contracts.get(user_id, set())
                }
                if not user_fixed_contracts:
                    continue

                for contract_id in user_fixed_contracts:
                    assigned_user = assigned_user_by_contract.get(contract_id)
                    if assigned_user is None:
                        missing_by_user[user_id].add(contract_id)
                        missing_all.add(contract_id)
                        continue

                    if assigned_user == int(user_id):
                        stats["already_assigned"] += 1
                        continue

                    # No se reasigna automaticamente entre casas para evitar churn diario.
                    stats["already_assigned"] += 1
                    logger.warning(
                        "Contrato fijo %s tiene promesa activa para user %s pero hoy esta en user %s; se mantiene sin mover.",
                        contract_id,
                        user_id,
                        assigned_user,
                    )

            if not missing_all:
                logger.info(
                    "Todos los contratos fijos por promesa activa ya estan asignados (%s contratos)",
                    stats["already_assigned"],
                )
                return stats

            missing_days_map = self._require_contract_service().get_days_overdue_for_contracts(
                sorted(missing_all)
            )
            eligible_missing = {
                int(contract_id)
                for contract_id in missing_all
                if int(missing_days_map.get(contract_id, 0)) <= max_days_threshold
            }
            skipped_count = len(missing_all) - len(eligible_missing)
            stats["skipped_gt_threshold"] += skipped_count
            if max_days_threshold == 209:
                stats["skipped_gt_209"] += skipped_count

            states_cache: Dict[int, str] = {}
            if eligible_missing:
                try:
                    states_cache = self._require_contract_service().get_current_state_for_contracts(
                        sorted(eligible_missing)
                    )
                except Exception as estado_error:
                    stats["estado_lookup_failed"] = len(eligible_missing)
                    logger.warning(
                        "No se pudo consultar estado_actual para contratos fijos. "
                        "Se insertara con valor por defecto. Error: %s",
                        estado_error,
                    )

            rows_to_insert: List[Dict[str, Any]] = []
            for user_id in settings.USER_IDS:
                ordered_missing = sorted(
                    contract_id
                    for contract_id in missing_by_user.get(user_id, set())
                    if contract_id in eligible_missing
                )
                if not ordered_missing:
                    continue

                new_fixed_assignments[user_id] = ordered_missing
                for contract_id in ordered_missing:
                    rows_to_insert.append(
                        {
                            "user_id": int(user_id),
                            "contract_id": int(contract_id),
                            "estado_actual": str(
                                states_cache.get(int(contract_id), "PENDIENTE")
                            ),
                        }
                    )

                    stats["inserted_total"] += 1
                    if user_id == 45:
                        stats["inserted_cobyser"] += 1
                    elif user_id == 81:
                        stats["inserted_serlefin"] += 1

            if not rows_to_insert:
                logger.info(
                    "No hubo contratos fijos elegibles dentro del tope %s",
                    max_days_threshold,
                )
                return stats

            self._ensure_estado_actual_column()
            self.postgres_session.bulk_insert_mappings(
                ContractAdvisor,
                rows_to_insert,
            )
            self.postgres_session.commit()

            inserted_contract_ids: List[int] = []
            for contract_ids in new_fixed_assignments.values():
                inserted_contract_ids.extend(contract_ids)

            days_map = {
                int(contract_id): int(missing_days_map.get(contract_id, 0))
                for contract_id in inserted_contract_ids
            }
            states_for_history = {
                int(contract_id): str(states_cache.get(contract_id, "PENDIENTE"))
                for contract_id in inserted_contract_ids
            }
            assignment_metadata = self._build_history_metadata_from_days(
                days_map,
                tipo="FIJO_PROMESA_ACTIVA",
                states_map=states_for_history,
            )

            history_stats = self.history_service.register_assignments(
                new_fixed_assignments,
                assignment_metadata=assignment_metadata,
                default_tipo="FIJO_PROMESA_ACTIVA",
            )

            logger.info("Contratos fijos por promesa activa insertados:")
            logger.info("  Total: %s", stats["inserted_total"])
            logger.info("  COBYSER: %s", stats["inserted_cobyser"])
            logger.info("  SERLEFIN: %s", stats["inserted_serlefin"])
            logger.info(
                "  Omitidos >%s: %s",
                max_days_threshold,
                stats["skipped_gt_threshold"],
            )
            logger.info(
                "  Historial: %s",
                history_stats.get("total_registered", 0),
            )

            return stats

        except Exception as e:
            logger.error(f"Error al asegurar contratos fijos: {e}")
            self.postgres_session.rollback()
            raise

    def execute_assignment_process(self) -> Dict:
        """
        Ejecuta el proceso completo de asignacion:
        0. Carga configuracion dinamica (porcentaje y rango).
        1. Carga fijos por promesa activa y asegura su insercion.
        2. Consulta contratos en rango configurado (MySQL).
        3. Balancea nuevos por menor atraso, alternancia y cuota 60/40.
        4. Guarda asignaciones e historial.
        Modo operativo: append-only (nunca elimina contratos existentes).
        """
        logger.info("=" * 80)
        logger.info("INICIANDO PROCESO DE ASIGNACION DE CONTRATOS")
        logger.info("MODO: append-only (sin eliminaciones), con fijos por promesa activa")
        logger.info("=" * 80)

        process_start = datetime.now()
        results = {
            "success": False,
            "blacklist_contracts_count": 0,
            "blacklist_enforcement_stats": {},
            "fixed_contracts_count": {},
            "fixed_insert_stats": {},
            "contracts_to_assign": [],
            "balance_stats": {},
            "insert_stats": {},
            "estado_actual_update_stats": {},
            "final_assignments": {},
            "runtime_config": {},
            "error": None,
            "started_at": process_start.isoformat(),
        }

        try:
            runtime_config = self._load_runtime_assignment_config()
            effective_min_days = max(
                int(runtime_config.min_days),
                int(settings.DAYS_THRESHOLD),
            )
            effective_max_days = max(
                int(runtime_config.max_days),
                effective_min_days,
            )

            if effective_min_days != int(runtime_config.min_days):
                logger.warning(
                    "Rango minimo configurado (%s) ajustado a minimo operativo (%s)",
                    runtime_config.min_days,
                    effective_min_days,
                )
            if effective_max_days != int(runtime_config.max_days):
                logger.warning(
                    "Rango maximo configurado (%s) ajustado a %s",
                    runtime_config.max_days,
                    effective_max_days,
                )

            results["runtime_config"] = {
                "serlefin_percent": runtime_config.serlefin_percent,
                "cobyser_percent": runtime_config.cobyser_percent,
                "min_days": effective_min_days,
                "max_days": effective_max_days,
                "updated_by": runtime_config.updated_by,
                "updated_at": runtime_config.updated_at.isoformat(),
            }

            # Modo append-only: no se aplican exclusiones ni limpiezas en asignados activos.
            blocked_contract_ids: Set[int] = set()
            results["blacklist_contracts_count"] = 0
            results["blacklist_enforcement_stats"] = {
                "blocked_found_active": 0,
                "removed_from_contract_advisors": 0,
                "history_closed": 0,
            }

            fixed_contracts = self.get_fixed_contracts()
            results["fixed_contracts_count"] = {
                "cobyser_45": len(fixed_contracts.get(45, set())),
                "serlefin_81": len(fixed_contracts.get(81, set())),
            }
            fixed_insert_stats = self.ensure_fixed_contracts_assigned(
                fixed_contracts=fixed_contracts,
                max_days_threshold=effective_max_days,
            )
            results["fixed_insert_stats"] = fixed_insert_stats

            current_assignments = self.get_current_assignments()

            if fixed_insert_stats.get("inserted_total", 0) > 0:
                current_assignments = self.get_current_assignments()

            # Consulta principal a MySQL en una sola llamada simple.
            # El filtrado de lista negra se aplica en memoria para evitar queries
            # enormes cuando el TXT tenga miles de contratos bloqueados.
            contracts_with_arrears = self._require_contract_service().get_contracts_with_arrears(
                min_days=effective_min_days,
                max_days=effective_max_days,
                excluded_contract_ids=None,
            )
            results["contracts_to_assign"] = [
                contract["contract_id"]
                for contract in contracts_with_arrears
            ]

            new_assignments, contracts_days_map = self.balance_assignments(
                contracts_with_days=contracts_with_arrears,
                current_assignments=current_assignments,
                serlefin_ratio=runtime_config.serlefin_ratio,
                blocked_contract_ids=set(),
            )
            results["balance_stats"] = {
                user_id: len(contract_ids)
                for user_id, contract_ids in new_assignments.items()
            }
            results["contracts_days_map"] = contracts_days_map

            insert_stats = self.save_assignments(
                new_assignments,
                contracts_days_map=contracts_days_map,
                excluded_contract_ids=set(),
            )
            results["insert_stats"] = insert_stats

            current_assignments_after_insert = self.get_current_assignments()
            estado_stats = self.refresh_estado_actual_for_assignments(
                current_assignments_after_insert
            )
            results["estado_actual_update_stats"] = estado_stats

            results["final_assignments"] = {
                user_id: contract_ids
                for user_id, contract_ids in new_assignments.items()
            }
            results["success"] = True

            logger.info("=" * 80)
            logger.info("PROCESO DE ASIGNACION COMPLETADO")
            logger.info("=" * 80)

            try:
                logger.info("Generando y enviando informes por correo...")
                report_result = self.generate_and_send_reports()
                results["report_sent"] = report_result
            except Exception as report_error:
                logger.error(f"Error generando/enviando informes: {report_error}")
                results["report_sent"] = False
                results["report_error"] = str(report_error)

        except Exception as e:
            logger.error(f"Error en el proceso de asignacion: {e}")
            results["error"] = str(e)
            raise
        finally:
            process_end = datetime.now()
            results["finished_at"] = process_end.isoformat()
            results["duration_seconds"] = round(
                (process_end - process_start).total_seconds(),
                3,
            )

            try:
                completion_sent = self.send_completion_notification(results)
                results["completion_notification_sent"] = completion_sent
            except Exception as notify_error:
                logger.error(
                    "Error enviando notificacion de finalizacion: %s",
                    notify_error,
                )
                results["completion_notification_sent"] = False
                results["completion_notification_error"] = str(notify_error)

        return results

    def send_completion_notification(self, results: Dict[str, Any]) -> bool:
        """
        Notifica siempre por correo el resultado final del proceso de asignacion.
        """
        from app.services.email_service import email_service

        recipients = list(settings.notification_recipients)
        mandatory_recipient = "mdeulofeuth@alocredit.co"
        if mandatory_recipient not in recipients:
            recipients.append(mandatory_recipient)
        if not recipients:
            logger.warning(
                "No hay destinatarios configurados para notificacion de finalizacion"
            )
            return False

        success = bool(results.get("success"))
        status_label = "EXITOSO" if success else "CON ERROR"
        insert_stats = results.get("insert_stats", {}) or {}
        balance_stats = results.get("balance_stats", {}) or {}
        estado_stats = results.get("estado_actual_update_stats", {}) or {}
        fixed_counts = results.get("fixed_contracts_count", {}) or {}
        fixed_insert_stats = results.get("fixed_insert_stats", {}) or {}
        runtime_cfg = results.get("runtime_config", {}) or {}
        blacklist_stats = results.get("blacklist_enforcement_stats", {}) or {}
        contracts_to_assign_count = len(results.get("contracts_to_assign", []) or [])
        report_sent = bool(results.get("report_sent", False))
        error_message = str(results.get("error") or "").strip()
        report_error = str(results.get("report_error") or "").strip()
        final_assignments = results.get("final_assignments", {}) or {}
        assigned_serlefin = list(final_assignments.get(81, []) or [])
        assigned_cobyser = list(final_assignments.get(45, []) or [])
        assigned_total = len(assigned_serlefin) + len(assigned_cobyser)
        execution_reference = str(
            results.get("finished_at")
            or results.get("started_at")
            or datetime.now().isoformat()
        )
        execution_day = execution_reference.split("T")[0].split(" ")[0]

        def _ids_preview(contract_ids: List[int], limit: int = 25) -> str:
            if not contract_ids:
                return "sin nuevos contratos"
            head = ", ".join(str(int(cid)) for cid in contract_ids[:limit])
            if len(contract_ids) > limit:
                return f"{head} ... (+{len(contract_ids) - limit} mas)"
            return head

        subject = f"[ALOCREDIT] Proceso de asignacion automatica finalizado - {status_label}"

        error_block = ""
        if error_message:
            error_block += f"<p><strong>Error:</strong> {error_message}</p>"
        if report_error:
            error_block += f"<p><strong>Error reportes:</strong> {report_error}</p>"
        if not error_block:
            error_block = "<p><strong>Error:</strong> Sin errores reportados.</p>"

        body = f"""
        <html>
        <body style="font-family:Arial,sans-serif">
          <h2>Proceso de Asignacion Finalizado ({status_label})</h2>
          <p><strong>Tipo:</strong> Ejecucion automatica programada</p>
          <p><strong>Horario programado:</strong> {int(settings.AUTO_ASSIGNMENT_HOUR):02d}:{int(settings.AUTO_ASSIGNMENT_MINUTE):02d} ({settings.AUTO_ASSIGNMENT_TIMEZONE})</p>
          <p><strong>Se asigno el dia:</strong> {execution_day}</p>
          <p><strong>Inicio:</strong> {results.get("started_at", "-")}</p>
          <p><strong>Fin:</strong> {results.get("finished_at", "-")}</p>
          <p><strong>Duracion (s):</strong> {results.get("duration_seconds", "-")}</p>
          <hr />
          <p><strong>Rango operativo:</strong> {runtime_cfg.get("min_days", "-")} a {runtime_cfg.get("max_days", "-")} dias</p>
          <p><strong>Porcentaje:</strong> Serlefin {runtime_cfg.get("serlefin_percent", "-")}% / Cobyser {runtime_cfg.get("cobyser_percent", "-")}%</p>
          <p><strong>Contratos evaluados para asignar:</strong> {contracts_to_assign_count}</p>
          <p><strong>Fijos por promesa activa detectados:</strong> Cobyser {fixed_counts.get("cobyser_45", 0)} / Serlefin {fixed_counts.get("serlefin_81", 0)}</p>
          <p><strong>Fijos insertados esta corrida:</strong> {fixed_insert_stats.get("inserted_total", 0)} (Serlefin {fixed_insert_stats.get("inserted_serlefin", 0)} / Cobyser {fixed_insert_stats.get("inserted_cobyser", 0)})</p>
          <p><strong>Insertados:</strong> {insert_stats.get("inserted_total", 0)} (Serlefin {insert_stats.get("inserted_serlefin", 0)} / Cobyser {insert_stats.get("inserted_cobyser", 0)})</p>
          <p><strong>Balance calculado:</strong> Serlefin {balance_stats.get(81, 0)} / Cobyser {balance_stats.get(45, 0)}</p>
          <p><strong>Que se asigno en esta corrida:</strong> total {assigned_total} (Serlefin {len(assigned_serlefin)} / Cobyser {len(assigned_cobyser)})</p>
          <p><strong>Muestra contratos Serlefin:</strong> {_ids_preview(assigned_serlefin)}</p>
          <p><strong>Muestra contratos Cobyser:</strong> {_ids_preview(assigned_cobyser)}</p>
          <p><strong>estado_actual actualizado:</strong> contract_advisors={estado_stats.get("rows_updated", 0)} / history={estado_stats.get("history_rows_updated", 0)} (contratos activos {estado_stats.get("contracts_considered", 0)})</p>
          <p><strong>Lista negra cargada:</strong> {results.get("blacklist_contracts_count", 0)}</p>
          <p><strong>Removidos por lista negra (activos):</strong> {blacklist_stats.get("removed_from_contract_advisors", 0)}</p>
          <p><strong>Reporte adjunto enviado:</strong> {"SI" if report_sent else "NO"}</p>
          <hr />
          {error_block}
          <p>Correo automatico de cierre de ejecucion.</p>
        </body>
        </html>
        """

        if email_service.send_assignment_report(
            recipient=recipients,
            subject=subject,
            body=body,
            attachments=None,
        ):
            logger.info(
                "Notificacion de finalizacion enviada en un solo correo a %s destinatarios",
                len(recipients),
            )
            return True

        logger.warning(
            "No se pudo enviar la notificacion de finalizacion al grupo de %s destinatarios",
            len(recipients),
        )
        return False

    def generate_and_send_reports(self) -> bool:
        """
        Genera informes de asignacion y los envia por correo electronico.

        Returns:
            bool: True si todos los correos configurados fueron enviados
        """
        generated_report_files: List[str] = []
        try:
            from app.services.email_service import email_service
            from app.services.report_service_extended import report_service_extended

            metrics = report_service_extended.calculate_distribution_metrics()
            if not metrics or metrics.get("total", 0) == 0:
                logger.warning("No hay contratos asignados para generar informes")
                return False

            logger.info(
                "Metricas: Serlefin %s%% | Cobyser %s%%",
                metrics.get("serlefin_percent", 0),
                metrics.get("cobyser_percent", 0),
            )

            contracts_81 = report_service_extended.get_assigned_contracts(81)
            file_81, _ = report_service_extended.generate_report_for_user(
                user_id=81,
                user_name="Serlefin",
                contracts=contracts_81,
            )
            if file_81:
                generated_report_files.append(file_81)

            contracts_45 = report_service_extended.get_assigned_contracts(45)
            file_45, _ = report_service_extended.generate_report_for_user(
                user_id=45,
                user_name="Cobyser",
                contracts=contracts_45,
            )
            if file_45:
                generated_report_files.append(file_45)

            if not file_81 and not file_45:
                logger.error("No se pudieron generar los archivos de informe")
                return False

            metrics_html_general = report_service_extended.generate_metrics_html(
                metrics,
                audience="general",
            )
            metrics_html_cobyser = report_service_extended.generate_metrics_html(
                metrics,
                audience="cobyser",
            )
            metrics_html_serlefin = report_service_extended.generate_metrics_html(
                metrics,
                audience="serlefin",
            )
            serlefin_total_contracts = len(contracts_81)
            cobyser_total_contracts = len(contracts_45)
            total_contracts = serlefin_total_contracts + cobyser_total_contracts
            serlefin_percent = (
                round((serlefin_total_contracts / total_contracts) * 100, 2)
                if total_contracts > 0
                else 0.0
            )
            cobyser_percent = (
                round((cobyser_total_contracts / total_contracts) * 100, 2)
                if total_contracts > 0
                else 0.0
            )
            cobyser_recipients = settings.cobyser_notification_recipients
            serlefin_recipients = settings.serlefin_notification_recipients
            both_reports_recipients = settings.notification_recipients

            if not any([cobyser_recipients, serlefin_recipients, both_reports_recipients]):
                logger.error(
                    "No hay destinatarios configurados. "
                    "Define COBYSER_NOTIFICATION_RECIPIENTS, "
                    "SERLEFIN_NOTIFICATION_RECIPIENTS o NOTIFICATION_RECIPIENTS."
                )
                return False

            sent_ok = 0
            expected_total = 0
            generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            cobyser_attachment_text = (
                "Se adjunta base de Cobyser."
                if file_45
                else "No se pudo adjuntar base de Cobyser en esta corrida."
            )
            both_attachments_text = (
                "Se adjuntan base de Cobyser y base de Serlefin."
                if file_45 and file_81
                else "Se adjuntan las bases disponibles en esta corrida."
            )

            def _send_group(
                recipients: List[str],
                subject: str,
                body: str,
                attachments: List[str],
                label: str,
            ) -> None:
                nonlocal sent_ok, expected_total
                if not recipients:
                    return

                expected_total += len(recipients)
                for recipient in recipients:
                    ok = email_service.send_assignment_report(
                        recipient=recipient,
                        subject=subject,
                        body=body,
                        attachments=attachments or None,
                    )
                    if ok:
                        sent_ok += 1
                        logger.info("Correo %s enviado a %s", label, recipient)
                    else:
                        logger.warning("No se pudo enviar correo %s a %s", label, recipient)

            if cobyser_recipients:
                cobyser_subject = "Asignacion de cartera - Cobyser (notificacion + base)"
                cobyser_body = f"""
                <html>
                <body style="font-family:Arial,sans-serif">
                  <h2>Asignacion ejecutada - Cobyser</h2>
                  <p><strong>Fecha:</strong> {generated_at}</p>
                  <p>Proceso de asignacion ejecutado correctamente para Cobyser.</p>
                  <p><strong>Contratos asignados a Cobyser:</strong> {cobyser_total_contracts}</p>
                  <p><strong>Participacion Cobyser:</strong> {cobyser_percent}%</p>
                  <p>{cobyser_attachment_text}</p>
                  <p>Este correo muestra solo la informacion de Cobyser.</p>
                  <hr />
                  {metrics_html_cobyser}
                  <p>Correo automatico del sistema de asignacion.</p>
                </body>
                </html>
                """
                _send_group(
                    recipients=cobyser_recipients,
                    subject=cobyser_subject,
                    body=cobyser_body,
                    attachments=[file_45] if file_45 else [],
                    label="COBYSER",
                )

            if serlefin_recipients:
                serlefin_subject = "Asignacion de cartera - Serlefin (solo notificacion)"
                serlefin_body = f"""
                <html>
                <body style="font-family:Arial,sans-serif">
                  <h2>Asignacion ejecutada - Serlefin</h2>
                  <p><strong>Fecha:</strong> {generated_at}</p>
                  <p>Proceso de asignacion ejecutado correctamente para Serlefin.</p>
                  <p><strong>Contratos asignados a Serlefin:</strong> {serlefin_total_contracts}</p>
                  <p><strong>Participacion Serlefin:</strong> {serlefin_percent}%</p>
                  <p>Este correo se envia sin archivo adjunto, segun la regla operativa.</p>
                  <p>Este correo muestra solo la informacion de Serlefin.</p>
                  <hr />
                  {metrics_html_serlefin}
                  <p>Correo automatico del sistema de asignacion.</p>
                </body>
                </html>
                """
                _send_group(
                    recipients=serlefin_recipients,
                    subject=serlefin_subject,
                    body=serlefin_body,
                    attachments=[],
                    label="SERLEFIN",
                )

            if both_reports_recipients:
                both_subject = "Asignacion de cartera - Notificacion con ambas bases"
                both_body = f"""
                <html>
                <body style="font-family:Arial,sans-serif">
                  <h2>Asignacion ejecutada - Notificacion general</h2>
                  <p><strong>Fecha:</strong> {generated_at}</p>
                  <p>Proceso de asignacion ejecutado correctamente.</p>
                  <p><strong>Contratos asignados Serlefin:</strong> {serlefin_total_contracts}</p>
                  <p><strong>Contratos asignados Cobyser:</strong> {cobyser_total_contracts}</p>
                  <p>{both_attachments_text}</p>
                  <hr />
                  {metrics_html_general}
                  <p>Correo automatico del sistema de asignacion.</p>
                </body>
                </html>
                """
                both_attachments: List[str] = []
                if file_45:
                    both_attachments.append(file_45)
                if file_81:
                    both_attachments.append(file_81)

                _send_group(
                    recipients=both_reports_recipients,
                    subject=both_subject,
                    body=both_body,
                    attachments=both_attachments,
                    label="GENERAL_AMBAS_BASES",
                )

            if expected_total == 0:
                logger.warning("No se ejecutaron envios: no hay destinatarios activos")
                return False

            if sent_ok == expected_total:
                logger.info("Envio de informes completado: %s/%s", sent_ok, expected_total)
                return True

            logger.warning(
                "Envio parcial de informes: %s/%s correos enviados",
                sent_ok,
                expected_total,
            )
            return False

        except Exception as e:
            logger.error(f"Error en generate_and_send_reports: {e}")
            return False
        finally:
            self._cleanup_generated_report_files(generated_report_files)

    @staticmethod
    def _cleanup_generated_report_files(file_paths: List[str]) -> None:
        """
        Elimina reportes generados temporalmente despues del envio por correo.
        """
        if not file_paths:
            return

        unique_paths = []
        for raw_path in file_paths:
            normalized = str(raw_path or "").strip()
            if normalized and normalized not in unique_paths:
                unique_paths.append(normalized)

        for file_path in unique_paths:
            try:
                path = Path(file_path)
                if path.exists():
                    path.unlink()
                    logger.info("Reporte temporal eliminado: %s", path)
            except Exception as cleanup_error:
                logger.warning(
                    "No se pudo eliminar reporte temporal %s: %s",
                    file_path,
                    cleanup_error,
                )

    def finalize_all_active_assignments(self) -> Dict[str, int]:
        """
        Operacion deshabilitada en modo append-only.
        El sistema no elimina asignaciones activas.
        """
        logger.warning(
            "Operacion bloqueada: finalize_all_active_assignments deshabilitado en modo append-only (sin eliminaciones)."
        )
        return {
            "active_assignments_found": 0,
            "history_closed": 0,
            "history_updated": 0,
            "history_inserted": 0,
            "deleted_from_contract_advisors": 0,
            "disabled": 1,
        }



