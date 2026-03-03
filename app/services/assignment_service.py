"""
Servicio principal de asignaciÃ³n de contratos.
Implementa la lÃ³gica de contratos fijos, limpieza y balanceo configurable.
"""
import logging
import math
from datetime import datetime
from typing import List, Dict, Set, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.core.config import settings
from app.core.dpd import ASSIGNMENT_DPD_ORDER, get_assignment_dpd_range, get_dpd_range
from app.database.models import ContractAdvisor, Management
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
        
        # Variables de control para balanceo
        self._last_assigned_user = settings.USER_IDS[1]  # Empieza con 81

    def _require_contract_service(self) -> ContractService:
        """Devuelve ContractService o falla si no hay sesion MySQL."""
        if self.contract_service is None:
            raise RuntimeError(
                "No hay sesion MySQL configurada para esta operacion."
            )
        return self.contract_service

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
        """Carga lista negra de contratos desde TXT."""
        blocked_ids = blacklist_service.load_contract_ids()
        if blocked_ids:
            logger.info("Lista negra activa: %s contratos bloqueados", len(blocked_ids))
        else:
            logger.info("Lista negra vacia")
        return blocked_ids

    def enforce_blacklist_on_active_assignments(
        self,
        blocked_contract_ids: Set[int],
    ) -> Dict[str, int]:
        """
        Garantiza que contratos bloqueados no permanezcan asignados.
        """
        stats = {
            "blocked_found_active": 0,
            "removed_from_contract_advisors": 0,
            "history_closed": 0,
        }
        if not blocked_contract_ids:
            return stats

        try:
            active_blocked = self.postgres_session.query(
                ContractAdvisor.user_id,
                ContractAdvisor.contract_id,
            ).filter(
                ContractAdvisor.user_id.in_(settings.USER_IDS),
                ContractAdvisor.contract_id.in_(blocked_contract_ids),
            ).all()

            if not active_blocked:
                return stats

            contracts_removed: Dict[int, List[int]] = {45: [], 81: []}
            terminal_metadata: Dict[int, Dict[str, Any]] = {}

            for user_id, contract_id in active_blocked:
                user_id_int = int(user_id)
                contract_id_int = int(contract_id)
                if user_id_int not in contracts_removed:
                    continue
                contracts_removed[user_id_int].append(contract_id_int)
                stats["blocked_found_active"] += 1
                terminal_metadata[contract_id_int] = {
                    "tipo": "LISTA_NEGRA_REMOVIDO",
                    "dpd_terminal": None,
                    "dias_atraso_terminal": None,
                }

            if stats["blocked_found_active"] == 0:
                return stats

            deleted_count = self.postgres_session.query(ContractAdvisor).filter(
                ContractAdvisor.user_id.in_(settings.USER_IDS),
                ContractAdvisor.contract_id.in_(blocked_contract_ids),
            ).delete(synchronize_session=False)
            stats["removed_from_contract_advisors"] = int(deleted_count or 0)
            self.postgres_session.commit()

            history_stats = self.history_service.close_assignments(
                contracts_removed=contracts_removed,
                terminal_metadata=terminal_metadata,
            )
            stats["history_closed"] = int(history_stats.get("total_closed", 0))

            logger.warning(
                "Se removieron contratos bloqueados activos: %s (historial cerrado=%s)",
                stats["removed_from_contract_advisors"],
                stats["history_closed"],
            )
            return stats

        except Exception:
            self.postgres_session.rollback()
            raise

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
        tipo: str = "ASIGNACION"
    ) -> Dict[int, Dict[str, Any]]:
        """
        Construye metadatos de historial por contrato usando dias de atraso.
        """
        metadata: Dict[int, Dict[str, Any]] = {}
        for contract_id, days in contracts_days_map.items():
            days_int = int(days) if days is not None else None
            metadata[int(contract_id)] = {
                "tipo": tipo,
                "dias_atraso_inicial": days_int,
                "dpd_inicial": get_dpd_range(days_int),
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
        Obtiene los contratos FIJOS desde la tabla managements en PostgreSQL.
        
        Aplica dos filtros en orden:
        
        FILTRO 0 - effect='acuerdo_de_pago':
            - Mantener SOLO si promise_date >= HOY (la promesa NO ha pasado)
            - Si promise_date < HOY â†’ NO es fijo (marcar is_fixed=0)
        
        FILTRO 1 - effect='pago_total':
            - Mantener SOLO si management_date es de mÃ¡ximo 30 dÃ­as
            - Si han pasado mÃ¡s de 30 dÃ­as â†’ NO es fijo (marcar is_fixed=0)
        
        Los contratos que cumplen las condiciones se asignan a:
        - COBYSER: usuario 45 (incluye contratos de 45-51)
        - SERLEFIN: usuario 81 (incluye contratos de 81-86, 102-103)
        
        Returns:
            Diccionario {user_id: set(contract_ids)} - Solo usuarios 45 y 81
        """
        from datetime import datetime, timedelta
        from sqlalchemy import or_
        
        logger.info(
            "Consultando contratos fijos desde managements (PostgreSQL)..."
        )
        logger.info(
            "Aplicando filtros: acuerdo_de_pago (promise_date) "
            "y pago_total (management_date)..."
        )
        
        fixed_contracts = {45: set(), 81: set()}
        # IDs de registros en managements a marcar como NO fijos
        contracts_to_unfix = []
        
        try:
            today = datetime.now().date()
            logger.info(f"Fecha actual para filtros: {today} (tipo: {type(today).__name__})")
            
            # Crear datetime naive para comparaciones
            validity_datetime = datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=None
            ) - timedelta(days=settings.PAGO_TOTAL_VALIDITY_DAYS)
            logger.info(f"Fecha lÃ­mite pago_total (hace 30 dÃ­as): {validity_datetime.date()}")
            
            # Obtener TODOS los contratos con effect relevantes
            all_users = settings.COBYSER_USERS + settings.SERLEFIN_USERS
            all_managements = self.postgres_session.query(Management).filter(
                Management.user_id.in_(all_users),
                or_(
                    Management.effect == settings.EFFECT_ACUERDO_PAGO,
                    Management.effect == settings.EFFECT_PAGO_TOTAL
                )
            ).all()
            
            logger.info(
                f"Registros encontrados en managements: "
                f"{len(all_managements)}"
            )
            
            # Procesar cada registro aplicando los filtros
            stats = {
                'acuerdo_pago_valid': 0,
                'acuerdo_pago_expired': 0,
                'pago_total_valid': 0,
                'pago_total_expired': 0
            }
            
            for record in all_managements:
                is_valid = False
                
                # FILTRO 0: acuerdo_de_pago
                if record.effect == settings.EFFECT_ACUERDO_PAGO:
                    if record.promise_date and record.promise_date >= today:
                        is_valid = True
                        stats['acuerdo_pago_valid'] += 1
                        logger.info(f"  âœ“ Acuerdo VÃLIDO: contrato {record.contract_id}, user {record.user_id}, promise_date={record.promise_date} >= {today}")
                    else:
                        contracts_to_unfix.append(record.id)
                        stats['acuerdo_pago_expired'] += 1
                        if record.promise_date:
                            logger.info(f"  âœ— Acuerdo EXPIRADO: contrato {record.contract_id}, user {record.user_id}, promise_date={record.promise_date} < {today}")
                        else:
                            logger.info(f"  âœ— Acuerdo SIN FECHA: contrato {record.contract_id}, user {record.user_id}, promise_date=None")
                
                # FILTRO 1: pago_total
                elif record.effect == settings.EFFECT_PAGO_TOTAL:
                    if record.management_date:
                        # Convertir a naive si es aware para comparaciÃ³n
                        mgmt_date = record.management_date
                        if mgmt_date.tzinfo is not None:
                            mgmt_date = mgmt_date.replace(tzinfo=None)
                        
                        # Rango de 1 mes: hace 30 dÃ­as <= mgmt_date <= hoy
                        hoy_naive = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=None)
                        if validity_datetime <= mgmt_date <= hoy_naive:
                            is_valid = True
                            stats['pago_total_valid'] += 1
                            logger.info(f"  âœ“ Pago VÃLIDO: contrato {record.contract_id}, user {record.user_id}, mgmt_date={mgmt_date.date()} en [{validity_datetime.date()}, {hoy_naive.date()}]")
                        else:
                            contracts_to_unfix.append(record.id)
                            stats['pago_total_expired'] += 1
                            logger.info(f"  âœ— Pago EXPIRADO: contrato {record.contract_id}, user {record.user_id}, mgmt_date={mgmt_date.date()} fuera [{validity_datetime.date()}, {hoy_naive.date()}]")
                    else:
                        contracts_to_unfix.append(record.id)
                        stats['pago_total_expired'] += 1
                        logger.info(f"  âœ— Pago SIN FECHA: contrato {record.contract_id}, user {record.user_id}, management_date=None")
                
                # Si es vÃ¡lido, asignar al usuario correspondiente
                if is_valid:
                    if record.user_id in settings.COBYSER_USERS:
                        fixed_contracts[45].add(record.contract_id)
                    elif record.user_id in settings.SERLEFIN_USERS:
                        fixed_contracts[81].add(record.contract_id)
            
            # Actualizar registros que ya NO son fijos
            # (por lotes para optimizar)
            # NOTA: Esta actualizaciÃ³n solo se hace si la columna exists
            if contracts_to_unfix:
                logger.info(
                    f"Contratos que ya NO cumplen condiciones para "
                    f"ser fijos: {len(contracts_to_unfix)}"
                )
                # No actualizamos is_fixed por ahora (columna opcional)
            
            logger.info("âœ“ AnÃ¡lisis de contratos fijos completado:")
            logger.info("  Acuerdo de Pago:")
            logger.info(
                f"    - VÃ¡lidos (promise_date >= hoy): "
                f"{stats['acuerdo_pago_valid']}"
            )
            logger.info(
                f"    - Expirados (promise_date < hoy): "
                f"{stats['acuerdo_pago_expired']}"
            )
            logger.info("  Pago Total:")
            logger.info(
                f"    - VÃ¡lidos (â‰¤ 30 dÃ­as): "
                f"{stats['pago_total_valid']}"
            )
            logger.info(
                f"    - Expirados (> 30 dÃ­as): "
                f"{stats['pago_total_expired']}"
            )
            logger.info("")
            logger.info("  Contratos fijos activos:")
            logger.info(
                f"    - COBYSER (Usuario 45): "
                f"{len(fixed_contracts[45])} contratos"
            )
            logger.info(
                f"    - SERLEFIN (Usuario 81): "
                f"{len(fixed_contracts[81])} contratos"
            )
            total_fixed = len(fixed_contracts[45]) + len(fixed_contracts[81])
            logger.info(f"    - Total: {total_fixed}")
            
            return fixed_contracts
        
        except Exception as e:
            logger.error(f"âœ— Error al consultar contratos fijos: {e}")
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
            assignments = self.postgres_session.query(ContractAdvisor).filter(
                ContractAdvisor.user_id.in_(settings.USER_IDS)
            ).all()
            
            for assignment in assignments:
                if assignment.user_id in current_assignments:
                    current_assignments[assignment.user_id].add(assignment.contract_id)
            
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
        Elimina contratos asignados segun reglas de negocio:
        1) Dias de atraso > tope configurado: eliminar siempre, aunque sea fijo.
        2) Dias de atraso == 0 y sin pago_total/acuerdo_de_pago: eliminar.

        Ademas, registra cierre en historial con tipo, DPD y dias terminales.
        """
        logger.info(
            "Ejecutando limpieza de asignaciones segun reglas >%s y 0 sin gestion...",
            max_days_threshold,
        )

        stats = {
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

        try:
            all_assigned_contracts: Set[int] = set()
            for user_id in settings.USER_IDS:
                all_assigned_contracts.update(current_assignments.get(user_id, set()))

            if not all_assigned_contracts:
                logger.info("No hay contratos asignados para limpiar")
                return stats

            contracts_days_map = self._require_contract_service().get_days_overdue_for_contracts(
                list(all_assigned_contracts)
            )

            all_fixed_contracts: Set[int] = set()
            for user_id in settings.USER_IDS:
                all_fixed_contracts.update(fixed_contracts.get(user_id, set()))

            contracts_removed: Dict[int, List[int]] = {45: [], 81: []}
            terminal_metadata: Dict[int, Dict[str, Any]] = {}

            for user_id in settings.USER_IDS:
                for contract_id in current_assignments.get(user_id, set()):
                    days_overdue = int(contracts_days_map.get(contract_id, 0))
                    removal_type = None

                    if days_overdue > max_days_threshold:
                        removal_type = (
                            "MAYOR_209_DIAS"
                            if max_days_threshold == 209
                            else f"MAYOR_{max_days_threshold}_DIAS"
                        )
                        stats["deleted_gt_threshold"] += 1
                        if max_days_threshold == 209:
                            stats["deleted_gt_209"] += 1
                    elif days_overdue == 0 and contract_id not in all_fixed_contracts:
                        removal_type = "CERO_DIAS_SIN_GESTION"
                        stats["deleted_zero_no_effect"] += 1
                    else:
                        if contract_id in all_fixed_contracts:
                            stats["protected_fixed"] += 1
                        continue

                    contracts_removed[user_id].append(contract_id)
                    terminal_metadata[contract_id] = {
                        "tipo": removal_type,
                        "dias_atraso_terminal": days_overdue,
                        "dpd_terminal": get_dpd_range(days_overdue),
                        "dias_atraso_inicial": days_overdue,
                        "dpd_inicial": get_dpd_range(days_overdue),
                    }

            for user_id in settings.USER_IDS:
                contract_ids = contracts_removed[user_id]
                if not contract_ids:
                    continue

                deleted_count = self.postgres_session.query(ContractAdvisor).filter(
                    ContractAdvisor.user_id == user_id,
                    ContractAdvisor.contract_id.in_(contract_ids),
                ).delete(synchronize_session=False)

                stats["deleted_total"] += deleted_count
                if user_id in settings.COBYSER_USERS:
                    stats["deleted_cobyser"] += deleted_count
                elif user_id in settings.SERLEFIN_USERS:
                    stats["deleted_serlefin"] += deleted_count

            if stats["deleted_total"] > 0:
                self.postgres_session.commit()

                history_stats = self.history_service.close_assignments(
                    contracts_removed=contracts_removed,
                    terminal_metadata=terminal_metadata,
                )
                stats["history_closed"] = history_stats.get("total_closed", 0)

                logger.info(
                    "Limpieza completada: total=%s, >%s=%s, 0_sin_gestion=%s",
                    stats["deleted_total"],
                    max_days_threshold,
                    stats["deleted_gt_threshold"],
                    stats["deleted_zero_no_effect"],
                )
            else:
                logger.info("No se encontraron contratos para eliminar en esta ejecucion")

            return stats

        except Exception as e:
            logger.error(f"Error durante limpieza de asignaciones: {e}")
            self.postgres_session.rollback()
            raise

    def balance_assignments(
        self,
        contracts_with_days: List[Dict],
        current_assignments: Dict[int, Set[int]],
        serlefin_ratio: float,
        blocked_contract_ids: Optional[Set[int]] = None,
    ) -> tuple[Dict[int, List[int]], Dict[int, int]]:
        """
        Balancea contratos nuevos por menor atraso, alternando por pares
        y respetando cuota final configurable 60/40.
        """
        serlefin_ratio = max(0.0, min(1.0, float(serlefin_ratio)))
        cobyser_ratio = 1.0 - serlefin_ratio
        logger.info(
            "Iniciando balanceo de contratos nuevos por rango DPD (SERLEFIN %.2f%% | COBYSER %.2f%%)...",
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
        quotas = self._compute_house_quotas(len(contracts_new), serlefin_ratio)
        user_sequence = self._build_alternating_user_sequence(
            total=len(contracts_new),
            quotas=quotas,
            first_user=81,
        )

        range_stats: Dict[str, Dict[int, int]] = {
            dpd_range: {45: 0, 81: 0}
            for dpd_range in ASSIGNMENT_DPD_ORDER
        }

        for contract, user_id in zip(contracts_new, user_sequence):
            contract_id = int(contract["contract_id"])
            days_overdue = int(contract["days_overdue"])
            new_assignments[user_id].append(contract_id)
            dpd_range = get_assignment_dpd_range(days_overdue)
            if dpd_range:
                range_stats.setdefault(dpd_range, {45: 0, 81: 0})
                range_stats[dpd_range][user_id] += 1

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
            "  Cuotas aplicadas -> SERLEFIN: %s, COBYSER: %s",
            quotas.get(81, 0),
            quotas.get(45, 0),
        )
        logger.info(
            "  Total insertables en esta corrida: %s",
            total_assigned_now,
        )

        for dpd_range in ASSIGNMENT_DPD_ORDER:
            stats = range_stats.get(dpd_range, {45: 0, 81: 0})
            total_bucket = int(stats[45]) + int(stats[81])
            if total_bucket <= 0:
                continue
            logger.info(
                "  Rango %s -> SERLEFIN:%s COBYSER:%s",
                dpd_range,
                int(stats[81]),
                int(stats[45]),
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

            for user_id, contract_ids in assignments.items():
                new_assignments[user_id] = []

                for contract_id in contract_ids:
                    contract_id = int(contract_id)
                    if contract_id not in eligible_contract_ids:
                        continue
                    if contract_id in existing_contract_ids:
                        continue

                    self.postgres_session.add(
                        ContractAdvisor(contract_id=contract_id, user_id=user_id)
                    )
                    new_assignments[user_id].append(contract_id)
                    existing_contract_ids.add(contract_id)

                    stats["inserted_total"] += 1
                    if user_id in settings.COBYSER_USERS:
                        stats["inserted_cobyser"] += 1
                    elif user_id in settings.SERLEFIN_USERS:
                        stats["inserted_serlefin"] += 1

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
                assignment_metadata = self._build_history_metadata_from_days(
                    days_map,
                    tipo="ASIGNACION",
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
            "days_threshold_applied": max_days_threshold,
        }
        new_fixed_assignments: Dict[int, List[int]] = {}

        try:
            all_assigned = self.postgres_session.query(ContractAdvisor.contract_id).all()
            existing_contract_ids = set(int(row[0]) for row in all_assigned)

            for user_id in settings.USER_IDS:
                user_fixed_contracts = fixed_contracts.get(user_id, set())
                if not user_fixed_contracts:
                    continue

                missing_fixed = user_fixed_contracts - existing_contract_ids
                if not missing_fixed:
                    stats["already_assigned"] += len(
                        user_fixed_contracts & existing_contract_ids
                    )
                    continue

                missing_days_map = self._require_contract_service().get_days_overdue_for_contracts(
                    list(missing_fixed)
                )
                eligible_missing = {
                    contract_id
                    for contract_id in missing_fixed
                    if int(missing_days_map.get(contract_id, 0)) <= max_days_threshold
                }
                skipped_count = len(missing_fixed) - len(eligible_missing)
                stats["skipped_gt_threshold"] += skipped_count
                if max_days_threshold == 209:
                    stats["skipped_gt_209"] += skipped_count

                if not eligible_missing:
                    continue

                ordered_missing = sorted(eligible_missing)
                new_fixed_assignments[user_id] = ordered_missing

                for contract_id in ordered_missing:
                    self.postgres_session.add(
                        ContractAdvisor(user_id=user_id, contract_id=contract_id)
                    )
                    existing_contract_ids.add(contract_id)

                    stats["inserted_total"] += 1
                    if user_id == 45:
                        stats["inserted_cobyser"] += 1
                    elif user_id == 81:
                        stats["inserted_serlefin"] += 1

            if stats["inserted_total"] > 0:
                self.postgres_session.commit()

                inserted_contract_ids: List[int] = []
                for contract_ids in new_fixed_assignments.values():
                    inserted_contract_ids.extend(contract_ids)

                days_map = self._require_contract_service().get_days_overdue_for_contracts(
                    inserted_contract_ids
                )
                assignment_metadata = self._build_history_metadata_from_days(
                    days_map,
                    tipo="FIJO_NUEVO",
                )

                history_stats = self.history_service.register_assignments(
                    new_fixed_assignments,
                    assignment_metadata=assignment_metadata,
                    default_tipo="FIJO_NUEVO",
                )

                logger.info("Contratos fijos insertados:")
                logger.info(f"  Total: {stats['inserted_total']}")
                logger.info(f"  COBYSER: {stats['inserted_cobyser']}")
                logger.info(f"  SERLEFIN: {stats['inserted_serlefin']}")
                logger.info(
                    f"  Omitidos >{max_days_threshold}: {stats['skipped_gt_threshold']}"
                )
                logger.info(
                    f"  Historial: {history_stats.get('total_registered', 0)}"
                )
            else:
                logger.info(
                    "Todos los contratos fijos ya estan asignados "
                    f"({stats['already_assigned']} contratos)"
                )
                if stats["skipped_gt_threshold"] > 0:
                    logger.info(
                        f"  Omitidos >{max_days_threshold}: {stats['skipped_gt_threshold']}"
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
        1. Carga lista negra (contratos nunca asignables).
        2. Garantiza que lista negra no quede asignada.
        3. Consulta contratos en rango configurado (MySQL).
        4. Balancea nuevos por menor atraso, alternancia y cuota 60/40.
        5. Guarda asignaciones e historial.
        """
        logger.info("=" * 80)
        logger.info("INICIANDO PROCESO DE ASIGNACION DE CONTRATOS")
        logger.info("MODO: asignacion sin limpieza diaria, sin fijos")
        logger.info("=" * 80)

        process_start = datetime.now()
        results = {
            "success": False,
            "blacklist_contracts_count": 0,
            "blacklist_enforcement_stats": {},
            "contracts_to_assign": [],
            "balance_stats": {},
            "insert_stats": {},
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

            blocked_contract_ids = self._load_contract_blacklist()
            results["blacklist_contracts_count"] = len(blocked_contract_ids)

            blacklist_enforcement_stats = self.enforce_blacklist_on_active_assignments(
                blocked_contract_ids=blocked_contract_ids,
            )
            results["blacklist_enforcement_stats"] = blacklist_enforcement_stats

            current_assignments = self.get_current_assignments()

            if blacklist_enforcement_stats.get("removed_from_contract_advisors", 0) > 0:
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
                if int(contract["contract_id"]) not in blocked_contract_ids
            ]

            new_assignments, contracts_days_map = self.balance_assignments(
                contracts_with_days=contracts_with_arrears,
                current_assignments=current_assignments,
                serlefin_ratio=runtime_config.serlefin_ratio,
                blocked_contract_ids=blocked_contract_ids,
            )
            results["balance_stats"] = {
                user_id: len(contract_ids)
                for user_id, contract_ids in new_assignments.items()
            }
            results["contracts_days_map"] = contracts_days_map

            insert_stats = self.save_assignments(
                new_assignments,
                contracts_days_map=contracts_days_map,
                excluded_contract_ids=blocked_contract_ids,
            )
            results["insert_stats"] = insert_stats

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

        recipients = settings.notification_recipients
        if not recipients:
            logger.warning(
                "No hay destinatarios configurados para notificacion de finalizacion"
            )
            return False

        success = bool(results.get("success"))
        status_label = "EXITOSO" if success else "CON ERROR"
        insert_stats = results.get("insert_stats", {}) or {}
        balance_stats = results.get("balance_stats", {}) or {}
        runtime_cfg = results.get("runtime_config", {}) or {}
        blacklist_stats = results.get("blacklist_enforcement_stats", {}) or {}
        contracts_to_assign_count = len(results.get("contracts_to_assign", []) or [])
        report_sent = bool(results.get("report_sent", False))
        error_message = str(results.get("error") or "").strip()
        report_error = str(results.get("report_error") or "").strip()
        execution_reference = str(
            results.get("finished_at")
            or results.get("started_at")
            or datetime.now().isoformat()
        )
        execution_day = execution_reference.split("T")[0].split(" ")[0]

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
          <p><strong>Insertados:</strong> {insert_stats.get("inserted_total", 0)} (Serlefin {insert_stats.get("inserted_serlefin", 0)} / Cobyser {insert_stats.get("inserted_cobyser", 0)})</p>
          <p><strong>Balance calculado:</strong> Serlefin {balance_stats.get(81, 0)} / Cobyser {balance_stats.get(45, 0)}</p>
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

            contracts_45 = report_service_extended.get_assigned_contracts(45)
            file_45, _ = report_service_extended.generate_report_for_user(
                user_id=45,
                user_name="Cobyser",
                contracts=contracts_45,
            )

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

    def finalize_all_active_assignments(self) -> Dict[str, int]:
        """
        Finaliza todas las asignaciones activas:
        1) Cierra historial activo en contract_advisors_history (fecha terminal).
        2) Elimina todos los registros de contract_advisors.

        Returns:
            Estadisticas del cierre y limpieza.
        """
        logger.info("Finalizando todas las asignaciones activas tras generar reporte...")

        stats = {
            "active_assignments_found": 0,
            "history_closed": 0,
            "history_updated": 0,
            "history_inserted": 0,
            "deleted_from_contract_advisors": 0,
        }

        try:
            result = self.postgres_session.execute(
                text(
                    """
                    WITH clock AS (
                        SELECT NOW() AS ts
                    ),
                    active_rows AS (
                        SELECT
                            ca.id,
                            ca.user_id,
                            ca.contract_id
                        FROM alocreditindicators.contract_advisors ca
                    ),
                    active_pairs AS (
                        SELECT DISTINCT
                            ar.user_id,
                            ar.contract_id
                        FROM active_rows ar
                    ),
                    open_history AS (
                        SELECT
                            h.id,
                            h.user_id,
                            h.contract_id
                        FROM alocreditindicators.contract_advisors_history h
                        INNER JOIN active_pairs ap
                            ON ap.user_id = h.user_id
                           AND ap.contract_id = h.contract_id
                        WHERE h."Fecha Terminal" IS NULL
                    ),
                    updated AS (
                        UPDATE alocreditindicators.contract_advisors_history h
                        SET
                            "Fecha Terminal" = (SELECT ts FROM clock),
                            tipo = :tipo_finalizacion
                        FROM open_history oh
                        WHERE h.id = oh.id
                        RETURNING h.id
                    ),
                    inserted AS (
                        INSERT INTO alocreditindicators.contract_advisors_history (
                            user_id,
                            contract_id,
                            "Fecha Inicial",
                            "Fecha Terminal",
                            tipo,
                            dpd_inicial,
                            dpd_final,
                            dias_atraso_incial,
                            dias_atraso_terminal
                        )
                        SELECT
                            ap.user_id,
                            ap.contract_id,
                            (SELECT ts FROM clock),
                            (SELECT ts FROM clock),
                            :tipo_finalizacion,
                            NULL,
                            NULL,
                            NULL,
                            NULL
                        FROM active_pairs ap
                        LEFT JOIN open_history oh
                            ON oh.user_id = ap.user_id
                           AND oh.contract_id = ap.contract_id
                        WHERE oh.id IS NULL
                        RETURNING id
                    ),
                    deleted AS (
                        DELETE FROM alocreditindicators.contract_advisors ca
                        USING active_rows ar
                        WHERE ca.id = ar.id
                        RETURNING ca.id
                    )
                    SELECT
                        (SELECT COUNT(*) FROM active_rows) AS active_assignments_found,
                        (SELECT COUNT(*) FROM updated) AS history_updated,
                        (SELECT COUNT(*) FROM inserted) AS history_inserted,
                        (SELECT COUNT(*) FROM deleted) AS deleted_from_contract_advisors
                    """
                ),
                {"tipo_finalizacion": "FINALIZADO_REPORTE_CARTERA"},
            )

            payload = result.mappings().one()
            stats["active_assignments_found"] = int(payload["active_assignments_found"] or 0)
            stats["history_updated"] = int(payload["history_updated"] or 0)
            stats["history_inserted"] = int(payload["history_inserted"] or 0)
            stats["deleted_from_contract_advisors"] = int(payload["deleted_from_contract_advisors"] or 0)
            stats["history_closed"] = stats["history_updated"] + stats["history_inserted"]

            self.postgres_session.commit()

            if stats["active_assignments_found"] == 0:
                logger.info("No hay asignaciones activas para finalizar")
                return stats

            logger.info(
                "Finalizacion completada: activos=%s, historial_cerrado=%s, eliminados=%s",
                stats["active_assignments_found"],
                stats["history_closed"],
                stats["deleted_from_contract_advisors"],
            )

            return stats

        except Exception as e:
            logger.error(f"Error finalizando asignaciones activas: {e}")
            self.postgres_session.rollback()
            raise



