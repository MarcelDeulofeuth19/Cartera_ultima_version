"""
Servicio principal de asignaciÃ³n de contratos.
Implementa la lÃ³gica de contratos fijos, limpieza y balanceo configurable.
"""
import logging
from datetime import datetime
from typing import List, Dict, Set, Any
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.dpd import ASSIGNMENT_DPD_ORDER, get_assignment_dpd_range, get_dpd_range
from app.database.models import ContractAdvisor, Management
from app.runtime_config.service import AssignmentRuntimeConfig, RuntimeConfigService
from app.services.contract_service import ContractService
from app.services.history_service import HistoryService
from app.services.manual_fixed_service import ManualFixedService

logger = logging.getLogger(__name__)


class AssignmentService:
    """
    Servicio de asignaciÃ³n de contratos a asesores.
    Implementa la lÃ³gica de contratos fijos, limpieza y balanceo.
    """
    
    def __init__(self, mysql_session: Session, postgres_session: Session):
        """
        Args:
            mysql_session: SesiÃ³n MySQL para consultar contratos
            postgres_session: SesiÃ³n PostgreSQL para gestionar asignaciones
        """
        self.mysql_session = mysql_session
        self.postgres_session = postgres_session
        self.contract_service = ContractService(mysql_session)
        self.history_service = HistoryService(postgres_session)
        self.manual_fixed_service = ManualFixedService(postgres_session)
        self.runtime_config_service = RuntimeConfigService()
        
        # Variables de control para balanceo
        self._last_assigned_user = settings.USER_IDS[1]  # Empieza con 81

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

            contracts_days_map = self.contract_service.get_days_overdue_for_contracts(
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
        fixed_contracts: Dict[int, Set[int]],
        current_assignments: Dict[int, Set[int]],
        serlefin_ratio: float,
        min_days: int,
        max_days: int,
    ) -> tuple[Dict[int, List[int]], Dict[int, int]]:
        """
        Balancea contratos nuevos con porcentaje configurable y equidad por rango DPD.

        Reglas clave:
        - Solo asigna contratos no asignados previamente.
        - Reparte cada rango DPD en proporcion configurable (81/45).
        - Evita sesgo de "viejos" vs "nuevos" dentro de cada rango.
        """
        serlefin_ratio = max(0.0, min(1.0, float(serlefin_ratio)))
        cobyser_ratio = 1.0 - serlefin_ratio
        logger.info(
            "Iniciando balanceo de contratos nuevos por rango DPD (SERLEFIN %.2f%% | COBYSER %.2f%%)...",
            serlefin_ratio * 100,
            cobyser_ratio * 100,
        )

        new_assignments: Dict[int, List[int]] = {45: [], 81: []}
        contracts_days_map: Dict[int, int] = {
            int(contract["contract_id"]): int(contract["days_overdue"])
            for contract in contracts_with_days
        }

        valid_contract_ids = set(contracts_days_map.keys())

        current_assignments_in_range = {
            user_id: current_assignments.get(user_id, set()) & valid_contract_ids
            for user_id in settings.USER_IDS
        }

        all_currently_assigned_in_range: Set[int] = set()
        all_currently_assigned_any: Set[int] = set()
        for user_id in settings.USER_IDS:
            all_currently_assigned_in_range.update(current_assignments_in_range[user_id])
            all_currently_assigned_any.update(current_assignments.get(user_id, set()))

        all_fixed_contracts: Set[int] = set()
        for user_id in settings.USER_IDS:
            all_fixed_contracts.update(fixed_contracts.get(user_id, set()))

        # Contratos nuevos = en rango de consulta, no asignados y no fijos existentes.
        contracts_new = [
            contract
            for contract in contracts_with_days
            if contract["contract_id"] not in all_currently_assigned_in_range
            and contract["contract_id"] not in all_fixed_contracts
        ]

        logger.info(
            f"Contratos en rango {min_days}-{max_days}: {len(contracts_with_days)} | "
            f"ya asignados en rango: {len(all_currently_assigned_in_range)} | "
            f"nuevos a balancear: {len(contracts_new)}"
        )

        # Paso 1: insertar fijos faltantes (si no estan asignados en ninguna parte).
        logger.info("Paso 1: incorporando contratos fijos faltantes...")
        for user_id in settings.USER_IDS:
            fixed_not_assigned = fixed_contracts.get(user_id, set()) - all_currently_assigned_any
            if not fixed_not_assigned:
                continue

            fixed_days_map = self.contract_service.get_days_overdue_for_contracts(
                list(fixed_not_assigned)
            )
            eligible_fixed = [
                contract_id
                for contract_id in fixed_not_assigned
                if int(fixed_days_map.get(contract_id, 0)) <= max_days
            ]
            ordered_fixed = sorted(eligible_fixed)
            if not ordered_fixed:
                continue

            new_assignments[user_id].extend(ordered_fixed)
            all_currently_assigned_any.update(ordered_fixed)

            logger.info(
                f"  Usuario {user_id}: {len(ordered_fixed)} contratos fijos agregados"
            )

        # Paso 2: balanceo por rango DPD.
        range_order = list(ASSIGNMENT_DPD_ORDER)

        contracts_by_range: Dict[str, List[Dict[str, Any]]] = {
            dpd_range: [] for dpd_range in range_order
        }

        for contract in contracts_new:
            contract_id = int(contract["contract_id"])
            days = int(contract["days_overdue"])
            dpd_range = get_assignment_dpd_range(days)

            if not dpd_range:
                continue

            contracts_by_range.setdefault(dpd_range, []).append(
                {
                    "contract_id": contract_id,
                    "days_overdue": days,
                }
            )

        range_stats: Dict[str, Dict[int, int]] = {
            dpd_range: {45: 0, 81: 0}
            for dpd_range in range_order
        }

        if contracts_new:
            logger.info("Paso 2: distribuyendo nuevos por rango DPD...")

            for dpd_range in range_order:
                bucket = contracts_by_range.get(dpd_range, [])
                if not bucket:
                    continue

                contracts_by_exact_day: Dict[int, List[int]] = {}
                for contract in bucket:
                    contracts_by_exact_day.setdefault(
                        int(contract["days_overdue"]),
                        [],
                    ).append(int(contract["contract_id"]))

                count_81 = 0
                count_45 = 0
                for days_overdue in sorted(contracts_by_exact_day.keys(), reverse=True):
                    contract_ids = sorted(contracts_by_exact_day[days_overdue])
                    user_sequence = self._build_weighted_sequence(
                        total=len(contract_ids),
                        serlefin_ratio=serlefin_ratio,
                        initial_count_81=count_81,
                        initial_count_45=count_45,
                    )

                    for contract_id, user_id in zip(contract_ids, user_sequence):
                        new_assignments[user_id].append(contract_id)
                        range_stats[dpd_range][user_id] += 1
                        if user_id == 81:
                            count_81 += 1
                        else:
                            count_45 += 1

                logger.info(
                    f"  Rango {dpd_range}: total={len(bucket)}, "
                    f"SERLEFIN={range_stats[dpd_range][81]}, "
                    f"COBYSER={range_stats[dpd_range][45]}"
                )
        else:
            logger.info("Paso 2: no hay contratos nuevos para balancear")

        total_45 = len(new_assignments[45])
        total_81 = len(new_assignments[81])
        total_assigned_now = total_45 + total_81

        new_45 = sum(range_stats[dpd_range][45] for dpd_range in range_order)
        new_81 = sum(range_stats[dpd_range][81] for dpd_range in range_order)
        total_new = new_45 + new_81

        pct_45 = (new_45 / total_new * 100) if total_new > 0 else 0.0
        pct_81 = (new_81 / total_new * 100) if total_new > 0 else 0.0

        logger.info("Balanceo completado:")
        logger.info(
            f"  Nuevos por regla configurable -> SERLEFIN: {new_81} ({pct_81:.1f}%), "
            f"COBYSER: {new_45} ({pct_45:.1f}%)"
        )
        logger.info(
            f"  Total insertables en esta corrida (incluye fijos faltantes): {total_assigned_now}"
        )

        return new_assignments, contracts_days_map

    def save_assignments(
        self,
        assignments: Dict[int, List[int]],
        max_days_threshold: int | None = None,
    ) -> Dict[str, int]:
        """
        Guarda nuevas asignaciones en contract_advisors y las registra en historial.
        """
        logger.info("Guardando nuevas asignaciones...")

        stats = {
            "inserted_total": 0,
            "inserted_cobyser": 0,
            "inserted_serlefin": 0,
            "skipped_gt_threshold": 0,
        }
        new_assignments: Dict[int, List[int]] = {}

        try:
            all_contract_ids: Set[int] = set()
            for contract_ids in assignments.values():
                all_contract_ids.update(contract_ids)

            if not all_contract_ids:
                logger.info("No hay contratos para insertar")
                return stats

            eligible_contract_ids: Set[int] = set(all_contract_ids)
            if max_days_threshold is not None:
                days_map = self.contract_service.get_days_overdue_for_contracts(
                    list(all_contract_ids)
                )
                eligible_contract_ids = {
                    contract_id
                    for contract_id in all_contract_ids
                    if int(days_map.get(contract_id, 0)) <= int(max_days_threshold)
                }
                stats["skipped_gt_threshold"] = (
                    len(all_contract_ids) - len(eligible_contract_ids)
                )
                if stats["skipped_gt_threshold"] > 0:
                    logger.warning(
                        "Se omitieron %s contratos por superar tope de %s dias",
                        stats["skipped_gt_threshold"],
                        max_days_threshold,
                    )
                if not eligible_contract_ids:
                    logger.info(
                        "No hay contratos elegibles para insertar tras filtro de tope (%s dias)",
                        max_days_threshold,
                    )
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
                days_map = self.contract_service.get_days_overdue_for_contracts(
                    inserted_contract_ids
                )
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

                missing_days_map = self.contract_service.get_days_overdue_for_contracts(
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

                days_map = self.contract_service.get_days_overdue_for_contracts(
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
        1. Obtiene contratos fijos.
        2. Inserta fijos faltantes.
        3. Limpia asignaciones (>tope y 0 sin gestion).
        4. Consulta contratos en rango configurado.
        5. Balancea nuevos con porcentaje configurable por rango DPD.
        6. Guarda asignaciones e historial.
        """
        logger.info("=" * 80)
        logger.info("INICIANDO PROCESO DE ASIGNACION DE CONTRATOS")
        logger.info("MODO: limpieza + asignacion configurable por rango DPD")
        logger.info("=" * 80)

        results = {
            "success": False,
            "fixed_contracts": {},
            "fixed_inserted_stats": {},
            "contracts_to_assign": [],
            "clean_stats": {},
            "balance_stats": {},
            "insert_stats": {},
            "final_assignments": {},
            "runtime_config": {},
            "error": None,
        }

        try:
            runtime_config = self._load_runtime_assignment_config()
            results["runtime_config"] = {
                "serlefin_percent": runtime_config.serlefin_percent,
                "cobyser_percent": runtime_config.cobyser_percent,
                "min_days": runtime_config.min_days,
                "max_days": runtime_config.max_days,
                "updated_by": runtime_config.updated_by,
                "updated_at": runtime_config.updated_at.isoformat(),
            }

            fixed_contracts = self.get_fixed_contracts()
            results["fixed_contracts"] = {
                user_id: list(contract_ids)
                for user_id, contract_ids in fixed_contracts.items()
            }

            fixed_insert_stats = self.ensure_fixed_contracts_assigned(
                fixed_contracts=fixed_contracts,
                max_days_threshold=runtime_config.max_days,
            )
            results["fixed_inserted_stats"] = fixed_insert_stats

            current_assignments = self.get_current_assignments()

            clean_stats = self.clean_assignments(
                fixed_contracts=fixed_contracts,
                current_assignments=current_assignments,
                max_days_threshold=runtime_config.max_days,
            )
            results["clean_stats"] = clean_stats

            if clean_stats.get("deleted_total", 0) > 0:
                current_assignments = self.get_current_assignments()

            contracts_with_arrears = self.contract_service.get_contracts_with_arrears(
                min_days=runtime_config.min_days,
                max_days=runtime_config.max_days,
            )
            results["contracts_to_assign"] = [
                contract["contract_id"] for contract in contracts_with_arrears
            ]

            new_assignments, contracts_days_map = self.balance_assignments(
                contracts_with_days=contracts_with_arrears,
                fixed_contracts=fixed_contracts,
                current_assignments=current_assignments,
                serlefin_ratio=runtime_config.serlefin_ratio,
                min_days=runtime_config.min_days,
                max_days=runtime_config.max_days,
            )
            results["balance_stats"] = {
                user_id: len(contract_ids)
                for user_id, contract_ids in new_assignments.items()
            }
            results["contracts_days_map"] = contracts_days_map

            insert_stats = self.save_assignments(
                new_assignments,
                max_days_threshold=runtime_config.max_days,
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

        return results

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

            metrics_html = report_service_extended.generate_metrics_html(metrics)

            recipients = settings.notification_recipients
            if not recipients:
                logger.error("No hay destinatarios configurados en NOTIFICATION_RECIPIENTS")
                return False

            sent_ok = 0
            for recipient in recipients:
                success = email_service.send_multiple_reports(
                    recipient=recipient,
                    serlefin_file=file_81 if file_81 else "",
                    cobyser_file=file_45 if file_45 else "",
                    metrics_html=metrics_html,
                    attach_serlefin_file=False,
                    attach_cobyser_file=True,
                )

                if success:
                    sent_ok += 1
                    logger.info("Informes enviados exitosamente a %s", recipient)
                else:
                    logger.warning("No se pudo enviar el correo a %s", recipient)

            if sent_ok == len(recipients):
                return True

            logger.warning(
                "Envio parcial de notificaciones: %s/%s correos enviados",
                sent_ok,
                len(recipients),
            )
            return False

        except Exception as e:
            logger.error(f"Error en generate_and_send_reports: {e}")
            return False



