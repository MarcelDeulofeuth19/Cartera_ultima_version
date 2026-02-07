"""
Servicio de división de contratos para 8 usuarios.
Implementa la lógica de contratos fijos, limpieza y balanceo equitativo.
Trabaja con contratos del día 1 al 60 de atraso.
"""
import logging
from typing import List, Dict, Set
from sqlalchemy.orm import Session
from app.core.config import settings
from app.database.models import ContractAdvisor, Management
from app.services.contract_service import ContractService
from app.services.history_service import HistoryService

logger = logging.getLogger(__name__)


class DivisionService:
    """
    Servicio de división de contratos a 8 usuarios.
    Implementa la lógica de contratos fijos, limpieza y balanceo equitativo.
    """
    
    def __init__(self, mysql_session: Session, postgres_session: Session):
        """
        Args:
            mysql_session: Sesión MySQL para consultar contratos
            postgres_session: Sesión PostgreSQL para gestionar asignaciones
        """
        self.mysql_session = mysql_session
        self.postgres_session = postgres_session
        self.contract_service = ContractService(mysql_session)
        self.history_service = HistoryService(postgres_session)
    
    def get_fixed_contracts(self) -> Dict[int, Set[int]]:
        """
        Obtiene los contratos FIJOS desde la tabla managements en PostgreSQL.
        
        Aplica dos filtros en orden:
        
        FILTRO 0 - effect='acuerdo_de_pago':
            - Mantener SOLO si promise_date >= HOY (la promesa NO ha pasado)
            - Si promise_date < HOY → NO es fijo (marcar is_fixed=0)
        
        FILTRO 1 - effect='pago_total':
            - Mantener SOLO si management_date es de máximo 30 días
            - Si han pasado más de 30 días → NO es fijo (marcar is_fixed=0)
        
        Los contratos que cumplen las condiciones se asignan según el usuario
        que tiene el registro en managements (usuarios 3, 4, 5, 6, 7, 8, 11, 12).
        
        Returns:
            Diccionario {user_id: set(contract_ids)} - Para los 8 usuarios
        """
        from datetime import datetime, timedelta
        from sqlalchemy import or_
        
        logger.info(
            "Consultando contratos fijos desde managements (PostgreSQL) "
            "para división de contratos..."
        )
        logger.info(
            "Aplicando filtros: acuerdo_de_pago (promise_date) "
            "y pago_total (management_date)..."
        )
        
        # Inicializar diccionario para los 8 usuarios
        fixed_contracts = {user_id: set() for user_id in settings.DIVISION_USER_IDS}
        contracts_to_unfix = []
        
        try:
            today = datetime.now().date()
            logger.info(f"Fecha actual para filtros: {today} (tipo: {type(today).__name__})")
            
            # Crear datetime naive para comparaciones
            validity_datetime = datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=None
            ) - timedelta(days=settings.PAGO_TOTAL_VALIDITY_DAYS)
            logger.info(f"Fecha límite pago_total (hace 30 días): {validity_datetime.date()}")
            
            # Obtener TODOS los contratos con effect relevantes de los 8 usuarios
            all_managements = self.postgres_session.query(Management).filter(
                Management.user_id.in_(settings.DIVISION_USER_IDS),
                or_(
                    Management.effect == settings.EFFECT_ACUERDO_PAGO,
                    Management.effect == settings.EFFECT_PAGO_TOTAL
                )
            ).all()
            
            logger.info(
                f"Registros encontrados en managements para división: "
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
                        logger.info(f"  ✓ Acuerdo VÁLIDO: contrato {record.contract_id}, user {record.user_id}, promise_date={record.promise_date} >= {today}")
                    else:
                        contracts_to_unfix.append(record.id)
                        stats['acuerdo_pago_expired'] += 1
                        if record.promise_date:
                            logger.info(f"  ✗ Acuerdo EXPIRADO: contrato {record.contract_id}, user {record.user_id}, promise_date={record.promise_date} < {today}")
                        else:
                            logger.info(f"  ✗ Acuerdo SIN FECHA: contrato {record.contract_id}, user {record.user_id}, promise_date=None")
                
                # FILTRO 1: pago_total
                elif record.effect == settings.EFFECT_PAGO_TOTAL:
                    if record.management_date:
                        # Convertir a naive si es aware para comparación
                        mgmt_date = record.management_date
                        if mgmt_date.tzinfo is not None:
                            mgmt_date = mgmt_date.replace(tzinfo=None)
                        
                        # Rango de 1 mes: hace 30 días <= mgmt_date <= hoy
                        hoy_naive = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=None)
                        if validity_datetime <= mgmt_date <= hoy_naive:
                            is_valid = True
                            stats['pago_total_valid'] += 1
                            logger.info(f"  ✓ Pago VÁLIDO: contrato {record.contract_id}, user {record.user_id}, mgmt_date={mgmt_date.date()} en [{validity_datetime.date()}, {hoy_naive.date()}]")
                        else:
                            contracts_to_unfix.append(record.id)
                            stats['pago_total_expired'] += 1
                            logger.info(f"  ✗ Pago EXPIRADO: contrato {record.contract_id}, user {record.user_id}, mgmt_date={mgmt_date.date()} fuera [{validity_datetime.date()}, {hoy_naive.date()}]")
                    else:
                        contracts_to_unfix.append(record.id)
                        stats['pago_total_expired'] += 1
                        logger.info(f"  ✗ Pago SIN FECHA: contrato {record.contract_id}, user {record.user_id}, management_date=None")
                
                # Si es válido, asignar al usuario correspondiente
                if is_valid and record.user_id in settings.DIVISION_USER_IDS:
                    fixed_contracts[record.user_id].add(record.contract_id)
            
            logger.info(f"✓ Análisis de contratos fijos para división completado:")
            logger.info("  Acuerdo de Pago:")
            logger.info(
                f"    - Válidos (promise_date >= hoy): "
                f"{stats['acuerdo_pago_valid']}"
            )
            logger.info(
                f"    - Expirados (promise_date < hoy): "
                f"{stats['acuerdo_pago_expired']}"
            )
            logger.info("  Pago Total:")
            logger.info(
                f"    - Válidos (≤ 30 días): "
                f"{stats['pago_total_valid']}"
            )
            logger.info(
                f"    - Expirados (> 30 días): "
                f"{stats['pago_total_expired']}"
            )
            logger.info("")
            logger.info("  Contratos fijos activos por usuario:")
            total_fixed = 0
            for user_id in settings.DIVISION_USER_IDS:
                count = len(fixed_contracts[user_id])
                total_fixed += count
                logger.info(f"    - Usuario {user_id}: {count} contratos")
            logger.info(f"    - Total: {total_fixed}")
            
            return fixed_contracts
        
        except Exception as e:
            logger.error(f"✗ Error al consultar contratos fijos para división: {e}")
            self.postgres_session.rollback()
            raise
    
    def get_current_assignments(self) -> Dict[int, Set[int]]:
        """
        Obtiene las asignaciones actuales desde contract_advisors para los 8 usuarios.
        
        Returns:
            Diccionario {user_id: set(contract_ids)}
        """
        logger.info("Consultando asignaciones actuales para división...")
        
        current_assignments = {user_id: set() for user_id in settings.DIVISION_USER_IDS}
        
        try:
            assignments = self.postgres_session.query(ContractAdvisor).filter(
                ContractAdvisor.user_id.in_(settings.DIVISION_USER_IDS)
            ).all()
            
            for assignment in assignments:
                if assignment.user_id in current_assignments:
                    current_assignments[assignment.user_id].add(assignment.contract_id)
            
            logger.info(f"✓ Asignaciones actuales para división:")
            for user_id in settings.DIVISION_USER_IDS:
                logger.info(f"  - Usuario {user_id}: {len(current_assignments[user_id])} contratos")
            
            return current_assignments
        
        except Exception as e:
            logger.error(f"✗ Error al consultar asignaciones actuales para división: {e}")
            raise
    
    def get_contracts_for_division(self) -> List[Dict]:
        """
        Obtiene contratos con 1 a 60 días de atraso desde MySQL.
        Reutiliza la lógica de ContractService pero con diferentes parámetros.
        
        Returns:
            Lista de diccionarios con contract_id y days_overdue
        """
        logger.info(
            f"Consultando contratos con {settings.DIVISION_MIN_DAYS} a "
            f"{settings.DIVISION_MAX_DAYS} días de atraso..."
        )
        
        try:
            # Usar el método existente pero con parámetros de división
            contracts = self.contract_service.get_contracts_with_arrears(
                min_days=settings.DIVISION_MIN_DAYS,
                max_days=settings.DIVISION_MAX_DAYS
            )
            
            logger.info(
                f"✓ Contratos encontrados para división: {len(contracts)}"
            )
            
            return contracts
        
        except Exception as e:
            logger.error(f"✗ Error al consultar contratos para división: {e}")
            raise
    
    def balance_assignments(
        self, 
        contracts_with_days: List[Dict],
        fixed_contracts: Dict[int, Set[int]],
        current_assignments: Dict[int, Set[int]]
    ) -> tuple[Dict[int, List[int]], Dict[int, int]]:
        """
        Balancea SOLO contratos NUEVOS (no asignados) de forma EQUITATIVA entre 8 usuarios.
        Cada usuario recibe aproximadamente 1/8 de los contratos nuevos.
        
        IMPORTANTE: NO elimina contratos ya asignados. Solo agrega nuevos.
        
        Lógica:
        1. Asignar contratos fijos que no estén asignados
        2. Identificar contratos NUEVOS (no asignados a ningún usuario)
        3. Ordenar contratos nuevos por días de atraso (mayor a menor)
        4. Dividir equitativamente entre los 8 usuarios (round-robin)
        
        Args:
            contracts_with_days: Lista de diccionarios con contract_id y days_overdue
            fixed_contracts: Contratos fijos por usuario
            current_assignments: Asignaciones actuales por usuario
        
        Returns:
            Tupla: (Diccionario {user_id: [contract_ids]}, Diccionario {contract_id: days_overdue})
        """
        logger.info(
            f"Iniciando balanceo equitativo de contratos NUEVOS entre "
            f"{len(settings.DIVISION_USER_IDS)} usuarios..."
        )
        
        new_assignments = {user_id: [] for user_id in settings.DIVISION_USER_IDS}
        contracts_days_map = {}
        
        # Crear mapeo de días de atraso
        for c in contracts_with_days:
            contracts_days_map[c['contract_id']] = c['days_overdue']
        
        # Crear set de contratos VÁLIDOS (solo los del rango 1-60 días)
        valid_contract_ids = set(c['contract_id'] for c in contracts_with_days)
        
        # Filtrar current_assignments para SOLO incluir contratos del rango válido (1-60 días)
        # Esto evita que contratos de OTROS rangos (0 días, 61-210 días, etc.) afecten el balance
        current_assignments_in_range = {}
        for user_id in settings.DIVISION_USER_IDS:
            current_assignments_in_range[user_id] = current_assignments[user_id] & valid_contract_ids
        
        # Obtener TODOS los contratos ya asignados EN EL RANGO (cualquier usuario)
        all_currently_assigned = set()
        for user_id in settings.DIVISION_USER_IDS:
            all_currently_assigned.update(current_assignments_in_range[user_id])
        
        # Contar contratos fijos detectados automáticamente
        total_fixed_auto = sum(len(fixed_contracts[uid]) for uid in settings.DIVISION_USER_IDS)
        # Contar TODOS los contratos actuales EN EL RANGO (fijos automáticos + fijos manuales + otros)
        total_currently_assigned = len(all_currently_assigned)
        
        logger.info(
            f"Total contratos con {settings.DIVISION_MIN_DAYS}-"
            f"{settings.DIVISION_MAX_DAYS} días: {len(contracts_with_days)}"
        )
        logger.info(
            f"Contratos ya asignados EN EL RANGO 1-60 días (se mantienen): {total_currently_assigned}"
        )
        logger.info(
            f"Contratos FIJOS automáticos detectados: {total_fixed_auto}"
        )
        logger.info("Desglose por usuario (SOLO contratos 1-60 días):")
        for user_id in settings.DIVISION_USER_IDS:
            fixed_auto = len(fixed_contracts[user_id])
            current_in_range = len(current_assignments_in_range[user_id])
            current_total_all_ranges = len(current_assignments[user_id])
            logger.info(
                f"  Usuario {user_id}: {fixed_auto} auto-fijos, {current_in_range} en rango 1-60 días "
                f"(+{current_total_all_ranges - current_in_range} fuera de rango)"
            )
        
        # Paso 0: EXCLUIR contratos ya asignados Y contratos fijos
        all_fixed_contracts = set()
        for user_id in settings.DIVISION_USER_IDS:
            all_fixed_contracts.update(fixed_contracts[user_id])
        
        # Contratos NUEVOS = no asignados y no fijos existentes
        contracts_new = [
            c for c in contracts_with_days
            if c['contract_id'] not in all_currently_assigned
            and c['contract_id'] not in all_fixed_contracts
        ]
        
        logger.info(
            f"Contratos NUEVOS a balancear: {len(contracts_new)}"
        )
        
        # Paso 1: Asignar contratos fijos que no estén asignados
        logger.info("Paso 1: Asignando contratos fijos no asignados...")
        for user_id in settings.DIVISION_USER_IDS:
            fixed_not_assigned = (
                fixed_contracts[user_id] - current_assignments_in_range[user_id]
            )
            
            for contract_id in fixed_not_assigned:
                new_assignments[user_id].append(contract_id)
            
            if fixed_not_assigned:
                logger.info(
                    f"  Usuario {user_id}: {len(fixed_not_assigned)} "
                    f"contratos fijos agregados"
                )
        
        # Paso 2: Dividir equitativamente contratos nuevos basado en balance actual
        if contracts_new:
            logger.info(
                f"Paso 2: Ordenando {len(contracts_new)} contratos NUEVOS "
                f"por días de atraso y dividiendo equitativamente..."
            )
            
            # Ordenar por días de atraso descendente
            sorted_contracts = sorted(
                contracts_new,
                key=lambda x: x['days_overdue'],
                reverse=True
            )
            
# BALANCE EQUITATIVO: Calcular contratos EN RANGO 1-60 días por usuario
            # Incluye: SOLO contratos del rango 1-60 días + fijos que se van a asignar ahora
            # NO incluye contratos de otros rangos (0 días, 61-210 días, etc.)
            current_counts = {}
            for user_id in settings.DIVISION_USER_IDS:
                # Contar SOLO contratos EN RANGO (manuales + automáticos) + fijos nuevos
                current_counts[user_id] = (
                    len(current_assignments_in_range[user_id]) + 
                    len(new_assignments[user_id])
                )
            
            logger.info("  Balance actual por usuario (SOLO rango 1-60 días + fijos nuevos):")
            for user_id in settings.DIVISION_USER_IDS:
                fixed_auto = len(fixed_contracts[user_id])
                current_in_range = len(current_assignments_in_range[user_id])
                logger.info(
                    f"    Usuario {user_id}: {current_counts[user_id]} totales en rango = "
                    f"{current_in_range} actuales ({fixed_auto} auto-fijos) + {len(new_assignments[user_id])} fijos nuevos"
                )
            
            # Asignar cada contrato al usuario que tiene MENOS contratos
            # Esto garantiza distribución equitativa (máximo 1 de diferencia)
            for contract in sorted_contracts:
                # Encontrar usuario con menor cantidad de contratos
                min_user = min(current_counts.keys(), key=lambda u: current_counts[u])
                
                # Asignar contrato a ese usuario
                new_assignments[min_user].append(contract['contract_id'])
                
                # Actualizar contador
                current_counts[min_user] += 1
            
            logger.info("  Balance FINAL por usuario (después de asignar nuevos):")
            for user_id in settings.DIVISION_USER_IDS:
                fixed_auto = len(fixed_contracts[user_id])
                current_in_range = len(current_assignments_in_range[user_id])
                nuevos = len(new_assignments[user_id])
                logger.info(
                    f"    Usuario {user_id}: {current_counts[user_id]} totales en rango 1-60 = "
                    f"{current_in_range} actuales ({fixed_auto} auto-fijos) + {nuevos} nuevos"
                )
        else:
            logger.info(
                "Paso 2: No hay contratos nuevos para balancear"
            )
        
        # Calcular estadísticas
        logger.info(f"✓ Balanceo completado (SOLO contratos nuevos):")
        total_nuevos = 0
        for user_id in settings.DIVISION_USER_IDS:
            count = len(new_assignments[user_id])
            total_nuevos += count
            porcentaje = (count / len(contracts_new) * 100) if contracts_new else 0
            logger.info(
                f"  - Usuario {user_id}: {count} contratos ({porcentaje:.1f}%)"
            )
        logger.info(f"  - Total nuevos asignados: {total_nuevos} contratos")
        logger.info(
            f"  - Contratos previamente asignados (mantenidos): "
            f"{len(all_currently_assigned)}"
        )
        
        return new_assignments, contracts_days_map
    
    def save_assignments(self, assignments: Dict[int, List[int]]) -> Dict[str, int]:
        """
        Guarda las nuevas asignaciones en la base de datos y en el historial.
        
        OPTIMIZADO: Verifica duplicados en lote para mejor performance.
        
        Args:
            assignments: Diccionario {user_id: [contract_ids]}
        
        Returns:
            Estadísticas de inserción por usuario
        """
        logger.info("Guardando nuevas asignaciones de división...")
        
        stats = {
            'inserted_total': 0,
        }
        # Agregar stats por usuario
        for user_id in settings.DIVISION_USER_IDS:
            stats[f'inserted_user_{user_id}'] = 0
            
        new_assignments = {}  # Para registrar en historial
        
        try:
            # OPTIMIZACIÓN: Obtener TODOS los contratos ya asignados de una sola vez
            all_contract_ids = set()
            for contract_ids in assignments.values():
                all_contract_ids.update(contract_ids)
            
            logger.info(f"Verificando duplicados para {len(all_contract_ids)} contratos únicos...")
            existing_assignments = self.postgres_session.query(
                ContractAdvisor.contract_id
            ).filter(
                ContractAdvisor.contract_id.in_(all_contract_ids)
            ).all()
            
            existing_contract_ids = set(row[0] for row in existing_assignments)
            logger.info(f"Encontrados {len(existing_contract_ids)} contratos ya asignados en BD")
            
            # Insertar solo los que NO existen
            for user_id, contract_ids in assignments.items():
                new_assignments[user_id] = []
                
                for contract_id in contract_ids:
                    if contract_id not in existing_contract_ids:
                        new_assignment = ContractAdvisor(
                            contract_id=contract_id,
                            user_id=user_id
                        )
                        self.postgres_session.add(new_assignment)
                        new_assignments[user_id].append(contract_id)
                        
                        stats['inserted_total'] += 1
                        stats[f'inserted_user_{user_id}'] += 1
            
            logger.info(f"Insertando {stats['inserted_total']} nuevas asignaciones de división...")
            self.postgres_session.commit()
            
            # Registrar en historial con Fecha Inicial
            logger.info("Registrando asignaciones de división en historial...")
            history_stats = self.history_service.register_assignments(new_assignments)
            
            logger.info(f"✓ Asignaciones de división guardadas:")
            logger.info(f"  - Total: {stats['inserted_total']}")
            for user_id in settings.DIVISION_USER_IDS:
                logger.info(f"  - Usuario {user_id}: {stats[f'inserted_user_{user_id}']} contratos")
            logger.info(f"  - Historial registrado: {history_stats['total_registered']} registros")
            
            return stats
        
        except Exception as e:
            logger.error(f"✗ Error al guardar asignaciones de división: {e}")
            self.postgres_session.rollback()
            raise
    
    def ensure_fixed_contracts_assigned(self, fixed_contracts: Dict[int, Set[int]]) -> Dict[str, int]:
        """
        Asegura que TODOS los contratos fijos estén insertados en contract_advisors.
        Si un contrato fijo NO está asignado, lo inserta automáticamente.
        
        Args:
            fixed_contracts: Diccionario {user_id: set(contract_ids)}
        
        Returns:
            Estadísticas de contratos fijos insertados
        """
        logger.info("Verificando que todos los contratos fijos de división estén asignados...")
        
        stats = {'inserted_total': 0, 'already_assigned': 0}
        for user_id in settings.DIVISION_USER_IDS:
            stats[f'inserted_user_{user_id}'] = 0
            
        new_fixed_assignments = {}
        
        try:
            # Obtener TODOS los contratos ya asignados
            all_assigned = self.postgres_session.query(ContractAdvisor.contract_id).all()
            existing_contract_ids = set([row[0] for row in all_assigned])
            
            logger.info(f"Contratos ya asignados en sistema: {len(existing_contract_ids)}")
            
            # Para cada usuario de división, identificar contratos fijos NO asignados
            for user_id in settings.DIVISION_USER_IDS:
                user_fixed_contracts = fixed_contracts.get(user_id, set())
                
                if not user_fixed_contracts:
                    continue
                
                # Identificar contratos fijos que NO existen en ninguna asignación
                missing_fixed = user_fixed_contracts - existing_contract_ids
                
                if missing_fixed:
                    logger.info(f"  Usuario {user_id}: {len(missing_fixed)} contratos fijos sin asignar")
                    new_fixed_assignments[user_id] = list(missing_fixed)
                    
                    # Insertar contratos fijos faltantes
                    for contract_id in missing_fixed:
                        new_assignment = ContractAdvisor(
                            user_id=user_id,
                            contract_id=contract_id
                        )
                        self.postgres_session.add(new_assignment)
                        stats['inserted_total'] += 1
                        stats[f'inserted_user_{user_id}'] += 1
                        
                        # Agregar a existing para evitar duplicados
                        existing_contract_ids.add(contract_id)
                else:
                    already = user_fixed_contracts & existing_contract_ids
                    stats['already_assigned'] += len(already)
            
            if stats['inserted_total'] > 0:
                self.postgres_session.commit()
                
                # Registrar en historial
                logger.info("Registrando contratos fijos de división en historial...")
                history_stats = self.history_service.register_assignments(new_fixed_assignments)
                
                logger.info(f"✓ Contratos fijos de división insertados:")
                logger.info(f"  - Total: {stats['inserted_total']}")
                for user_id in settings.DIVISION_USER_IDS:
                    if stats[f'inserted_user_{user_id}'] > 0:
                        logger.info(f"  - Usuario {user_id}: {stats[f'inserted_user_{user_id}']} contratos")
                logger.info(f"  - Historial: {history_stats['total_registered']} registros")
            else:
                logger.info(f"✓ Todos los contratos fijos de división ya están asignados ({stats['already_assigned']} contratos)")
            
            return stats
        
        except Exception as e:
            logger.error(f"✗ Error al asegurar contratos fijos de división: {e}")
            self.postgres_session.rollback()
            raise
    
    def execute_division_process(self) -> Dict:
        """
        Ejecuta el proceso completo de división de contratos a 8 usuarios.
        
        IMPORTANTE: NO elimina contratos. Solo agrega nuevos.
        
        Este es el método principal que orquesta todo el flujo:
        1. Obtener contratos fijos desde managements
        2. Asegurar que contratos fijos estén insertados en contract_advisors
        3. Obtener asignaciones actuales
        4. Consultar contratos con 1-60 días de atraso
        5. Balancear SOLO contratos NUEVOS (no asignados) equitativamente
        6. Guardar asignaciones con historial
        
        Returns:
            Diccionario con resultados completos del proceso
        """
        logger.info("=" * 80)
        logger.info("INICIANDO PROCESO DE DIVISIÓN DE CONTRATOS (8 USUARIOS)")
        logger.info("MODO: Solo asignación de nuevos contratos (sin eliminar)")
        logger.info("RANGO: Día 1 al 60 de atraso")
        logger.info("=" * 80)
        
        results = {
            'success': False,
            'fixed_contracts': {},
            'fixed_inserted_stats': {},
            'contracts_to_assign': [],
            'balance_stats': {},
            'insert_stats': {},
            'final_assignments': {},
            'error': None
        }
        
        try:
            # 1. Obtener contratos fijos
            fixed_contracts = self.get_fixed_contracts()
            results['fixed_contracts'] = {
                k: list(v) for k, v in fixed_contracts.items()
            }
            
            # 2. Asegurar que todos los contratos fijos estén insertados
            fixed_insert_stats = self.ensure_fixed_contracts_assigned(
                fixed_contracts
            )
            results['fixed_inserted_stats'] = fixed_insert_stats
            
            # 3. Obtener asignaciones actuales
            current_assignments = self.get_current_assignments()
            
            # 4. Obtener contratos entre 1 y 60 días de atraso
            contracts_with_arrears = self.get_contracts_for_division()
            contract_ids = [c['contract_id'] for c in contracts_with_arrears]
            results['contracts_to_assign'] = contract_ids
            
            # 5. Balanceo SOLO de contratos nuevos
            new_assignments, contracts_days_map = self.balance_assignments(
                contracts_with_arrears,
                fixed_contracts,
                current_assignments
            )
            results['balance_stats'] = {
                k: len(v) for k, v in new_assignments.items()
            }
            results['contracts_days_map'] = contracts_days_map
            
            # 6. Guardar asignaciones
            insert_stats = self.save_assignments(new_assignments)
            results['insert_stats'] = insert_stats
            
            # 7. Resultado final
            results['final_assignments'] = {
                k: v for k, v in new_assignments.items()
            }
            results['success'] = True
            
            logger.info("=" * 80)
            logger.info("✓ PROCESO DE DIVISIÓN DE CONTRATOS COMPLETADO EXITOSAMENTE")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"✗ Error en el proceso de división de contratos: {e}")
            results['error'] = str(e)
            raise
        
        return results
