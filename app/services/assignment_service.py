"""
Servicio principal de asignación de contratos.
Implementa la lógica de contratos fijos, limpieza y balanceo 50/50.
"""
import logging
from typing import List, Dict, Set, Tuple
from sqlalchemy.orm import Session
from app.core.config import settings
from app.database.models import ContractAdvisor, Management
from app.services.contract_service import ContractService
from app.services.history_service import HistoryService

logger = logging.getLogger(__name__)


class AssignmentService:
    """
    Servicio de asignación de contratos a asesores.
    Implementa la lógica de contratos fijos, limpieza y balanceo.
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
        
        # Variables de control para balanceo
        self._last_assigned_user = settings.USER_IDS[1]  # Empieza con 81
    
    def get_fixed_contracts(self) -> Dict[int, Set[int]]:
        """
        Obtiene los contratos FIJOS desde la tabla managements.
        
        Un contrato es FIJO si tiene effect='pago_total' para usuarios de:
        - COBYSER: 45, 46, 47, 48, 49, 50, 51
        - SERLEFIN: 81, 82, 83, 84, 85, 86, 102, 103
        
        Returns:
            Diccionario {user_id: set(contract_ids)}
        """
        logger.info("Consultando contratos fijos desde managements...")
        
        fixed_contracts = {user_id: set() for user_id in settings.all_users}
        
        try:
            # Query para obtener contratos con effect='pago_total'
            managements = self.postgres_session.query(Management).filter(
                Management.effect == settings.FIXED_CONTRACT_EFFECT,
                Management.user_id.in_(settings.all_users)
            ).all()
            
            for mgmt in managements:
                if mgmt.user_id in fixed_contracts:
                    fixed_contracts[mgmt.user_id].add(mgmt.contract_id)
            
            # Agrupar por casa de cobranza
            cobyser_total = sum(len(contracts) for uid, contracts in fixed_contracts.items() if uid in settings.COBYSER_USERS)
            serlefin_total = sum(len(contracts) for uid, contracts in fixed_contracts.items() if uid in settings.SERLEFIN_USERS)
            
            logger.info(f"✓ Contratos fijos encontrados:")
            logger.info(f"  - COBYSER: {cobyser_total} contratos")
            logger.info(f"  - SERLEFIN: {serlefin_total} contratos")
            logger.info(f"  - Total: {cobyser_total + serlefin_total}")
            
            return fixed_contracts
        
        except Exception as e:
            logger.error(f"✗ Error al consultar contratos fijos: {e}")
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
                ContractAdvisor.advisor_id.in_(settings.USER_IDS)
            ).all()
            
            for assignment in assignments:
                if assignment.advisor_id in current_assignments:
                    current_assignments[assignment.advisor_id].add(assignment.contract_id)
            
            logger.info(f"✓ Asignaciones actuales: Usuario 45: {len(current_assignments[45])}, Usuario 81: {len(current_assignments[81])}")
            
            return current_assignments
        
        except Exception as e:
            logger.error(f"✗ Error al consultar asignaciones actuales: {e}")
            raise
    
    def clean_assignments(self, fixed_contracts: Dict[int, Set[int]]) -> Dict[str, int]:
        """
        Limpia las asignaciones según las reglas:
        - Elimina contratos con 0-60 días de atraso
        - NO elimina contratos fijos
        - Registra Fecha Terminal en el historial
        
        Args:
            fixed_contracts: Diccionario de contratos fijos por usuario
        
        Returns:
            Diccionario con estadísticas de limpieza
        """
        logger.info("Iniciando limpieza de asignaciones...")
        
        stats = {'deleted_total': 0, 'deleted_cobyser': 0, 'deleted_serlefin': 0, 'protected_fixed': 0}
        contracts_removed = {}  # Para registrar en historial
        
        try:
            # Obtener contratos con 0-60 días de atraso
            contracts_to_clean = set(self.contract_service.get_contracts_in_range(0, 60))
            logger.info(f"Contratos candidatos para limpieza (0-60 días): {len(contracts_to_clean)}")
            
            for user_id in settings.all_users:
                contracts_removed[user_id] = []
                
                # Obtener asignaciones actuales del usuario
                current = self.postgres_session.query(ContractAdvisor).filter(
                    ContractAdvisor.advisor_id == user_id,
                    ContractAdvisor.contract_id.in_(contracts_to_clean)
                ).all()
                
                for assignment in current:
                    # Proteger contratos fijos
                    if assignment.contract_id in fixed_contracts.get(user_id, set()):
                        stats['protected_fixed'] += 1
                        logger.debug(f"  Protegido (fijo): Contrato {assignment.contract_id} - Usuario {user_id}")
                        continue
                    
                    # Eliminar si NO es fijo
                    self.postgres_session.delete(assignment)
                    contracts_removed[user_id].append(assignment.contract_id)
                    stats['deleted_total'] += 1
                    
                    # Clasificar por casa de cobranza
                    if user_id in settings.COBYSER_USERS:
                        stats['deleted_cobyser'] += 1
                    elif user_id in settings.SERLEFIN_USERS:
                        stats['deleted_serlefin'] += 1
            
            self.postgres_session.commit()
            
            # Registrar Fecha Terminal en historial
            logger.info("Actualizando historial con Fecha Terminal...")
            history_stats = self.history_service.close_assignments(contracts_removed)
            
            logger.info(f"✓ Limpieza completada:")
            logger.info(f"  - Total eliminados: {stats['deleted_total']}")
            logger.info(f"  - COBYSER: {stats['deleted_cobyser']}")
            logger.info(f"  - SERLEFIN: {stats['deleted_serlefin']}")
            logger.info(f"  - Protegidos (fijos): {stats['protected_fixed']}")
            logger.info(f"  - Historial cerrado: {history_stats['total_closed']} registros")
            
            return stats
        
        except Exception as e:
            logger.error(f"✗ Error durante la limpieza: {e}")
            self.postgres_session.rollback()
            raise
    
    def balance_assignments(
        self, 
        contracts_to_assign: List[int], 
        fixed_contracts: Dict[int, Set[int]],
        current_assignments: Dict[int, Set[int]]
    ) -> Dict[int, List[int]]:
        """
        Balancea la asignación de contratos 50/50 entre usuarios 45 y 81.
        
        Lógica:
        1. Prioriza asignar contratos fijos que no estén asignados
        2. Balancea nuevos contratos equitativamente
        3. Si hay número impar, alterna el usuario que recibe el extra
        
        Args:
            contracts_to_assign: Lista de IDs de contratos a asignar
            fixed_contracts: Contratos fijos por usuario
            current_assignments: Asignaciones actuales por usuario
        
        Returns:
            Diccionario {user_id: [contract_ids]}
        """
        logger.info(f"Iniciando balanceo de {len(contracts_to_assign)} contratos...")
        
        new_assignments = {45: [], 81: []}
        contracts_set = set(contracts_to_assign)
        
        # Paso 1: Asignar contratos fijos que no estén asignados
        logger.info("Paso 1: Asignando contratos fijos no asignados...")
        for user_id in settings.USER_IDS:
            fixed_not_assigned = fixed_contracts[user_id] - current_assignments[user_id]
            fixed_to_add = fixed_not_assigned & contracts_set
            
            for contract_id in fixed_to_add:
                new_assignments[user_id].append(contract_id)
                contracts_set.remove(contract_id)
            
            if fixed_to_add:
                logger.info(f"  Usuario {user_id}: {len(fixed_to_add)} contratos fijos agregados")
        
        # Paso 2: Balancear contratos restantes 50/50
        logger.info(f"Paso 2: Balanceando {len(contracts_set)} contratos restantes...")
        remaining_contracts = list(contracts_set)
        
        # Distribuir de forma alternada
        for i, contract_id in enumerate(remaining_contracts):
            # Alternar entre usuarios
            if i % 2 == 0:
                assigned_user = settings.USER_IDS[0]  # 45
            else:
                assigned_user = settings.USER_IDS[1]  # 81
            
            new_assignments[assigned_user].append(contract_id)
        
        # Si hay número impar, usar la variable de control para balancear
        if len(remaining_contracts) % 2 != 0:
            # La próxima vez empezará con el otro usuario
            self._last_assigned_user = settings.USER_IDS[0] if self._last_assigned_user == settings.USER_IDS[1] else settings.USER_IDS[1]
        
        logger.info(f"✓ Balanceo completado:")
        logger.info(f"  - Usuario 45: {len(new_assignments[45])} contratos")
        logger.info(f"  - Usuario 81: {len(new_assignments[81])} contratos")
        logger.info(f"  - Diferencia: {abs(len(new_assignments[45]) - len(new_assignments[81]))}")
        
        return new_assignments
    
    def save_assignments(self, assignments: Dict[int, List[int]]) -> Dict[str, int]:
        """
        Guarda las nuevas asignaciones en la base de datos y en el historial.
        
        Args:
            assignments: Diccionario {user_id: [contract_ids]}
        
        Returns:
            Estadísticas de inserción
        """
        logger.info("Guardando nuevas asignaciones...")
        
        stats = {'inserted_total': 0, 'inserted_cobyser': 0, 'inserted_serlefin': 0}
        new_assignments = {}  # Para registrar en historial
        
        try:
            for user_id, contract_ids in assignments.items():
                new_assignments[user_id] = []
                
                for contract_id in contract_ids:
                    # Verificar si ya existe para evitar duplicados
                    existing = self.postgres_session.query(ContractAdvisor).filter(
                        ContractAdvisor.contract_id == contract_id,
                        ContractAdvisor.advisor_id == user_id
                    ).first()
                    
                    if not existing:
                        new_assignment = ContractAdvisor(
                            contract_id=contract_id,
                            advisor_id=user_id
                        )
                        self.postgres_session.add(new_assignment)
                        new_assignments[user_id].append(contract_id)
                        
                        stats['inserted_total'] += 1
                        
                        # Clasificar por casa de cobranza
                        if user_id in settings.COBYSER_USERS:
                            stats['inserted_cobyser'] += 1
                        elif user_id in settings.SERLEFIN_USERS:
                            stats['inserted_serlefin'] += 1
            
            self.postgres_session.commit()
            
            # Registrar en historial con Fecha Inicial
            logger.info("Registrando asignaciones en historial con Fecha Inicial...")
            history_stats = self.history_service.register_assignments(new_assignments)
            
            logger.info(f"✓ Asignaciones guardadas:")
            logger.info(f"  - Total: {stats['inserted_total']}")
            logger.info(f"  - COBYSER: {stats['inserted_cobyser']}")
            logger.info(f"  - SERLEFIN: {stats['inserted_serlefin']}")
            logger.info(f"  - Historial registrado: {history_stats['total_registered']} registros")
            
            return stats
        
        except Exception as e:
            logger.error(f"✗ Error al guardar asignaciones: {e}")
            self.postgres_session.rollback()
            raise
    
    def execute_assignment_process(self) -> Dict:
        """
        Ejecuta el proceso completo de asignación de contratos.
        
        Este es el método principal que orquesta todo el flujo:
        1. Obtener contratos fijos
        2. Obtener asignaciones actuales
        3. Consultar contratos con >= 61 días de atraso
        4. Limpiar asignaciones (0-60 días)
        5. Balancear y asignar contratos
        
        Returns:
            Diccionario con resultados completos del proceso
        """
        logger.info("=" * 80)
        logger.info("INICIANDO PROCESO DE ASIGNACIÓN DE CONTRATOS")
        logger.info("=" * 80)
        
        results = {
            'success': False,
            'fixed_contracts': {},
            'contracts_to_assign': [],
            'clean_stats': {},
            'balance_stats': {},
            'insert_stats': {},
            'final_assignments': {},
            'error': None
        }
        
        try:
            # 1. Obtener contratos fijos
            fixed_contracts = self.get_fixed_contracts()
            results['fixed_contracts'] = {k: list(v) for k, v in fixed_contracts.items()}
            
            # 2. Obtener asignaciones actuales
            current_assignments = self.get_current_assignments()
            
            # 3. Obtener contratos con >= 61 días de atraso
            contracts_with_arrears = self.contract_service.get_contracts_with_arrears()
            contract_ids = [c['contract_id'] for c in contracts_with_arrears]
            results['contracts_to_assign'] = contract_ids
            
            # 4. Limpieza de asignaciones
            clean_stats = self.clean_assignments(fixed_contracts)
            results['clean_stats'] = clean_stats
            
            # 5. Balanceo y asignación
            new_assignments = self.balance_assignments(
                contract_ids,
                fixed_contracts,
                current_assignments
            )
            results['balance_stats'] = {k: len(v) for k, v in new_assignments.items()}
            
            # 6. Guardar asignaciones
            insert_stats = self.save_assignments(new_assignments)
            results['insert_stats'] = insert_stats
            
            # 7. Resultado final
            results['final_assignments'] = {k: v for k, v in new_assignments.items()}
            results['success'] = True
            
            logger.info("=" * 80)
            logger.info("✓ PROCESO DE ASIGNACIÓN COMPLETADO EXITOSAMENTE")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"✗ Error en el proceso de asignación: {e}")
            results['error'] = str(e)
            raise
        
        return results
