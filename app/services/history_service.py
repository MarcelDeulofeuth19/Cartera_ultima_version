"""
Servicio de gestión de historial de asignaciones.
Maneja los INSERT y UPDATE en contract_advisors_history.
"""
import logging
from datetime import datetime
from typing import List, Dict, Set
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.database.models import ContractAdvisorHistory
from app.core.config import settings

logger = logging.getLogger(__name__)


class HistoryService:
    """
    Servicio para gestionar el historial de asignaciones de contratos.
    
    Responsabilidades:
    - INSERT: Registrar fecha inicial cuando se asigna un contrato
    - UPDATE: Registrar fecha terminal cuando se remueve un contrato
    """
    
    def __init__(self, postgres_session: Session):
        """
        Args:
            postgres_session: Sesión de PostgreSQL
        """
        self.postgres_session = postgres_session
    
    def register_assignments(
        self, 
        assignments: Dict[int, List[int]]
    ) -> Dict[str, int]:
        """
        Registra nuevas asignaciones en el historial con Fecha Inicial = hoy.
        
        Args:
            assignments: Diccionario {user_id: [contract_ids]}
        
        Returns:
            Estadísticas de inserciones: {'total_registered': X, 'cobyser': Y, 'serlefin': Z}
        """
        logger.info("Registrando nuevas asignaciones en historial...")
        
        stats = {'total_registered': 0, 'cobyser': 0, 'serlefin': 0}
        fecha_actual = datetime.now()
        
        try:
            for user_id, contract_ids in assignments.items():
                for contract_id in contract_ids:
                    # Verificar si ya existe un registro activo (sin Fecha Terminal)
                    existing = self.postgres_session.query(ContractAdvisorHistory).filter(
                        and_(
                            ContractAdvisorHistory.contract_id == contract_id,
                            ContractAdvisorHistory.user_id == user_id,
                            ContractAdvisorHistory.fecha_terminal.is_(None)
                        )
                    ).first()
                    
                    if not existing:
                        # Crear nuevo registro en historial
                        new_history = ContractAdvisorHistory(
                            user_id=user_id,
                            contract_id=contract_id,
                            fecha_inicial=fecha_actual,
                            fecha_terminal=None
                        )
                        self.postgres_session.add(new_history)
                        
                        stats['total_registered'] += 1
                        
                        # Clasificar por casa de cobranza
                        if user_id in settings.COBYSER_USERS:
                            stats['cobyser'] += 1
                        elif user_id in settings.SERLEFIN_USERS:
                            stats['serlefin'] += 1
            
            self.postgres_session.commit()
            
            logger.info(f"✓ Historial registrado:")
            logger.info(f"  - Total: {stats['total_registered']}")
            logger.info(f"  - Cobyser: {stats['cobyser']}")
            logger.info(f"  - Serlefin: {stats['serlefin']}")
            
            return stats
        
        except Exception as e:
            logger.error(f"✗ Error al registrar historial: {e}")
            self.postgres_session.rollback()
            raise
    
    def close_assignments(
        self, 
        contracts_removed: Dict[int, List[int]]
    ) -> Dict[str, int]:
        """
        Cierra asignaciones en el historial actualizando Fecha Terminal = hoy.
        
        Se cierra cuando:
        - El contrato tiene menos de 61 días de atraso
        - El contrato NO tiene effect='pago_total'
        
        Args:
            contracts_removed: Diccionario {user_id: [contract_ids]} de contratos eliminados
        
        Returns:
            Estadísticas de cierres: {'total_closed': X, 'cobyser': Y, 'serlefin': Z}
        """
        logger.info("Cerrando asignaciones en historial (Fecha Terminal)...")
        
        stats = {'total_closed': 0, 'cobyser': 0, 'serlefin': 0}
        fecha_actual = datetime.now()
        
        try:
            for user_id, contract_ids in contracts_removed.items():
                for contract_id in contract_ids:
                    # Buscar registros activos (sin Fecha Terminal)
                    active_records = self.postgres_session.query(ContractAdvisorHistory).filter(
                        and_(
                            ContractAdvisorHistory.contract_id == contract_id,
                            ContractAdvisorHistory.user_id == user_id,
                            ContractAdvisorHistory.fecha_terminal.is_(None)
                        )
                    ).all()
                    
                    for record in active_records:
                        record.fecha_terminal = fecha_actual
                        stats['total_closed'] += 1
                        
                        # Clasificar por casa de cobranza
                        if user_id in settings.COBYSER_USERS:
                            stats['cobyser'] += 1
                        elif user_id in settings.SERLEFIN_USERS:
                            stats['serlefin'] += 1
            
            self.postgres_session.commit()
            
            logger.info(f"✓ Asignaciones cerradas en historial:")
            logger.info(f"  - Total: {stats['total_closed']}")
            logger.info(f"  - Cobyser: {stats['cobyser']}")
            logger.info(f"  - Serlefin: {stats['serlefin']}")
            
            return stats
        
        except Exception as e:
            logger.error(f"✗ Error al cerrar asignaciones en historial: {e}")
            self.postgres_session.rollback()
            raise
    
    def get_active_assignments(self, user_ids: List[int] = None) -> Dict[int, Set[int]]:
        """
        Obtiene las asignaciones activas (sin Fecha Terminal) del historial.
        
        Args:
            user_ids: Lista de IDs de usuarios a consultar (None = todos)
        
        Returns:
            Diccionario {user_id: set(contract_ids)}
        """
        logger.info("Consultando asignaciones activas del historial...")
        
        try:
            query = self.postgres_session.query(ContractAdvisorHistory).filter(
                ContractAdvisorHistory.fecha_terminal.is_(None)
            )
            
            if user_ids:
                query = query.filter(ContractAdvisorHistory.user_id.in_(user_ids))
            
            records = query.all()
            
            active_assignments = {}
            for record in records:
                if record.user_id not in active_assignments:
                    active_assignments[record.user_id] = set()
                active_assignments[record.user_id].add(record.contract_id)
            
            total_active = sum(len(contracts) for contracts in active_assignments.values())
            logger.info(f"✓ Asignaciones activas encontradas: {total_active}")
            
            return active_assignments
        
        except Exception as e:
            logger.error(f"✗ Error al consultar historial activo: {e}")
            raise
    
    def get_history_stats(self) -> Dict:
        """
        Obtiene estadísticas generales del historial.
        
        Returns:
            Diccionario con estadísticas
        """
        try:
            # Total de registros
            total_records = self.postgres_session.query(ContractAdvisorHistory).count()
            
            # Registros activos
            active_records = self.postgres_session.query(ContractAdvisorHistory).filter(
                ContractAdvisorHistory.fecha_terminal.is_(None)
            ).count()
            
            # Registros cerrados
            closed_records = self.postgres_session.query(ContractAdvisorHistory).filter(
                ContractAdvisorHistory.fecha_terminal.isnot(None)
            ).count()
            
            stats = {
                'total_records': total_records,
                'active_assignments': active_records,
                'closed_assignments': closed_records
            }
            
            logger.info(f"Estadísticas del historial: {stats}")
            return stats
        
        except Exception as e:
            logger.error(f"✗ Error al obtener estadísticas del historial: {e}")
            raise
