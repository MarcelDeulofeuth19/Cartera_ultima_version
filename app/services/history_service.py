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
        OPTIMIZADO: Verifica duplicados en lote para mejor performance.
        
        Args:
            assignments: Diccionario {user_id: [contract_ids]}
        
        Returns:
            Estadísticas de inserciones: {'total_registered': X, 'cobyser': Y, 'serlefin': Z}
        """
        logger.info("Registrando nuevas asignaciones en historial...")
        
        stats = {'total_registered': 0, 'cobyser': 0, 'serlefin': 0}
        fecha_actual = datetime.now()
        
        try:
            # OPTIMIZACIÓN: Obtener TODOS los registros activos de una sola vez
            all_pairs = []
            for user_id, contract_ids in assignments.items():
                for contract_id in contract_ids:
                    all_pairs.append((contract_id, user_id))
            
            if not all_pairs:
                logger.info("No hay asignaciones para registrar")
                return stats
            
            logger.info(f"Verificando {len(all_pairs)} registros activos en historial...")
            
            # Extraer todos los contract_ids para la query
            all_contract_ids = [pair[0] for pair in all_pairs]
            
            # Query batch: obtener todos los registros activos (sin fecha_terminal)
            existing_active = self.postgres_session.query(
                ContractAdvisorHistory.contract_id,
                ContractAdvisorHistory.user_id
            ).filter(
                and_(
                    ContractAdvisorHistory.contract_id.in_(all_contract_ids),
                    ContractAdvisorHistory.fecha_terminal.is_(None)
                )
            ).all()
            
            # Crear set de tuplas (contract_id, user_id) activas
            existing_pairs = set((row[0], row[1]) for row in existing_active)
            logger.info(f"Encontrados {len(existing_pairs)} registros activos existentes")
            
            # Insertar solo los que NO tienen registro activo
            new_history_records = []
            for contract_id, user_id in all_pairs:
                if (contract_id, user_id) not in existing_pairs:
                    new_history_records.append({
                        'user_id': user_id,
                        'contract_id': contract_id,
                        'fecha_inicial': fecha_actual,
                        'fecha_terminal': None
                    })
                    
                    stats['total_registered'] += 1
                    
                    # Clasificar por casa de cobranza
                    if user_id in settings.COBYSER_USERS:
                        stats['cobyser'] += 1
                    elif user_id in settings.SERLEFIN_USERS:
                        stats['serlefin'] += 1
            
            # Bulk insert
            if new_history_records:
                logger.info(f"Insertando {len(new_history_records)} nuevos registros en historial...")
                self.postgres_session.bulk_insert_mappings(ContractAdvisorHistory, new_history_records)
            
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
        
        OPTIMIZADO: Hace UPDATE en lote + INSERT para contratos sin historial previo.
        
        Se cierra cuando:
        - El contrato tiene menos de 61 días de atraso
        - El contrato NO tiene effect='pago_total'
        
        Args:
            contracts_removed: Diccionario {user_id: [contract_ids]} de contratos eliminados
        
        Returns:
            Estadísticas de cierres: {'total_closed': X, 'updated': Y, 'inserted': Z}
        """
        logger.info("Cerrando asignaciones en historial (Fecha Terminal)...")
        
        stats = {'total_closed': 0, 'updated': 0, 'inserted': 0, 'cobyser': 0, 'serlefin': 0}
        fecha_actual = datetime.now()
        
        total_contracts = sum(len(contracts) for contracts in contracts_removed.values())
        logger.info(f"Procesando {total_contracts} contratos eliminados para cerrar historial...")
        
        try:
            # Procesar en LOTE por usuario para mejor performance
            for user_id, contract_ids in contracts_removed.items():
                if not contract_ids:
                    continue
                
                logger.info(f"  Usuario {user_id}: Procesando {len(contract_ids)} contratos...")
                
                # PASO 1: Obtener contratos que YA tienen historial abierto
                existing_contracts = set(
                    row[0] for row in self.postgres_session.query(ContractAdvisorHistory.contract_id).filter(
                        and_(
                            ContractAdvisorHistory.user_id == user_id,
                            ContractAdvisorHistory.contract_id.in_(contract_ids),
                            ContractAdvisorHistory.fecha_terminal.is_(None)
                        )
                    ).distinct()
                )
                
                # PASO 2: UPDATE en LOTE para los que tienen historial
                if existing_contracts:
                    updated_count = self.postgres_session.query(ContractAdvisorHistory).filter(
                        and_(
                            ContractAdvisorHistory.user_id == user_id,
                            ContractAdvisorHistory.contract_id.in_(existing_contracts),
                            ContractAdvisorHistory.fecha_terminal.is_(None)
                        )
                    ).update(
                        {ContractAdvisorHistory.fecha_terminal: fecha_actual},
                        synchronize_session=False
                    )
                    
                    stats['updated'] += updated_count
                    logger.info(f"    - {updated_count} registros actualizados (tenían historial previo)")
                
                # PASO 3: INSERT para contratos sin historial previo
                contracts_without_history = set(contract_ids) - existing_contracts
                
                if contracts_without_history:
                    for contract_id in contracts_without_history:
                        new_history = ContractAdvisorHistory(
                            user_id=user_id,
                            contract_id=contract_id,
                            fecha_inicial=None,  # No hay fecha inicial (sistema nuevo)
                            fecha_terminal=fecha_actual
                        )
                        self.postgres_session.add(new_history)
                    
                    stats['inserted'] += len(contracts_without_history)
                    logger.info(f"    - {len(contracts_without_history)} registros insertados (sin historial previo)")
                
                # Total para este usuario
                stats['total_closed'] += len(contract_ids)
                
                # Clasificar por casa de cobranza
                if user_id in settings.COBYSER_USERS:
                    stats['cobyser'] += len(contract_ids)
                elif user_id in settings.SERLEFIN_USERS:
                    stats['serlefin'] += len(contract_ids)
            
            self.postgres_session.commit()
            
            logger.info(f"✓ Asignaciones cerradas en historial:")
            logger.info(f"  - Total contratos procesados: {total_contracts}")
            logger.info(f"  - Registros actualizados (con historial previo): {stats['updated']}")
            logger.info(f"  - Registros insertados (sin historial previo): {stats['inserted']}")
            logger.info(f"  - Cobyser: {stats['cobyser']}")
            logger.info(f"  - Serlefin: {stats['serlefin']}")
            
            return stats
        
        except Exception as e:
            logger.error(f"✗ Error al cerrar asignaciones en historial: {e}")
            logger.error(f"Detalles del error: {str(e)}")
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
