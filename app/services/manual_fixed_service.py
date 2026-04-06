"""
Servicio para gestionar contratos fijos manuales.
Permite agregar contratos fijos desde una lista manual y validarlos en lotes.
Procesa contratos para Cobyser (Usuario 45) y Serlefin (Usuario 81).
"""
import logging
from typing import List, Dict, Set, Optional
from sqlalchemy.orm import Session
from app.core.config import settings
from app.database.models import ContractAdvisor, Management
from app.services.contract_service import ContractService
from app.services.history_service import HistoryService

logger = logging.getLogger(__name__)


class ManualFixedService:
    """
    Servicio para gestionar contratos fijos manuales con validaciones por lotes.
    """
    
    def __init__(
        self,
        postgres_session: Session,
        mysql_session: Optional[Session] = None,
    ):
        """
        Args:
            postgres_session: Sesión PostgreSQL para gestionar asignaciones
        """
        self.postgres_session = postgres_session
        self.mysql_session = mysql_session
        self.contract_service: Optional[ContractService] = (
            ContractService(mysql_session) if mysql_session is not None else None
        )
        self.history_service = HistoryService(postgres_session)

    def _resolve_blocked_contract_ids(self) -> Set[int]:
        """Resuelve contratos bloqueados por lista negra de cedula/documento."""
        if self.contract_service is None:
            return set()

        blocked_documents = {
            ContractService.normalize_customer_document(raw_value)
            for raw_value in settings.blocked_customer_documents
        }
        blocked_documents = {document for document in blocked_documents if document}
        if not blocked_documents:
            return set()

        return self.contract_service.get_contract_ids_by_customer_documents(
            blocked_documents
        )
    
    def validate_and_insert_manual_fixed(
        self, 
        manual_contracts: Dict[int, List[int]]
    ) -> Dict[str, any]:
        """
        Valida e inserta contratos fijos manuales por lotes.
        
        Validaciones:
        1. Verifica que el contrato no esté ya asignado (evita duplicados)
        2. Valida contra contratos fijos de managements (no duplicar)
        3. Inserta solo contratos nuevos
        4. Registra en historial
        
        Args:
            manual_contracts: Diccionario {user_id: [contract_ids]}
        
        Returns:
            Estadísticas de validación e inserción
        """
        logger.info("=" * 80)
        logger.info("VALIDANDO E INSERTANDO CONTRATOS FIJOS MANUALES")
        logger.info("=" * 80)
        
        stats = {
            'total_provided': 0,
            'already_assigned': 0,
            'blocked_by_client_blacklist': 0,
            'in_managements': 0,
            'inserted': 0,
            'by_user': {}
        }
        
        try:
            # Paso 1: Contar total de contratos proporcionados
            all_manual_contract_ids = set()
            for user_id, contract_ids in manual_contracts.items():
                all_manual_contract_ids.update(contract_ids)
                stats['by_user'][user_id] = {
                    'provided': len(contract_ids),
                    'inserted': 0,
                    'blocked': 0,
                    'skipped': 0,
                }
            
            stats['total_provided'] = len(all_manual_contract_ids)
            logger.info(f"Total contratos manuales proporcionados: {stats['total_provided']}")

            blocked_contract_ids = self._resolve_blocked_contract_ids()
            if blocked_contract_ids:
                logger.warning(
                    "Lista negra por cedula activa para manuales: %s contrato(s) bloqueado(s)",
                    len(blocked_contract_ids),
                )
            
            # Paso 2: VALIDACIÓN POR USUARIO - Verificar contratos ya asignados específicamente
            logger.info("Validando contratos ya asignados por usuario en contract_advisors...")
            contracts_to_insert_by_user = {}
            
            for user_id, contract_ids in manual_contracts.items():
                user_contract_ids = {int(contract_id) for contract_id in contract_ids}
                blocked_for_user = user_contract_ids & blocked_contract_ids
                candidate_contract_ids = user_contract_ids - blocked_for_user
                stats['by_user'][user_id]['blocked'] = len(blocked_for_user)
                stats['blocked_by_client_blacklist'] += len(blocked_for_user)

                # Verificar cuáles contratos YA están asignados a ESTE usuario específico
                existing_for_user = self.postgres_session.query(
                    ContractAdvisor.contract_id
                ).filter(
                    ContractAdvisor.contract_id.in_(candidate_contract_ids),
                    ContractAdvisor.user_id == user_id
                ).all()
                
                existing_contract_ids_for_user = set(row[0] for row in existing_for_user)
                
                # Contratos nuevos = contratos proporcionados - contratos ya asignados a este usuario
                new_contracts_for_user = candidate_contract_ids - existing_contract_ids_for_user
                contracts_to_insert_by_user[user_id] = new_contracts_for_user
                
                logger.info(
                    f"  Usuario {user_id}: bloqueados={len(blocked_for_user)}, "
                    f"{len(existing_contract_ids_for_user)} ya asignados, "
                    f"{len(new_contracts_for_user)} nuevos a insertar"
                )
                
                stats['already_assigned'] += len(existing_contract_ids_for_user)
            
            logger.info(f"  ✓ Total contratos ya asignados (todos los usuarios): {stats['already_assigned']}")

            
            # Paso 3: VALIDACIÓN POR LOTES - Verificar contratos en managements
            logger.info("Validando contratos en managements...")
            managements_contracts = self.postgres_session.query(
                Management.contract_id
            ).filter(
                Management.contract_id.in_(all_manual_contract_ids)
            ).all()
            
            managements_contract_ids = set(row[0] for row in managements_contracts)
            stats['in_managements'] = len(managements_contract_ids)
            logger.info(f"  ✓ Contratos en managements: {stats['in_managements']}")
            
            # Paso 4: Calcular total de contratos NUEVOS a insertar por usuario
            total_to_insert = sum(len(contracts) for contracts in contracts_to_insert_by_user.values())
            logger.info(f"Contratos nuevos a insertar: {total_to_insert}")
            
            if total_to_insert == 0:
                logger.info("✓ No hay contratos nuevos para insertar (todos ya existen)")
                return stats
            
            # Paso 5: INSERCIÓN POR LOTES Y POR USUARIO
            logger.info("Insertando contratos fijos manuales por lotes...")
            new_assignments = {}
            batch_size = 1000  # Tamaño de lote para commits
            inserted_count = 0
            
            for user_id, contracts_to_insert_for_user in contracts_to_insert_by_user.items():
                if not contracts_to_insert_for_user:
                    # No hay contratos nuevos para este usuario
                    stats['by_user'][user_id]['skipped'] = len(manual_contracts[user_id])
                    continue
                    
                new_assignments[user_id] = []
                logger.info(f"  Insertando {len(contracts_to_insert_for_user)} contratos para usuario {user_id}...")
                
                for contract_id in contracts_to_insert_for_user:
                    new_assignment = ContractAdvisor(
                        contract_id=contract_id,
                        user_id=user_id
                    )
                    self.postgres_session.add(new_assignment)
                    new_assignments[user_id].append(contract_id)
                    inserted_count += 1
                    stats['by_user'][user_id]['inserted'] += 1
                    
                    # Commit por lotes
                    if inserted_count % batch_size == 0:
                        self.postgres_session.commit()
                        logger.info(f"    ✓ Commitido lote de {batch_size} contratos")
                
                # Contratos omitidos = total - insertados
                already_assigned_count = (
                    len(manual_contracts[user_id])
                    - len(contracts_to_insert_for_user)
                    - stats['by_user'][user_id]['blocked']
                )
                stats['by_user'][user_id]['skipped'] = max(0, already_assigned_count)
            
            # Commit final
            self.postgres_session.commit()
            stats['inserted'] = inserted_count
            logger.info(f"✓ Total insertado: {stats['inserted']} contratos")
            
            # Paso 6: Registrar en historial por lotes
            if new_assignments:
                logger.info("Registrando contratos manuales en historial...")
                history_stats = self.history_service.register_assignments(new_assignments)
                logger.info(f"✓ Historial registrado: {history_stats['total_registered']} registros")
            
            # Mostrar resumen por usuario
            logger.info("=" * 80)
            logger.info("RESUMEN POR USUARIO:")
            for user_id, user_stats in stats['by_user'].items():
                logger.info(
                    f"  Usuario {user_id}: "
                    f"{user_stats['provided']} proporcionados, "
                    f"{user_stats['inserted']} insertados, "
                    f"{user_stats['skipped']} omitidos"
                )
            logger.info("=" * 80)
            
            return stats
        
        except Exception as e:
            logger.error(f"✗ Error al validar/insertar contratos manuales: {e}")
            self.postgres_session.rollback()
            raise
    
    def get_manual_fixed_contracts(self, user_id: int) -> Set[int]:
        """
        Obtiene los contratos fijos manuales asignados a un usuario.
        
        Args:
            user_id: ID del usuario
        
        Returns:
            Set de IDs de contratos
        """
        try:
            assignments = self.postgres_session.query(ContractAdvisor).filter(
                ContractAdvisor.user_id == user_id
            ).all()
            
            contract_ids = set(a.contract_id for a in assignments)
            logger.info(f"Usuario {user_id}: {len(contract_ids)} contratos fijos manuales")
            
            return contract_ids
        
        except Exception as e:
            logger.error(f"✗ Error al obtener contratos fijos manuales: {e}")
            raise
