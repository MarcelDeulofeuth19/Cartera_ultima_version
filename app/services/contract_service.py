"""
Servicio de consulta de contratos desde MySQL.
Obtiene contratos con atraso >= 61 días desde alocreditprod.
"""
import logging
from typing import List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.core.config import settings

logger = logging.getLogger(__name__)


class ContractService:
    """
    Servicio para consultar contratos con atraso desde MySQL (alocreditprod).
    """
    
    def __init__(self, mysql_session: Session):
        """
        Args:
            mysql_session: Sesión de SQLAlchemy para MySQL
        """
        self.mysql_session = mysql_session
    
    def get_contracts_with_arrears(self, min_days: int = None, max_days: int = None) -> List[Dict]:
        """
        Obtiene todos los contratos con días de atraso entre min_days y max_days.
        
        Consulta las tablas contract_amortization y contract_status para
        calcular los días de atraso actuales.
        
        Args:
            min_days: Días mínimos de atraso (por defecto usa settings.DAYS_THRESHOLD)
            max_days: Días máximos de atraso (por defecto usa settings.MAX_DAYS_THRESHOLD)
        
        Returns:
            Lista de diccionarios con información de contratos:
            [
                {
                    'contract_id': int,
                    'days_overdue': int,
                    'total_debt': Decimal,
                    'status': str
                },
                ...
            ]
        """
        if min_days is None:
            min_days = settings.DAYS_THRESHOLD
        if max_days is None:
            max_days = settings.MAX_DAYS_THRESHOLD
        
        logger.info(f"Consultando contratos entre {min_days} y {max_days} días de atraso...")
        
        query = f"""
        SELECT 
            ca.contract_id,
            DATEDIFF(CURDATE(), ca.expiration_date) AS days_overdue,
            ca.outstanding_principal AS total_debt,
            'MORA' AS status
        FROM 
            contract_amortization ca
        INNER JOIN
            contract c ON c.id = ca.contract_id
        WHERE 
            ca.expiration_date < CURDATE()
            AND ca.outstanding_principal > 0
            AND ca.contract_amortization_payment_status_id = 4
            AND c.contracts_status_id NOT IN (5, 7)
            AND DATEDIFF(CURDATE(), ca.expiration_date) BETWEEN {min_days} AND {max_days}
        GROUP BY 
            ca.contract_id
        ORDER BY 
            days_overdue DESC
        """
        
        try:
            result = self.mysql_session.execute(text(query))
            contracts = []
            
            for row in result:
                contracts.append({
                    'contract_id': row[0],
                    'days_overdue': row[1],
                    'total_debt': row[2],
                    'status': row[3]
                })
            
            logger.info(f"✓ Se encontraron {len(contracts)} contratos entre {min_days} y {max_days} días de atraso")
            return contracts
        
        except Exception as e:
            logger.error(f"✗ Error al consultar contratos: {e}")
            raise
    
    def get_contracts_in_range(self, min_days: int, max_days: int) -> List[int]:
        """
        Obtiene IDs de contratos con atraso en un rango específico.
        
        Args:
            min_days: Días mínimos de atraso (inclusivo)
            max_days: Días máximos de atraso (inclusivo)
        
        Returns:
            Lista de IDs de contratos
        """
        logger.info(f"Consultando contratos entre {min_days} y {max_days} días de atraso...")
        
        query = f"""
        SELECT 
            ca.contract_id
        FROM 
            contract_amortization ca
        INNER JOIN
            contract c ON c.id = ca.contract_id
        WHERE 
            ca.expiration_date < CURDATE()
            AND ca.outstanding_principal > 0
            AND ca.contract_amortization_payment_status_id = 4
            AND c.contracts_status_id NOT IN (5, 7)
            AND DATEDIFF(CURDATE(), ca.expiration_date) BETWEEN {min_days} AND {max_days}
        GROUP BY 
            ca.contract_id
        """
        
        try:
            result = self.mysql_session.execute(text(query))
            contract_ids = [row[0] for row in result]
            
            logger.info(f"✓ Se encontraron {len(contract_ids)} contratos entre {min_days} y {max_days} días")
            return contract_ids
        
        except Exception as e:
            logger.error(f"✗ Error al consultar contratos por rango: {e}")
            raise
