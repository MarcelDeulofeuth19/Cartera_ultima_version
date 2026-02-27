"""
Servicio de consulta de contratos desde MySQL.
Obtiene contratos con atraso desde alocreditprod.
"""
import logging
from typing import List, Dict

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings

logger = logging.getLogger(__name__)


class ContractService:
    """Servicio para consultar contratos con atraso desde MySQL."""

    def __init__(self, mysql_session: Session):
        self.mysql_session = mysql_session

    def get_contracts_with_arrears(
        self,
        min_days: int = None,
        max_days: int = None,
    ) -> List[Dict]:
        """
        Obtiene contratos con dias de atraso entre min_days y max_days.

        Returns:
            [
                {
                    'contract_id': int,
                    'days_overdue': int,
                    'total_debt': Decimal,
                    'status': str,
                },
                ...
            ]
        """
        if min_days is None:
            min_days = settings.DAYS_THRESHOLD
        if max_days is None:
            max_days = settings.MAX_DAYS_THRESHOLD

        logger.info(
            f"Consultando contratos entre {min_days} y {max_days} dias de atraso..."
        )

        query = f"""
        SELECT
            ca.contract_id,
            DATEDIFF(CURDATE(), MIN(ca.expiration_date)) AS days_overdue,
            SUM(ca.outstanding_principal) AS total_debt,
            'MORA' AS status
        FROM contract_amortization ca
        INNER JOIN contract c ON c.id = ca.contract_id
        WHERE ca.expiration_date < CURDATE()
          AND ca.outstanding_principal > 0
          AND ca.contract_amortization_payment_status_id = 4
          AND c.contracts_status_id NOT IN (5, 7)
        GROUP BY ca.contract_id
        HAVING DATEDIFF(CURDATE(), MIN(ca.expiration_date)) BETWEEN {min_days} AND {max_days}
        ORDER BY days_overdue DESC
        """

        try:
            result = self.mysql_session.execute(text(query))
            contracts = []

            for row in result:
                contracts.append(
                    {
                        "contract_id": row[0],
                        "days_overdue": row[1],
                        "total_debt": row[2],
                        "status": row[3],
                    }
                )

            logger.info(
                f"Se encontraron {len(contracts)} contratos entre {min_days} y {max_days} dias de atraso"
            )
            return contracts

        except Exception as e:
            logger.error(f"Error al consultar contratos: {e}")
            raise

    def get_contracts_in_range(self, min_days: int, max_days: int) -> List[int]:
        """
        Obtiene IDs de contratos con atraso en un rango especifico.
        """
        logger.info(
            f"Consultando contratos entre {min_days} y {max_days} dias de atraso..."
        )

        query = f"""
        SELECT
            ca.contract_id
        FROM contract_amortization ca
        INNER JOIN contract c ON c.id = ca.contract_id
        WHERE ca.expiration_date < CURDATE()
          AND ca.outstanding_principal > 0
          AND ca.contract_amortization_payment_status_id = 4
          AND c.contracts_status_id NOT IN (5, 7)
        GROUP BY ca.contract_id
        HAVING DATEDIFF(CURDATE(), MIN(ca.expiration_date)) BETWEEN {min_days} AND {max_days}
        """

        try:
            result = self.mysql_session.execute(text(query))
            contract_ids = [row[0] for row in result]

            logger.info(
                f"Se encontraron {len(contract_ids)} contratos entre {min_days} y {max_days} dias"
            )
            return contract_ids

        except Exception as e:
            logger.error(f"Error al consultar contratos por rango: {e}")
            raise

    def get_days_overdue_for_contracts(self, contract_ids: List[int]) -> Dict[int, int]:
        """
        Obtiene dias de atraso para un conjunto de contratos.

        Reglas:
        - Si existe cuota vencida o que vence hoy: retorna dias >= 0.
        - Si no aparece en la consulta, el contrato queda en 0.

        Args:
            contract_ids: Lista de contratos.

        Returns:
            Diccionario {contract_id: days_overdue}
        """
        if not contract_ids:
            return {}

        logger.info(
            f"Consultando dias de atraso para {len(contract_ids)} contratos..."
        )

        days_map: Dict[int, int] = {int(contract_id): 0 for contract_id in contract_ids}

        try:
            batch_size = 1000
            for i in range(0, len(contract_ids), batch_size):
                batch = contract_ids[i : i + batch_size]
                batch_ids = ",".join(str(int(contract_id)) for contract_id in batch)

                query = f"""
                SELECT
                    ca.contract_id,
                    DATEDIFF(CURDATE(), MIN(ca.expiration_date)) AS days_overdue
                FROM contract_amortization ca
                INNER JOIN contract c ON c.id = ca.contract_id
                WHERE ca.contract_id IN ({batch_ids})
                  AND ca.expiration_date <= CURDATE()
                  AND ca.outstanding_principal > 0
                  AND ca.contract_amortization_payment_status_id = 4
                  AND c.contracts_status_id NOT IN (5, 7)
                GROUP BY ca.contract_id
                """

                result = self.mysql_session.execute(text(query))
                for row in result:
                    contract_id = int(row[0])
                    days_overdue = int(row[1]) if row[1] is not None else 0
                    days_map[contract_id] = days_overdue

            logger.info(
                f"Dias de atraso obtenidos para {len(days_map)} contratos"
            )
            return days_map

        except Exception as e:
            logger.error(f"Error al consultar dias de atraso por contrato: {e}")
            raise
