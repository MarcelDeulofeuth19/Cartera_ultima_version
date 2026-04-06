"""
Servicio de consulta de contratos desde MySQL.
Obtiene contratos con atraso desde alocreditprod.
"""
import logging
from typing import List, Dict, Set, Optional

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from app.core.config import settings

logger = logging.getLogger(__name__)


class ContractService:
    """Servicio para consultar contratos con atraso desde MySQL."""

    def __init__(self, mysql_session: Session):
        self.mysql_session = mysql_session

    @staticmethod
    def normalize_customer_document(raw_document: str) -> str:
        """Normaliza cedula/documento a solo digitos."""
        return "".join(ch for ch in str(raw_document or "") if ch.isdigit()).strip()

    def get_contract_ids_by_customer_documents(
        self,
        customer_documents: Set[str],
    ) -> Set[int]:
        """
        Obtiene IDs de contratos asociados a una lista de cedulas/documentos.
        """
        normalized_docs = {
            self.normalize_customer_document(document)
            for document in (customer_documents or set())
        }
        normalized_docs = {doc for doc in normalized_docs if doc}
        if not normalized_docs:
            return set()
        normalized_docs_no_zero = {
            (doc.lstrip("0") or "0")
            for doc in normalized_docs
        }

        logger.info(
            "Resolviendo contratos para %s documento(s) bloqueado(s)...",
            len(normalized_docs),
        )

        statement = text(
            """
            SELECT DISTINCT
                c.id AS contract_id
            FROM contract c
            INNER JOIN application a
                ON a.id = c.application_id
            INNER JOIN customer c2
                ON c2.id = a.customer_id
            WHERE REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    TRIM(COALESCE(c2.dni, '')),
                                    '.', ''
                                ),
                                '-', ''
                            ),
                            ' ', ''
                        ),
                        '/', ''
                    ),
                    '_', ''
                ),
                ',', ''
            ) IN :documents
            OR TRIM(LEADING '0' FROM REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    TRIM(COALESCE(c2.dni, '')),
                                    '.', ''
                                ),
                                '-', ''
                            ),
                            ' ', ''
                        ),
                        '/', ''
                    ),
                    '_', ''
                ),
                ',', ''
            )) IN :documents_no_zero
            """
        ).bindparams(
            bindparam("documents", expanding=True),
            bindparam("documents_no_zero", expanding=True),
        )

        try:
            rows = self.mysql_session.execute(
                statement,
                {
                    "documents": sorted(normalized_docs),
                    "documents_no_zero": sorted(normalized_docs_no_zero),
                },
            )
            contract_ids = {int(row[0]) for row in rows if row and row[0] is not None}
            logger.info(
                "Documentos bloqueados resueltos a %s contrato(s)",
                len(contract_ids),
            )
            return contract_ids
        except Exception as error:
            logger.error(
                "Error resolviendo contratos por documento bloqueado: %s",
                error,
            )
            raise

    def get_contracts_with_arrears(
        self,
        min_days: int = None,
        max_days: int = None,
        excluded_contract_ids: Optional[Set[int]] = None,
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

        effective_exclusions: Set[int] = set()
        if excluded_contract_ids:
            effective_exclusions.update(
                int(contract_id)
                for contract_id in excluded_contract_ids
                if int(contract_id) > 0
            )

        blocked_docs = {
            self.normalize_customer_document(doc)
            for doc in settings.blocked_customer_documents
        }
        blocked_docs = {doc for doc in blocked_docs if doc}
        if blocked_docs:
            blocked_contract_ids = self.get_contract_ids_by_customer_documents(blocked_docs)
            if blocked_contract_ids:
                logger.info(
                    "Excluyendo %s contrato(s) por lista negra de cedula/documento",
                    len(blocked_contract_ids),
                )
                effective_exclusions.update(blocked_contract_ids)

        exclusion_clause = ""
        filtered_ids = sorted(
            int(contract_id)
            for contract_id in effective_exclusions
            if int(contract_id) > 0
        )
        if filtered_ids:
            exclusion_clause = (
                "  AND ca.contract_id NOT IN ("
                + ",".join(str(contract_id) for contract_id in filtered_ids)
                + ")\n"
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
        {exclusion_clause}
        GROUP BY ca.contract_id
        HAVING DATEDIFF(CURDATE(), MIN(ca.expiration_date)) BETWEEN {min_days} AND {max_days}
        ORDER BY days_overdue ASC, ca.contract_id ASC
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

    def get_overdue_installments_count_for_contracts(self, contract_ids: List[int]) -> Dict[int, int]:
        """
        Obtiene cantidad de cuotas vencidas con la misma logica operativa.

        Reglas:
        - expiration_date <= CURDATE()
        - outstanding_principal > 0
        - contract_amortization_payment_status_id = 4
        - contrato activo (contracts_status_id NOT IN 5,7)
        """
        if not contract_ids:
            return {}

        logger.info(
            "Consultando cantidad de cuotas atrasadas para %s contratos...",
            len(contract_ids),
        )

        counts_map: Dict[int, int] = {int(contract_id): 0 for contract_id in contract_ids}

        try:
            batch_size = 1000
            for i in range(0, len(contract_ids), batch_size):
                batch = contract_ids[i : i + batch_size]
                batch_ids = ",".join(str(int(contract_id)) for contract_id in batch)

                query = f"""
                SELECT
                    ca.contract_id,
                    COUNT(*) AS overdue_installments
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
                    overdue_installments = int(row[1]) if row[1] is not None else 0
                    counts_map[contract_id] = overdue_installments

            logger.info(
                "Cantidad de cuotas atrasadas obtenida para %s contratos",
                len(counts_map),
            )
            return counts_map

        except Exception as e:
            logger.error(
                "Error al consultar cantidad de cuotas atrasadas por contrato: %s",
                e,
            )
            raise

    def get_current_state_for_contracts(self, contract_ids: List[int]) -> Dict[int, str]:
        """
        Obtiene el estado actual del contrato desde alocreditprod.contract.

        Se usa el nombre textual del estado (contracts_status.name) para
        persistirlo en contract_advisors.estado_actual.
        """
        if not contract_ids:
            return {}

        logger.info(
            "Consultando estado actual para %s contratos...",
            len(contract_ids),
        )

        state_map: Dict[int, str] = {
            int(contract_id): "SIN_ESTADO"
            for contract_id in contract_ids
        }

        try:
            try:
                # Estrategia preferida: tabla temporal + JOIN.
                self.mysql_session.execute(
                    text(
                        """
                        CREATE TEMPORARY TABLE IF NOT EXISTS tmp_contract_state_sync (
                            contract_id BIGINT PRIMARY KEY
                        ) ENGINE=MEMORY
                        """
                    )
                )
                self.mysql_session.execute(text("TRUNCATE TABLE tmp_contract_state_sync"))

                params = [{"contract_id": int(contract_id)} for contract_id in contract_ids]
                batch_size = 5000
                for i in range(0, len(params), batch_size):
                    self.mysql_session.execute(
                        text(
                            """
                            INSERT INTO tmp_contract_state_sync (contract_id)
                            VALUES (:contract_id)
                            """
                        ),
                        params[i : i + batch_size],
                    )

                result = self.mysql_session.execute(
                    text(
                        """
                        SELECT
                            c.id AS contract_id,
                            COALESCE(NULLIF(TRIM(cs.name), ''), 'SIN_ESTADO') AS estado_actual
                        FROM contract c
                        LEFT JOIN contracts_status cs
                            ON cs.id = c.contracts_status_id
                        INNER JOIN tmp_contract_state_sync t
                            ON t.contract_id = c.id
                        """
                    )
                )
                for row in result:
                    contract_id = int(row[0])
                    raw_state = row[1]
                    state_map[contract_id] = (
                        str(raw_state).strip()
                        if raw_state is not None and str(raw_state).strip()
                        else "SIN_ESTADO"
                    )
            except Exception as temp_error:
                # Fallback sin privilegios DDL: consulta IN por bloques grandes.
                logger.info(
                    "Sin privilegios para tabla temporal o fallo DDL (%s). "
                    "Usando fallback IN por bloques.",
                    temp_error,
                )
                batch_size = 50000
                for i in range(0, len(contract_ids), batch_size):
                    batch = contract_ids[i : i + batch_size]
                    if not batch:
                        continue
                    batch_ids = ",".join(str(int(contract_id)) for contract_id in batch)

                    query = f"""
                    SELECT
                        c.id AS contract_id,
                        COALESCE(NULLIF(TRIM(cs.name), ''), 'SIN_ESTADO') AS estado_actual
                    FROM contract c
                    LEFT JOIN contracts_status cs
                        ON cs.id = c.contracts_status_id
                    WHERE c.id IN ({batch_ids})
                    """

                    result = self.mysql_session.execute(text(query))
                    for row in result:
                        contract_id = int(row[0])
                        raw_state = row[1]
                        state_map[contract_id] = (
                            str(raw_state).strip()
                            if raw_state is not None and str(raw_state).strip()
                            else "SIN_ESTADO"
                        )

            logger.info(
                "Estado actual obtenido para %s contratos",
                len(state_map),
            )
            return state_map

        except Exception as e:
            logger.error(
                "Error al consultar estado actual por contrato: %s",
                e,
            )
            raise
