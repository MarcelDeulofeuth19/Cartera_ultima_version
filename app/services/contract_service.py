"""
Servicio de consulta de contratos desde MySQL.
Obtiene contratos con atraso desde alocreditprod.
"""
import logging
from typing import List, Dict, Set, Optional

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.dpd import is_cedula_impar

logger = logging.getLogger(__name__)

# Constantes de negocio derivadas de configuración (evita números mágicos en SQL).
_EXCLUDED_STATUS_CSV = ", ".join(str(i) for i in settings.excluded_contract_status_id_list)
_PHONE_ARREARS_STATUS = settings.PHONE_ARREARS_PAYMENT_STATUS_ID
_TWIST_ARREARS_STATUS = settings.TWIST_ARREARS_PAYMENT_STATUS_ID


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

        # Regla: excluir contratos endosados a afianzadora (pagaré).
        pagare_clause = ""
        pagare_ids = settings.pagare_excluded_status_id_list
        if pagare_ids:
            pagare_clause = (
                "  AND (c.pagare_status_id IS NULL OR c.pagare_status_id NOT IN ("
                + ",".join(str(p) for p in pagare_ids)
                + "))\n"
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
          AND ca.contract_amortization_payment_status_id = {_PHONE_ARREARS_STATUS}
          AND c.contracts_status_id NOT IN ({_EXCLUDED_STATUS_CSV})
        {exclusion_clause}{pagare_clause}
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

    def get_customer_documents_for_contracts(
        self,
        contract_ids: List[int],
    ) -> Dict[int, str]:
        """
        Obtiene la cedula/documento (customer.dni) por contrato.

        Camino: contract -> application -> customer (mismo JOIN validado en
        get_contract_ids_by_customer_documents). Si un contrato mapea a mas de
        un customer se toma MAX(c2.dni) para no duplicar filas.

        Returns:
            Diccionario {contract_id: dni_crudo}
        """
        if not contract_ids:
            return {}

        document_map: Dict[int, str] = {}

        try:
            batch_size = 1000
            for i in range(0, len(contract_ids), batch_size):
                batch = contract_ids[i : i + batch_size]
                batch_ids = ",".join(str(int(contract_id)) for contract_id in batch)

                query = f"""
                SELECT
                    c.id AS contract_id,
                    MAX(c2.dni) AS dni
                FROM contract c
                INNER JOIN application a ON a.id = c.application_id
                INNER JOIN customer c2 ON c2.id = a.customer_id
                WHERE c.id IN ({batch_ids})
                GROUP BY c.id
                """

                result = self.mysql_session.execute(text(query))
                for row in result:
                    document_map[int(row[0])] = row[1]

            return document_map

        except Exception as e:
            logger.error(f"Error al consultar cedulas por contrato: {e}")
            raise

    def get_franja_cobyser_odd_contracts(
        self,
        min_days: int = None,
        max_days: int = None,
        excluded_contract_ids: Optional[Set[int]] = None,
    ) -> List[Dict]:
        """
        Obtiene contratos de la franja Cobyser (por defecto 31-60 dias de atraso)
        cuya cedula termina en digito impar (1, 3, 5, 7, 9).

        Reutiliza get_contracts_with_arrears para el filtro de dias, exclusiones y
        lista negra, y agrega la cedula del cliente para evaluar la paridad.

        Returns:
            Lista de dicts con las mismas claves que get_contracts_with_arrears
            mas 'cedula' (normalizada a solo digitos).
        """
        if min_days is None:
            min_days = settings.FRANJA_COBYSER_MIN_DAYS
        if max_days is None:
            max_days = settings.FRANJA_COBYSER_MAX_DAYS

        franja_contracts = self.get_contracts_with_arrears(
            min_days=min_days,
            max_days=max_days,
            excluded_contract_ids=excluded_contract_ids,
        )
        if not franja_contracts:
            return []

        contract_ids = [int(contract["contract_id"]) for contract in franja_contracts]
        document_map = self.get_customer_documents_for_contracts(contract_ids)

        odd_contracts: List[Dict] = []
        for contract in franja_contracts:
            contract_id = int(contract["contract_id"])
            raw_document = document_map.get(contract_id)
            if not is_cedula_impar(raw_document):
                continue
            enriched = dict(contract)
            enriched["cedula"] = self.normalize_customer_document(raw_document)
            odd_contracts.append(enriched)

        logger.info(
            "Franja Cobyser %s-%s dias: %s contratos en rango, %s con cedula impar",
            min_days,
            max_days,
            len(franja_contracts),
            len(odd_contracts),
        )
        return odd_contracts

    # ------------------------------------------------------------------
    # Twist 1.0 (MySQL alocreditprod, tablas con prefijo twist_).
    # Misma estructura que Phone pero: amortizacion -> twist_contract_amortization
    # (status mora = 3 'Atrasado'), contrato -> twist_contract
    # (excluir status 5 'Anulado' / 7 'Fraude'), y cedula via
    # twist_contract -> twist_application -> customer.dni (customer es compartida).
    # ------------------------------------------------------------------
    def get_twist1_contract_ids_by_customer_documents(
        self,
        customer_documents: Set[str],
    ) -> Set[int]:
        """Resuelve contratos Twist1 asociados a cedulas/documentos (lista negra)."""
        normalized_docs = {
            self.normalize_customer_document(document)
            for document in (customer_documents or set())
        }
        normalized_docs = {doc for doc in normalized_docs if doc}
        if not normalized_docs:
            return set()
        normalized_docs_no_zero = {(doc.lstrip("0") or "0") for doc in normalized_docs}

        statement = text(
            """
            SELECT DISTINCT c.id AS contract_id
            FROM twist_contract c
            INNER JOIN twist_application a ON a.id = c.twist_application_id
            INNER JOIN customer c2 ON c2.id = a.customer_id
            WHERE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                TRIM(COALESCE(c2.dni, '')), '.', ''), '-', ''), ' ', ''), '/', ''), '_', ''), ',', ''
            ) IN :documents
            OR TRIM(LEADING '0' FROM REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                TRIM(COALESCE(c2.dni, '')), '.', ''), '-', ''), ' ', ''), '/', ''), '_', ''), ',', ''
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
            return {int(row[0]) for row in rows if row and row[0] is not None}
        except Exception as error:
            logger.error("Error resolviendo contratos Twist1 por documento: %s", error)
            raise

    def get_twist1_contracts_with_arrears(
        self,
        min_days: int = None,
        max_days: int = None,
        excluded_contract_ids: Optional[Set[int]] = None,
    ) -> List[Dict]:
        """Contratos Twist1 con atraso entre min_days y max_days (status mora=3)."""
        if min_days is None:
            min_days = settings.DAYS_THRESHOLD
        if max_days is None:
            max_days = settings.MAX_DAYS_THRESHOLD

        logger.info(
            "Consultando contratos Twist1 entre %s y %s dias de atraso...",
            min_days,
            max_days,
        )

        effective_exclusions: Set[int] = set()
        if excluded_contract_ids:
            effective_exclusions.update(
                int(cid) for cid in excluded_contract_ids if int(cid) > 0
            )

        blocked_docs = {
            self.normalize_customer_document(doc)
            for doc in settings.blocked_customer_documents
        }
        blocked_docs = {doc for doc in blocked_docs if doc}
        if blocked_docs:
            blocked_ids = self.get_twist1_contract_ids_by_customer_documents(blocked_docs)
            if blocked_ids:
                logger.info(
                    "Twist1: excluyendo %s contrato(s) por lista negra de cedula",
                    len(blocked_ids),
                )
                effective_exclusions.update(blocked_ids)

        exclusion_clause = ""
        filtered_ids = sorted(cid for cid in effective_exclusions if cid > 0)
        if filtered_ids:
            exclusion_clause = (
                "  AND ca.twist_contract_id NOT IN ("
                + ",".join(str(cid) for cid in filtered_ids)
                + ")\n"
            )

        # Regla: excluir contratos Twist1 endosados a afianzadora (pagaré).
        pagare_clause = ""
        pagare_ids = settings.pagare_excluded_status_id_list
        if pagare_ids:
            pagare_clause = (
                "  AND (c.twist_pagare_status_id IS NULL OR c.twist_pagare_status_id NOT IN ("
                + ",".join(str(p) for p in pagare_ids)
                + "))\n"
            )

        query = f"""
        SELECT
            ca.twist_contract_id AS contract_id,
            DATEDIFF(CURDATE(), MIN(ca.expiration_date)) AS days_overdue,
            SUM(ca.outstanding_principal) AS total_debt,
            'MORA' AS status
        FROM twist_contract_amortization ca
        INNER JOIN twist_contract c ON c.id = ca.twist_contract_id
        WHERE ca.expiration_date < CURDATE()
          AND ca.outstanding_principal > 0
          AND ca.twist_contract_payment_status_id = {_TWIST_ARREARS_STATUS}
          AND c.twist_contract_status_id NOT IN ({_EXCLUDED_STATUS_CSV})
        {exclusion_clause}{pagare_clause}
        GROUP BY ca.twist_contract_id
        HAVING DATEDIFF(CURDATE(), MIN(ca.expiration_date)) BETWEEN {min_days} AND {max_days}
        ORDER BY days_overdue ASC, ca.twist_contract_id ASC
        """

        try:
            result = self.mysql_session.execute(text(query))
            contracts = [
                {
                    "contract_id": row[0],
                    "days_overdue": row[1],
                    "total_debt": row[2],
                    "status": row[3],
                }
                for row in result
            ]
            logger.info("Twist1: %s contratos entre %s y %s dias", len(contracts), min_days, max_days)
            return contracts
        except Exception as e:
            logger.error("Error al consultar contratos Twist1: %s", e)
            raise

    def get_twist1_customer_documents_for_contracts(
        self,
        contract_ids: List[int],
    ) -> Dict[int, str]:
        """Cedula (customer.dni) por contrato Twist1."""
        if not contract_ids:
            return {}
        document_map: Dict[int, str] = {}
        try:
            batch_size = 1000
            for i in range(0, len(contract_ids), batch_size):
                batch = contract_ids[i : i + batch_size]
                batch_ids = ",".join(str(int(cid)) for cid in batch)
                query = f"""
                SELECT c.id AS contract_id, MAX(c2.dni) AS dni
                FROM twist_contract c
                INNER JOIN twist_application a ON a.id = c.twist_application_id
                INNER JOIN customer c2 ON c2.id = a.customer_id
                WHERE c.id IN ({batch_ids})
                GROUP BY c.id
                """
                result = self.mysql_session.execute(text(query))
                for row in result:
                    document_map[int(row[0])] = row[1]
            return document_map
        except Exception as e:
            logger.error("Error al consultar cedulas Twist1: %s", e)
            raise

    def get_twist1_franja_cobyser_odd_contracts(
        self,
        min_days: int = None,
        max_days: int = None,
        excluded_contract_ids: Optional[Set[int]] = None,
    ) -> List[Dict]:
        """Contratos Twist1 de la franja Cobyser (31-60) con cedula impar."""
        if min_days is None:
            min_days = settings.FRANJA_COBYSER_MIN_DAYS
        if max_days is None:
            max_days = settings.FRANJA_COBYSER_MAX_DAYS

        franja_contracts = self.get_twist1_contracts_with_arrears(
            min_days=min_days,
            max_days=max_days,
            excluded_contract_ids=excluded_contract_ids,
        )
        if not franja_contracts:
            return []

        contract_ids = [int(c["contract_id"]) for c in franja_contracts]
        document_map = self.get_twist1_customer_documents_for_contracts(contract_ids)

        odd_contracts: List[Dict] = []
        for contract in franja_contracts:
            contract_id = int(contract["contract_id"])
            raw_document = document_map.get(contract_id)
            if not is_cedula_impar(raw_document):
                continue
            enriched = dict(contract)
            enriched["cedula"] = self.normalize_customer_document(raw_document)
            odd_contracts.append(enriched)

        logger.info(
            "Twist1 franja Cobyser %s-%s: %s en rango, %s con cedula impar",
            min_days,
            max_days,
            len(franja_contracts),
            len(odd_contracts),
        )
        return odd_contracts

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
          AND ca.contract_amortization_payment_status_id = {_PHONE_ARREARS_STATUS}
          AND c.contracts_status_id NOT IN ({_EXCLUDED_STATUS_CSV})
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
                  AND ca.contract_amortization_payment_status_id = {_PHONE_ARREARS_STATUS}
                  AND c.contracts_status_id NOT IN ({_EXCLUDED_STATUS_CSV})
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
                  AND ca.contract_amortization_payment_status_id = {_PHONE_ARREARS_STATUS}
                  AND c.contracts_status_id NOT IN ({_EXCLUDED_STATUS_CSV})
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
                self._fill_state_map_with_temp_table(contract_ids, state_map)
            except Exception as temp_error:
                # Fallback sin privilegios DDL: consulta IN por bloques grandes.
                logger.info(
                    "Sin privilegios para tabla temporal o fallo DDL (%s). "
                    "Usando fallback IN por bloques.",
                    temp_error,
                )
                self._fill_state_map_with_in_clause(contract_ids, state_map)

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

    def _fill_state_map_with_temp_table(
        self,
        contract_ids: List[int],
        state_map: Dict[int, str],
    ) -> None:
        """Llena state_map usando la estrategia de tabla temporal + JOIN."""
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
        self._apply_state_rows(result, state_map)

    def _fill_state_map_with_in_clause(
        self,
        contract_ids: List[int],
        state_map: Dict[int, str],
    ) -> None:
        """Llena state_map usando la estrategia de IN por bloques grandes."""
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
            self._apply_state_rows(result, state_map)

    @staticmethod
    def _apply_state_rows(result, state_map: Dict[int, str]) -> None:
        """Vuelca filas (contract_id, estado_actual) en state_map."""
        for row in result:
            contract_id = int(row[0])
            raw_state = row[1]
            state_map[contract_id] = (
                str(raw_state).strip()
                if raw_state is not None and str(raw_state).strip()
                else "SIN_ESTADO"
            )
