"""
Asignacion de los productos Twist 1.0 y Twist 2.0.

Cada producto vive en su PROPIA tabla (su id no cabe en el contract_id entero
de contract_advisors):
- Twist 1.0 (MySQL alocreditprod, tablas twist_) -> contract_advisors_twist
  (tabla existente, minima: user_id, contract_id bigint).
- Twist 2.0 (PostgreSQL CBS+PDS) -> contract_advisors_twist2 (tabla nueva, rica).

Reglas (iguales a la imagen):
- Franja 31-60: SOLO Cobyser (45) y SOLO cedula impar (Serlefin 0%), tipo
  'CEDULAS_IMPAR'.
- 61-240: reparto 40/60 Cobyser/Serlefin por bucket DPD, tipo 'ASIGNACION'.
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.dpd import (
    ASSIGNMENT_DPD_ORDER,
    get_assignment_dpd_range,
    get_dpd_range,
    is_cedula_impar,
)
from app.database.models import ContractAdvisorHistory, ContractAdvisorTwist, Twist2Advisor
from app.services.assignment_service import AssignmentService

logger = logging.getLogger(__name__)

# Usuarios principales por casa para el reparto (desde configuración, sin hardcodear).
SERLEFIN_USER = settings.SERLEFIN_PRIMARY_USER_ID
COBYSER_USER = settings.FRANJA_COBYSER_USER_ID


class TwistAssignmentService:
    """Asigna Twist 1.0 y Twist 2.0 a sus tablas dedicadas."""

    def __init__(self, postgres_session: Session, contract_service=None):
        self.postgres_session = postgres_session
        self.contract_service = contract_service

    def _serlefin_ratio(self) -> float:
        try:
            return max(0.0, min(1.0, float(settings.DEFAULT_SERLEFIN_PERCENT) / 100.0))
        except Exception:
            return 0.6

    @staticmethod
    def _franja_decisions(contracts: List[dict]):
        """Franja 31-60 + cedula impar -> Cobyser (45), 'CEDULAS_IMPAR'."""
        decisions = []
        for contract in contracts:
            days = int(contract.get("days_overdue") or 0)
            if 31 <= days <= 60 and is_cedula_impar(contract.get("cedula")):
                decisions.append((contract, COBYSER_USER, "CEDULAS_IMPAR"))
        return decisions

    @staticmethod
    def _group_main_by_bucket(contracts: List[dict]) -> Dict[str, List[dict]]:
        """Agrupa los contratos 61-240 por bucket DPD de asignacion."""
        main = [
            contract
            for contract in contracts
            if 61 <= int(contract.get("days_overdue") or 0) <= 240
        ]
        by_bucket: Dict[str, List[dict]] = {}
        for contract in main:
            dpd_range = get_assignment_dpd_range(int(contract["days_overdue"]))
            if dpd_range:
                by_bucket.setdefault(dpd_range, []).append(contract)
        return by_bucket

    @staticmethod
    def _bucket_decisions(by_bucket: Dict[str, List[dict]], serlefin_ratio: float):
        """61-240 -> 40/60 por bucket, 'ASIGNACION'."""
        decisions = []
        for dpd_range in reversed(ASSIGNMENT_DPD_ORDER):
            bucket = by_bucket.get(dpd_range, [])
            if not bucket:
                continue
            quotas = AssignmentService._compute_house_quotas(len(bucket), serlefin_ratio)
            sequence = AssignmentService._build_alternating_user_sequence(
                total=len(bucket), quotas=quotas, first_user=SERLEFIN_USER,
            )
            for contract, user_id in zip(bucket, sequence):
                decisions.append((contract, user_id, "ASIGNACION"))
        return decisions

    @staticmethod
    def _decide_assignments(contracts: List[dict], serlefin_ratio: float):
        """
        Aplica las reglas de la imagen. Devuelve [(contract, user_id, tipo)].
        - 31-60 + cedula impar -> Cobyser (45), 'CEDULAS_IMPAR'.
        - 61-240 -> 40/60 por bucket, 'ASIGNACION'.
        - resto (1-30, 31-60 par) -> no se asigna.
        """
        decisions = []

        if settings.FRANJA_COBYSER_ENABLED:
            decisions.extend(TwistAssignmentService._franja_decisions(contracts))

        by_bucket = TwistAssignmentService._group_main_by_bucket(contracts)
        decisions.extend(
            TwistAssignmentService._bucket_decisions(by_bucket, serlefin_ratio)
        )

        return decisions

    # ------------------------------------------------------------------
    # Twist 1.0 -> contract_advisors_twist (MySQL como fuente)
    # ------------------------------------------------------------------
    def run_twist1(self) -> Dict:
        stats = {"producto": "TWIST1", "candidates": 0, "inserted": 0, "franja": 0}
        if self.contract_service is None:
            stats["error"] = "sin sesion MySQL para Twist1"
            return stats
        try:
            contracts = self.contract_service.get_twist1_contracts_with_arrears(
                min_days=settings.FRANJA_COBYSER_MIN_DAYS,
                max_days=settings.MAX_DAYS_THRESHOLD,
            )
            if not contracts:
                return stats

            ids = [int(c["contract_id"]) for c in contracts]
            doc_map = self.contract_service.get_twist1_customer_documents_for_contracts(ids)
            for contract in contracts:
                contract["cedula"] = self.contract_service.normalize_customer_document(
                    doc_map.get(int(contract["contract_id"]))
                )
            stats["candidates"] = len(contracts)

            decisions = self._decide_assignments(contracts, self._serlefin_ratio())

            existing = {
                int(row[0])
                for row in self.postgres_session.query(ContractAdvisorTwist.contract_id).all()
            }
            from datetime import datetime
            now = datetime.now()
            rows = []
            history_rows = []
            for contract, user_id, tipo in decisions:
                contract_id = int(contract["contract_id"])
                if contract_id in existing:
                    continue
                existing.add(contract_id)
                rows.append({"user_id": int(user_id), "contract_id": contract_id})
                # Guardar dia inicial en contract_advisors_history (producto TWIST1).
                d = int(contract.get("days_overdue") or 0)
                history_rows.append({
                    "user_id": int(user_id), "contract_id": contract_id,
                    "fecha_inicial": now, "fecha_terminal": None, "tipo": tipo,
                    "dias_atraso_inicial": d, "dpd_inicial": get_dpd_range(d),
                    "dpd_actual": get_dpd_range(d), "estado_actual": "PENDIENTE",
                    "producto": "TWIST1",
                })
                if tipo == "CEDULAS_IMPAR":
                    stats["franja"] += 1

            if rows:
                self.postgres_session.bulk_insert_mappings(ContractAdvisorTwist, rows)
                self.postgres_session.bulk_insert_mappings(ContractAdvisorHistory, history_rows)
                self.postgres_session.commit()
            stats["inserted"] = len(rows)
            logger.info(
                "Twist1: candidatos=%s, insertados=%s (franja impar=%s)",
                stats["candidates"], stats["inserted"], stats["franja"],
            )
            return stats
        except Exception as error:
            self.postgres_session.rollback()
            logger.error("Error en asignacion Twist1: %s", error)
            stats["error"] = str(error)
            return stats

    # ------------------------------------------------------------------
    # Twist 2.0 -> contract_advisors_twist2 (PostgreSQL CBS+PDS como fuente)
    # ------------------------------------------------------------------
    def _ensure_twist2_table(self) -> None:
        self.postgres_session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS alocreditindicators.contract_advisors_twist2 (
                    id SERIAL PRIMARY KEY,
                    line_id VARCHAR(64) NOT NULL,
                    cbs_id BIGINT,
                    user_id INTEGER NOT NULL,
                    cedula VARCHAR(50),
                    days_overdue INTEGER,
                    dpd VARCHAR(20),
                    tipo VARCHAR(50),
                    producto VARCHAR(20) NOT NULL DEFAULT 'TWIST2',
                    estado_actual VARCHAR(100),
                    fecha_inicial TIMESTAMP
                )
                """
            )
        )
        self.postgres_session.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_cat2_line_id
                ON alocreditindicators.contract_advisors_twist2 (line_id)
                """
            )
        )
        self.postgres_session.commit()

    @staticmethod
    def _digits(value) -> str:
        return "".join(ch for ch in str(value or "") if ch.isdigit())

    def _fetch_twist2_contracts(self) -> List[dict]:
        cbs_cfg = dict(
            host=settings.CBS_DB_HOST, port=settings.CBS_DB_PORT, user=settings.CBS_DB_USER,
            password=settings.CBS_DB_PASSWORD, dbname=settings.CBS_DB_NAME,
            connect_timeout=settings.CBS_DB_CONNECT_TIMEOUT,
        )
        pds_cfg = dict(
            host=settings.PDS_DB_HOST, port=settings.PDS_DB_PORT, user=settings.PDS_DB_USER,
            password=settings.PDS_DB_PASSWORD, dbname=settings.PDS_DB_NAME,
            connect_timeout=settings.PDS_DB_CONNECT_TIMEOUT,
        )

        cbs = psycopg2.connect(**cbs_cfg)
        cbs.autocommit = True
        try:
            cur = cbs.cursor()
            cur.execute(
                """
                SELECT id, ext_id, dpd_days
                FROM credit_line
                WHERE dpd_days BETWEEN %s AND %s
                  AND status::text NOT IN ('APPROVED', 'CLOSED')
                  AND ext_id IS NOT NULL
                """,
                (settings.FRANJA_COBYSER_MIN_DAYS, settings.MAX_DAYS_THRESHOLD),
            )
            cbs_rows = cur.fetchall()
            cur.close()
        finally:
            cbs.close()

        if not cbs_rows:
            return []

        ext_ids = [str(r[1]) for r in cbs_rows]
        cedula_map: Dict[str, str] = {}
        pds = psycopg2.connect(**pds_cfg)
        pds.autocommit = True
        try:
            cur = pds.cursor()
            batch = 1000
            for i in range(0, len(ext_ids), batch):
                chunk = ext_ids[i : i + batch]
                cur.execute(
                    """
                    SELECT cl.id::text, c.identification_number
                    FROM credit_lines cl
                    JOIN clients c ON c.id = cl.client_id
                    WHERE cl.id::text = ANY(%s)
                    """,
                    (chunk,),
                )
                for line_id, identification in cur.fetchall():
                    cedula_map[str(line_id)] = identification
            cur.close()
        finally:
            pds.close()

        contracts = []
        for cbs_id, ext_id, dpd_days in cbs_rows:
            line_id = str(ext_id)
            contracts.append(
                {
                    "line_id": line_id,
                    "cbs_id": int(cbs_id),
                    "days_overdue": int(dpd_days or 0),
                    "cedula": self._digits(cedula_map.get(line_id)),
                }
            )
        return contracts

    def run_twist2(self) -> Dict:
        stats = {"producto": "TWIST2", "candidates": 0, "inserted": 0, "franja": 0}
        if not settings.TWIST2_ENABLED:
            stats["enabled"] = False
            return stats
        if not (settings.CBS_DB_HOST and settings.PDS_DB_HOST):
            stats["error"] = "faltan credenciales CBS/PDS en el entorno (.env)"
            return stats
        try:
            self._ensure_twist2_table()
            contracts = self._fetch_twist2_contracts()
            stats["candidates"] = len(contracts)
            if not contracts:
                return stats

            decisions = self._decide_assignments(contracts, self._serlefin_ratio())

            existing = {
                str(row[0])
                for row in self.postgres_session.query(Twist2Advisor.line_id).all()
            }
            now = datetime.now()
            rows = []
            for contract, user_id, tipo in decisions:
                line_id = contract["line_id"]
                if line_id in existing:
                    continue
                existing.add(line_id)
                days = int(contract["days_overdue"])
                rows.append(
                    {
                        "line_id": line_id,
                        "cbs_id": contract.get("cbs_id"),
                        "user_id": int(user_id),
                        "cedula": contract.get("cedula") or None,
                        "days_overdue": days,
                        "dpd": get_assignment_dpd_range(days),
                        "tipo": tipo,
                        "producto": "TWIST2",
                        "estado_actual": "PENDIENTE",
                        "fecha_inicial": now,
                    }
                )
                if tipo == "CEDULAS_IMPAR":
                    stats["franja"] += 1

            if rows:
                self.postgres_session.bulk_insert_mappings(Twist2Advisor, rows)
                self.postgres_session.commit()
            stats["inserted"] = len(rows)
            logger.info(
                "Twist2: candidatos=%s, insertados=%s (franja impar=%s)",
                stats["candidates"], stats["inserted"], stats["franja"],
            )
            return stats
        except Exception as error:
            self.postgres_session.rollback()
            logger.error("Error en asignacion Twist2: %s", error)
            stats["error"] = str(error)
            return stats
