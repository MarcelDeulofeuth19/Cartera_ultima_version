"""
Pruebas unitarias de RuntimeConfigService._upsert_env_values (archivo temporal)
y de TwistAssignmentService.run_twist1/run_twist2 con dependencias falsas.
"""
from pathlib import Path

import pytest

from app.runtime_config.service import RuntimeConfigService
from app.services.twist_assignment_service import TwistAssignmentService

pytestmark = pytest.mark.unit


# --- _upsert_env_values (estático, con archivo temporal) --------------------
def test_upsert_env_crea_archivo(tmp_path):
    env = tmp_path / ".env"
    changed = RuntimeConfigService._upsert_env_values(
        env_path=env, values={"DEFAULT_SERLEFIN_PERCENT": "60.00"}
    )
    assert changed is True
    assert "DEFAULT_SERLEFIN_PERCENT=60.00" in env.read_text(encoding="utf-8")


def test_upsert_env_actualiza_existente(tmp_path):
    env = tmp_path / ".env"
    env.write_text("OTRA=1\nDEFAULT_SERLEFIN_PERCENT=55.00\n", encoding="utf-8")
    changed = RuntimeConfigService._upsert_env_values(
        env_path=env, values={"DEFAULT_SERLEFIN_PERCENT": "60.00"}
    )
    assert changed is True
    text = env.read_text(encoding="utf-8")
    assert "DEFAULT_SERLEFIN_PERCENT=60.00" in text
    assert "OTRA=1" in text                       # se conserva lo demás
    assert "55.00" not in text                    # valor viejo reemplazado


def test_upsert_env_sin_cambios(tmp_path):
    env = tmp_path / ".env"
    env.write_text("K=1\n", encoding="utf-8")
    changed = RuntimeConfigService._upsert_env_values(env_path=env, values={"K": "1"})
    assert changed is False


# --- twist run con dependencias falsas --------------------------------------
class _Q:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _Session:
    def __init__(self):
        self.bulk = []
        self.commits = 0

    def query(self, *a):
        return _Q([])  # no hay existentes

    def bulk_insert_mappings(self, model, rows):
        self.bulk.append((model, list(rows)))

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


class _FakeContractService:
    @staticmethod
    def normalize_customer_document(v):
        return "".join(ch for ch in str(v or "") if ch.isdigit())

    def get_twist1_contracts_with_arrears(self, min_days=None, max_days=None):
        return [
            {"contract_id": 10, "days_overdue": 40},  # franja
            {"contract_id": 11, "days_overdue": 70},  # 61-90
        ]

    def get_twist1_customer_documents_for_contracts(self, ids):
        return {10: "13", 11: "24"}  # 13 impar, 24 par


def test_run_twist1_sin_mysql():
    stats = TwistAssignmentService(_Session(), contract_service=None).run_twist1()
    assert stats["producto"] == "TWIST1"
    assert "error" in stats


def test_run_twist1_asigna_e_inserta():
    svc = TwistAssignmentService(_Session(), contract_service=_FakeContractService())
    stats = svc.run_twist1()
    assert stats["producto"] == "TWIST1"
    assert stats["candidates"] == 2
    assert stats["inserted"] >= 1


def test_run_twist2_deshabilitado(monkeypatch):
    monkeypatch.setattr("app.services.twist_assignment_service.settings.TWIST2_ENABLED", False)
    stats = TwistAssignmentService(_Session()).run_twist2()
    assert stats.get("enabled") is False
