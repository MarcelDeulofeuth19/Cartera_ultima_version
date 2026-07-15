"""Pruebas unitarias de ContractService: métodos Twist1, estado y rango (sesión falsa)."""
import pytest

from app.services.contract_service import ContractService

pytestmark = pytest.mark.unit


class _FakeMySQL:
    def __init__(self, rows):
        self._rows = rows
        self.calls = 0

    def execute(self, *a, **k):
        self.calls += 1
        return list(self._rows)


def _no_blacklist(monkeypatch):
    monkeypatch.setattr("app.services.contract_service.settings.CLIENT_DOCUMENT_BLACKLIST", "")
    monkeypatch.setattr(
        "app.services.contract_service.settings.CLIENT_DOCUMENT_BLACKLIST_FILE", "no-existe.txt"
    )


def test_twist1_contracts_with_arrears(monkeypatch):
    _no_blacklist(monkeypatch)
    svc = ContractService(_FakeMySQL([(10, 70, 500, "MORA"), (11, 40, 300, "MORA")]))
    res = svc.get_twist1_contracts_with_arrears(min_days=31, max_days=240)
    assert len(res) == 2 and res[0]["contract_id"] == 10


def test_twist1_customer_documents(monkeypatch):
    svc = ContractService(_FakeMySQL([(10, "111"), (11, "222")]))
    assert svc.get_twist1_customer_documents_for_contracts([10, 11]) == {10: "111", 11: "222"}


def test_twist1_contract_ids_by_documents():
    svc = ContractService(_FakeMySQL([(10,), (11,)]))
    assert svc.get_twist1_contract_ids_by_customer_documents({"111"}) == {10, 11}
    assert svc.get_twist1_contract_ids_by_customer_documents(set()) == set()


def test_twist1_franja_odd(monkeypatch):
    _no_blacklist(monkeypatch)
    svc = ContractService(_FakeMySQL([]))
    # stubs para aislar la clasificación por cédula impar
    svc.get_twist1_contracts_with_arrears = (
        lambda min_days=None, max_days=None, excluded_contract_ids=None: [
            {"contract_id": 10, "days_overdue": 40, "total_debt": 1, "status": "MORA"},
            {"contract_id": 11, "days_overdue": 50, "total_debt": 1, "status": "MORA"},
        ]
    )
    svc.get_twist1_customer_documents_for_contracts = lambda ids: {10: "77", 11: "88"}
    res = svc.get_twist1_franja_cobyser_odd_contracts(min_days=31, max_days=60)
    ids = sorted(c["contract_id"] for c in res)
    assert ids == [10]  # 77 impar -> incluido; 88 par -> excluido


def test_get_contracts_in_range():
    svc = ContractService(_FakeMySQL([(1,), (2,), (3,)]))
    assert svc.get_contracts_in_range(61, 240) == [1, 2, 3]


def test_get_current_state_for_contracts():
    svc = ContractService(_FakeMySQL([(1, "MORA"), (2, "AL_DIA")]))
    result = svc.get_current_state_for_contracts([1, 2, 3])
    assert result[1] == "MORA"
    assert result[2] == "AL_DIA"
    assert result[3] == "SIN_ESTADO"  # no aparece -> default
