"""
Pruebas unitarias de ContractService con sesión MySQL FALSA (sin BD).

Cubre el parseo/armado de resultados de los métodos de consulta (días de atraso,
cuotas, documentos, resolución por documento y contratos con atraso).
"""
import pytest

from app.services.contract_service import ContractService

pytestmark = pytest.mark.unit


class _FakeMySQL:
    """execute() devuelve filas fijas (ignora el SQL)."""

    def __init__(self, rows):
        self._rows = rows
        self.calls = 0

    def execute(self, *a, **k):
        self.calls += 1
        return list(self._rows)


def test_normalize_customer_document():
    assert ContractService.normalize_customer_document("1.234-567") == "1234567"
    assert ContractService.normalize_customer_document(None) == ""
    assert ContractService.normalize_customer_document("ABC12") == "12"


def test_get_days_overdue_for_contracts():
    svc = ContractService(_FakeMySQL([(1, 70), (2, 80)]))
    result = svc.get_days_overdue_for_contracts([1, 2, 3])
    assert result == {1: 70, 2: 80, 3: 0}  # 3 no aparece -> default 0


def test_get_days_overdue_vacio():
    svc = ContractService(_FakeMySQL([]))
    assert svc.get_days_overdue_for_contracts([]) == {}


def test_get_overdue_installments_count():
    svc = ContractService(_FakeMySQL([(1, 3)]))
    result = svc.get_overdue_installments_count_for_contracts([1, 2])
    assert result == {1: 3, 2: 0}


def test_get_customer_documents_for_contracts():
    svc = ContractService(_FakeMySQL([(1, "123"), (2, "456")]))
    result = svc.get_customer_documents_for_contracts([1, 2])
    assert result == {1: "123", 2: "456"}


def test_get_contract_ids_by_customer_documents():
    svc = ContractService(_FakeMySQL([(10,), (20,)]))
    result = svc.get_contract_ids_by_customer_documents({"123", "456"})
    assert result == {10, 20}


def test_get_contract_ids_by_documents_vacio():
    svc = ContractService(_FakeMySQL([]))
    assert svc.get_contract_ids_by_customer_documents(set()) == set()


def test_get_contracts_with_arrears(monkeypatch):
    # Desactiva la lista negra por documento para aislar la consulta principal.
    monkeypatch.setattr("app.services.contract_service.settings.CLIENT_DOCUMENT_BLACKLIST", "")
    monkeypatch.setattr(
        "app.services.contract_service.settings.CLIENT_DOCUMENT_BLACKLIST_FILE", "no-existe.txt"
    )
    svc = ContractService(_FakeMySQL([(1, 70, 1000, "MORA"), (2, 90, 2000, "MORA")]))
    result = svc.get_contracts_with_arrears(min_days=61, max_days=240)
    assert len(result) == 2
    assert result[0]["contract_id"] == 1 and result[0]["days_overdue"] == 70
    assert result[1]["status"] == "MORA"
