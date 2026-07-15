"""
Pruebas unitarias de las reglas de exclusión de asignación
(`app/core/assignment_rules.py`) y del mapeo de contratos endosados.

Regla cubierta: NO asignar contratos ENDOSADOS a afianzadora (pagaré). Todo se
prueba sin BD ni red (la sesión MySQL se reemplaza por un doble de prueba).
"""
import pytest

from app.core import assignment_rules as rules
from app.services.contract_service import ContractService

pytestmark = pytest.mark.unit


# --- reglas puras -----------------------------------------------------------
def test_labels_endoso():
    assert rules.ENDOSO_STATUS_LABELS == {
        1: "Endosado Libraval",
        2: "Endosado Fianzavasa",
        3: "Endosado Figarantías",
    }


def test_is_endorsed(monkeypatch):
    monkeypatch.setattr(rules, "endorsed_status_ids", lambda: [1, 2, 3])
    assert rules.is_endorsed(1) is True
    assert rules.is_endorsed(2) is True
    assert rules.is_endorsed(3) is True
    assert rules.is_endorsed("2") is True   # coacciona texto numérico
    assert rules.is_endorsed(None) is False  # sin endoso -> asignable
    assert rules.is_endorsed(0) is False
    assert rules.is_endorsed(4) is False
    assert rules.is_endorsed("abc") is False  # dato sucio -> no endosado


def test_is_endorsed_regla_deshabilitada(monkeypatch):
    monkeypatch.setattr(rules, "endorsed_status_ids", lambda: [])
    assert rules.is_endorsed(1) is False


def test_exclusion_sql_habilitada(monkeypatch):
    monkeypatch.setattr(rules, "endorsed_status_ids", lambda: [1, 2, 3])
    sql = rules.endorsed_exclusion_sql("c.pagare_status_id")
    assert "c.pagare_status_id IS NULL" in sql
    assert "NOT IN (1,2,3)" in sql
    assert sql.startswith("  AND")
    assert sql.endswith("\n")


def test_exclusion_sql_deshabilitada(monkeypatch):
    monkeypatch.setattr(rules, "endorsed_status_ids", lambda: [])
    assert rules.endorsed_exclusion_sql("c.pagare_status_id") == ""


def test_exclusion_sql_regresion_texto_exacto(monkeypatch):
    # Regresión: idéntico al fragmento SQL inline anterior (sin cambio de comportamiento).
    monkeypatch.setattr(rules, "endorsed_status_ids", lambda: [1, 2, 3])
    assert (
        rules.endorsed_exclusion_sql("c.pagare_status_id")
        == "  AND (c.pagare_status_id IS NULL OR c.pagare_status_id NOT IN (1,2,3))\n"
    )
    assert (
        rules.endorsed_exclusion_sql("c.twist_pagare_status_id")
        == "  AND (c.twist_pagare_status_id IS NULL OR c.twist_pagare_status_id NOT IN (1,2,3))\n"
    )


# --- mapeo de contratos endosados (con sesión MySQL falsa) ------------------
class _FakeMySQL:
    """Sesión falsa: execute() devuelve filas fijas (contract_id, pagare_status_id, name)."""

    def __init__(self, rows):
        self._rows = rows
        self.calls = 0

    def execute(self, _stmt):
        self.calls += 1
        return list(self._rows)


def test_get_endorsed_contracts_map(monkeypatch):
    monkeypatch.setattr("app.services.contract_service.endorsed_status_ids", lambda: [1, 2, 3])
    rows = [
        (6647, 1, "Endosado Libraval"),
        (8, 3, None),          # sin nombre -> usa etiqueta por id
        (99, 2, "  "),         # nombre en blanco -> usa etiqueta por id
    ]
    svc = ContractService(mysql_session=_FakeMySQL(rows))
    mapping = svc.get_endorsed_contracts_map()
    assert mapping == {
        6647: "Endosado Libraval",
        8: "Endosado Figarantías",
        99: "Endosado Fianzavasa",
    }


def test_get_endorsed_contract_ids(monkeypatch):
    monkeypatch.setattr("app.services.contract_service.endorsed_status_ids", lambda: [1, 2, 3])
    svc = ContractService(mysql_session=_FakeMySQL([(6647, 1, "Endosado Libraval"), (8, 3, "Endosado Figarantías")]))
    assert svc.get_endorsed_contract_ids() == {6647, 8}


def test_mapeo_regla_deshabilitada_no_consulta(monkeypatch):
    monkeypatch.setattr("app.services.contract_service.endorsed_status_ids", lambda: [])
    fake = _FakeMySQL([(1, 1, "x")])
    svc = ContractService(mysql_session=fake)
    assert svc.get_endorsed_contracts_map() == {}
    assert svc.get_endorsed_contract_ids() == set()
    assert fake.calls == 0  # no debe tocar la BD si la regla está apagada


def test_mapeo_por_subconjunto_batching(monkeypatch):
    monkeypatch.setattr("app.services.contract_service.endorsed_status_ids", lambda: [1, 2, 3])
    fake = _FakeMySQL([(6647, 1, "Endosado Libraval")])
    svc = ContractService(mysql_session=fake)
    mapping = svc.get_endorsed_contracts_map(contract_ids=[6647, 12345])
    assert mapping == {6647: "Endosado Libraval"}
    assert fake.calls == 1  # un solo lote
