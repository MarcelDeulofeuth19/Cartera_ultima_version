"""
Prueba unitaria de AssignmentService.classify_agreement_contracts (sin BD).

Verifica la clasificación por casa (Regla 2) vs no-casa (Regla 1) a partir de la
gestión vigente más reciente, usando el agency_id devuelto por la consulta.
"""
import types

import pytest

from app.services.assignment_service import AssignmentService

pytestmark = pytest.mark.unit


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows
        self.executed = 0

    def execute(self, _stmt):
        self.executed += 1
        return _FakeResult(self._rows)


def _classify(rows, producto="PHONE"):
    fake_self = types.SimpleNamespace(
        postgres_session=_FakeSession(rows),
        _MANAGEMENT_TABLE_BY_PRODUCT=AssignmentService._MANAGEMENT_TABLE_BY_PRODUCT,
        _normalize_contract_key=AssignmentService._normalize_contract_key,
    )
    return AssignmentService.classify_agreement_contracts(fake_self, producto)


def test_clasifica_casa_y_no_casa():
    rows = [
        {"contract_id": 6647, "agency_id": 1},   # Cobyser -> casa 45
        {"contract_id": 20, "agency_id": 2},     # Serlefin -> casa 81
        {"contract_id": 10, "agency_id": None},  # empleado interno -> no-casa
        {"contract_id": 11, "agency_id": 9},     # agency desconocido -> no-casa
    ]
    res = _classify(rows)
    assert res["house"][45] == {6647}
    assert res["house"][81] == {20}
    assert res["non_house"] == {10, 11}


def test_producto_sin_tabla_devuelve_vacio():
    res = _classify([{"contract_id": 1, "agency_id": 1}], producto="INEXISTENTE")
    assert res["house"] == {45: set(), 81: set()}
    assert res["non_house"] == set()


def test_tablas_por_producto():
    m = AssignmentService._MANAGEMENT_TABLE_BY_PRODUCT
    assert m["PHONE"] == "managements"
    assert m["TWIST1"] == "managements_twist"
    assert m["TWIST2"] == "managements_twist2"


@pytest.mark.parametrize(
    "raw, expected",
    [
        (188417, 188417),
        ("3074", 3074),                       # texto numérico -> int
        ("  55 ", 55),
        ("eaf222d0-8c84-4cda-ac5c", "eaf222d0-8c84-4cda-ac5c"),  # uuid -> str
        (None, None),
        ("", None),                           # vacío -> None (se ignora)
        ("   ", None),
    ],
)
def test_normalize_contract_key(raw, expected):
    assert AssignmentService._normalize_contract_key(raw) == expected


def test_clasifica_twist2_uuid():
    rows = [
        {"contract_id": "eaf222d0-8c84-4cda-ac5c-ad9f4aeae760", "agency_id": 2},
        {"contract_id": "", "agency_id": 1},   # vacío -> se ignora
        {"contract_id": "abc-uuid", "agency_id": None},
    ]
    res = _classify(rows, producto="TWIST2")
    assert res["house"][81] == {"eaf222d0-8c84-4cda-ac5c-ad9f4aeae760"}
    assert res["house"][45] == set()
    assert res["non_house"] == {"abc-uuid"}
