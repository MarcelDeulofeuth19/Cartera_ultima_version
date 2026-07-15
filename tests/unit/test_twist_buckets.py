"""
Pruebas unitarias de los helpers de reglas Twist (asignacion a casas de cobranza).

`_group_main_by_bucket`, `_franja_decisions` y `_bucket_decisions` son estaticos
y puros: implementan las mismas reglas de la imagen para los productos Twist.
"""
import pytest

from app.services.twist_assignment_service import TwistAssignmentService

pytestmark = pytest.mark.unit


def test_group_main_by_bucket_solo_61_240():
    contracts = [
        {"line_id": "a", "days_overdue": 20},   # < 61 -> fuera
        {"line_id": "b", "days_overdue": 61},   # 61_90
        {"line_id": "c", "days_overdue": 90},   # 61_90
        {"line_id": "d", "days_overdue": 200},  # 181_209
        {"line_id": "e", "days_overdue": 999},  # > 240 -> fuera
    ]
    by_bucket = TwistAssignmentService._group_main_by_bucket(contracts)

    assert sorted(by_bucket.keys()) == ["181_209", "61_90"]
    assert {c["line_id"] for c in by_bucket["61_90"]} == {"b", "c"}
    assert {c["line_id"] for c in by_bucket["181_209"]} == {"d"}


def test_franja_decisions_31_60_por_paridad():
    contracts = [
        {"line_id": "impar", "days_overdue": 40, "cedula": "12345671"},
        {"line_id": "par", "days_overdue": 50, "cedula": "12345672"},
        {"line_id": "sincedula", "days_overdue": 55, "cedula": ""},       # sin cedula -> no asigna
        {"line_id": "fuera", "days_overdue": 70, "cedula": "12345671"},   # no franja
    ]
    decisions = TwistAssignmentService._franja_decisions(contracts)
    by_line = {c["line_id"]: (uid, tipo) for c, uid, tipo in decisions}

    assert by_line == {
        "impar": (45, "CEDULAS_IMPAR"),
        "par": (81, "CEDULAS_PAR"),
    }


def test_bucket_decisions_reparte_40_60():
    bucket = [{"line_id": f"c{i}", "days_overdue": 70} for i in range(10)]
    decisions = TwistAssignmentService._bucket_decisions(
        {"61_90": bucket}, serlefin_ratio=0.6
    )

    users = [uid for _, uid, _ in decisions]
    assert len(decisions) == 10
    assert users.count(81) == 6   # Serlefin 60%
    assert users.count(45) == 4   # Cobyser 40%
    assert all(tipo == "ASIGNACION" for _, _, tipo in decisions)


def test_bucket_decisions_vacio():
    assert TwistAssignmentService._bucket_decisions({}, serlefin_ratio=0.6) == []


def test_decide_assignments_combina_franja_y_buckets():
    contracts = [
        {"line_id": "f", "days_overdue": 35, "cedula": "7"},     # franja impar -> 45
        {"line_id": "m1", "days_overdue": 70, "cedula": "2"},    # 61-90 -> reparto
        {"line_id": "m2", "days_overdue": 80, "cedula": "4"},    # 61-90 -> reparto
    ]
    decisions = TwistAssignmentService._decide_assignments(contracts, serlefin_ratio=0.6)
    by_line = {c["line_id"]: (uid, tipo) for c, uid, tipo in decisions}

    assert by_line["f"] == (45, "CEDULAS_IMPAR")
    assert {by_line["m1"][0], by_line["m2"][0]} == {45, 81}
