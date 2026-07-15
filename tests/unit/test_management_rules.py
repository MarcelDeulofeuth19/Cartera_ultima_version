"""
Pruebas unitarias de app/core/management_rules.py (reglas por gestión/casa).

Todo es lógica pura (sin BD): normalización de efectos, vigencia de compromisos,
mapeo de agency_id a casa, y construcción de las condiciones SQL.
"""
from datetime import date, datetime, timedelta

import pytest

from app.core import management_rules as mr

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("acuerdo_pago", "acuerdo_pago"),
        ("Acuerdo de Pago", "acuerdo_de_pago"),
        ("  Acuerdo   de  pago  ", "acuerdo_de_pago"),
        ("Pago Total ", "pago_total"),
        ("pago_total", "pago_total"),
        (None, ""),
    ],
)
def test_normalize_effect(raw, expected):
    assert mr.normalize_effect(raw) == expected


def test_is_acuerdo_pago_variantes():
    for e in ["acuerdo_pago", "acuerdo_de_pago", "Acuerdo de Pago", "Acuerdo de pago "]:
        assert mr.is_acuerdo_pago(e) is True
    assert mr.is_acuerdo_pago("pago_total") is False
    assert mr.is_acuerdo_pago("ya_pago") is False


def test_is_pago_total_variantes():
    for e in ["pago_total", "Pago Total", "Pago total ", "PAGO   TOTAL"]:
        assert mr.is_pago_total(e) is True
    assert mr.is_pago_total("acuerdo_pago") is False


def test_is_agreement_effect():
    assert mr.is_agreement_effect("acuerdo_pago") is True
    assert mr.is_agreement_effect("Pago Total") is True
    assert mr.is_agreement_effect("ya_pago") is False
    assert mr.is_agreement_effect(None) is False


def test_agency_helpers():
    assert mr.is_house_agency(1) is True
    assert mr.is_house_agency(2) is True
    assert mr.is_house_agency(None) is False
    assert mr.is_house_agency(3) is False
    assert mr.house_user_for_agency(1) == 45
    assert mr.house_user_for_agency(2) == 81
    assert mr.house_user_for_agency(None) is None
    assert mr.house_user_for_agency(9) is None


def test_vigente_acuerdo_pago():
    today = date(2026, 7, 15)
    manana = today + timedelta(days=1)
    ayer = today - timedelta(days=1)
    # promise_date >= hoy -> vigente
    assert mr.is_vigente_agreement("acuerdo_pago", today, None, today) is True
    assert mr.is_vigente_agreement("Acuerdo de Pago", manana, None, today) is True
    # promise_date < hoy -> NO vigente
    assert mr.is_vigente_agreement("acuerdo_pago", ayer, None, today) is False
    # sin promise_date -> NO vigente
    assert mr.is_vigente_agreement("acuerdo_pago", None, None, today) is False


def test_vigente_pago_total_mes_calendario():
    today = date(2026, 7, 15)
    este_mes = datetime(2026, 7, 1, 10, 0)
    mes_pasado = datetime(2026, 6, 30, 23, 59)
    # pago_total del mes vigente -> vigente (aunque sea del dia 1)
    assert mr.is_vigente_agreement("pago_total", None, este_mes, today) is True
    assert mr.is_vigente_agreement("Pago Total", None, datetime(2026, 7, 31), today) is True
    # pago_total del mes pasado -> NO cuenta
    assert mr.is_vigente_agreement("pago_total", None, mes_pasado, today) is False
    # sin management_date -> NO vigente
    assert mr.is_vigente_agreement("pago_total", None, None, today) is False


def test_vigente_efecto_no_compromiso():
    today = date(2026, 7, 15)
    assert mr.is_vigente_agreement("ya_pago", today, datetime(2026, 7, 1), today) is False


def test_normalized_effect_sql():
    sql = mr.normalized_effect_sql("m.effect")
    assert "lower(trim(m.effect))" in sql
    assert "regexp_replace" in sql


def test_vigente_agreement_sql_estructura():
    sql = mr.vigente_agreement_sql("m.effect", "m.promise_date", "m.management_date")
    assert "m.promise_date >= CURRENT_DATE" in sql
    assert "date_trunc('month', m.management_date)" in sql
    assert "date_trunc('month', CURRENT_DATE)" in sql
    assert "acuerdo_pago" in sql and "acuerdo_de_pago" in sql
    assert "pago_total" in sql
