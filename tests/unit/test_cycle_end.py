"""Pruebas de funciones puras del informe de fin de ciclo (sin BD)."""
from datetime import date

import pytest

from app.services import cycle_end_report_service as ce

pytestmark = pytest.mark.unit


def test_month_bounds():
    first, last = ce.month_bounds(date(2026, 7, 15))
    assert first == date(2026, 7, 1)
    assert last == date(2026, 7, 31)
    # febrero no bisiesto
    f2, l2 = ce.month_bounds(date(2026, 2, 10))
    assert l2 == date(2026, 2, 28)


def test_fmt_money():
    assert ce._fmt_money(1234567) == "$1,234,567"
    assert ce._fmt_money(0) == "$0"
    assert ce._fmt_money(None) == "$0"


def test_prod_ind_cfg_tienen_llaves():
    for cfg in (ce._prod_cfg(), ce._ind_cfg()):
        assert set(cfg) >= {"host", "user", "password", "database", "port", "options"}
        assert cfg["options"].startswith("-csearch_path=")


def test_build_email_html():
    stats = [
        {"house": "Cobyser", "path": "/x/c.csv", "filas": 100, "con_ingreso": 10, "suma_ingreso": 500000.0},
        {"house": "Serlefin", "path": "/x/s.csv", "filas": 200, "con_ingreso": 20, "suma_ingreso": 1500000.0},
    ]
    html = ce.build_email_html(date(2026, 7, 31), date(2026, 7, 1), date(2026, 7, 31), stats)
    assert "Cobyser" in html and "Serlefin" in html
    assert "$2,000,000" in html          # total ingreso (500k + 1.5M)
    assert "300" in html or "300".replace("", "") in html  # total filas 100+200
    assert "julio" in html.lower() or "Julio" in html
