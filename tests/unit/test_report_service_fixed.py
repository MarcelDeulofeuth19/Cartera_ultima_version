"""Cobertura del Excel de contratos fijos en ReportService (sesión falsa + tmp)."""
import types
from datetime import datetime, date
from pathlib import Path

import pytest

from app.core.config import settings

pytestmark = pytest.mark.unit


def _mgmt(effect, user_id=45, contract_id=1, promise=None, mdate=None):
    return types.SimpleNamespace(
        effect=effect, user_id=user_id, contract_id=contract_id,
        promise_date=promise, management_date=mdate,
    )


def test_collect_fixed_management_data():
    from app.services.report_service import ReportService
    today = date(2026, 7, 15)
    vdt = datetime(2026, 6, 15)
    hoy = datetime(2026, 7, 15, 23, 59)
    mgmts = [
        _mgmt(settings.EFFECT_ACUERDO_PAGO, 45, 1, promise=date(2026, 7, 20)),   # válido
        _mgmt(settings.EFFECT_ACUERDO_PAGO, 45, 2, promise=date(2026, 7, 1)),    # promesa pasada -> no
        _mgmt(settings.EFFECT_PAGO_TOTAL, 81, 3, mdate=datetime(2026, 7, 1)),    # en rango -> válido
    ]
    data = ReportService._collect_fixed_management_data(
        types.SimpleNamespace(), mgmts, today, vdt, hoy
    )
    ids = {d["Contract ID"] for d in data}
    assert ids == {1, 3}


def test_build_fixed_summary_data():
    from app.services.report_service import ReportService, ADVISOR_ID
    data = [
        {ADVISOR_ID: 45, "Effect": settings.EFFECT_ACUERDO_PAGO},
        {ADVISOR_ID: 81, "Effect": settings.EFFECT_PAGO_TOTAL},
    ]
    result = ReportService._build_fixed_summary_data(
        types.SimpleNamespace(), {45: [1], 81: [2, 3]}, data
    )
    summary = result[0]
    assert summary["Casa Cobranza"] == ["COBYSER", "SERLEFIN"]
    assert result[1] == 1   # cobyser_total
    assert result[4] == 2   # serlefin_total


class _Q:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *a, **k):
        return self

    def all(self):
        return list(self._rows)


class _Session:
    def __init__(self, rows):
        self._rows = rows

    def query(self, *a):
        return _Q(self._rows)


def test_generate_fixed_contracts_excel(tmp_path, monkeypatch):
    monkeypatch.setattr("app.services.report_service.settings.REPORTS_DIR", str(tmp_path))
    from app.services.report_service import ReportService
    svc = ReportService()
    mgmts = [_mgmt(settings.EFFECT_ACUERDO_PAGO, 45, 1, promise=date(2100, 1, 1))]
    path = svc.generate_fixed_contracts_excel({45: [1], 81: []}, _Session(mgmts))
    assert Path(path).exists() and path.endswith(".xlsx")


def test_generate_all_reports_solo_txt(tmp_path, monkeypatch):
    monkeypatch.setattr("app.services.report_service.settings.REPORTS_DIR", str(tmp_path))
    from app.services.report_service import ReportService
    svc = ReportService()
    results = {"final_assignments": {45: [1], 81: [2]}, "contracts_days_map": {1: 70, 2: 80}}
    files = svc.generate_all_reports(results, _Session([]))
    assert "user_45" in files and "user_81" in files
