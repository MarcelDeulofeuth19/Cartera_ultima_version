"""
Pruebas de los cálculos de programación de AutoAssignmentScheduler (puros) y
la limpieza de reportes. No ejerce los loops async ni la BD.
"""
import os
import time
import types
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from app.services.scheduler_service import AutoAssignmentScheduler
from app.core.config import settings

pytestmark = pytest.mark.unit

TZ = ZoneInfo(settings.AUTO_ASSIGNMENT_TIMEZONE)


def _sched_self():
    return types.SimpleNamespace(_timezone=TZ)


def test_next_business_run_es_futuro_y_dia_permitido():
    now = datetime(2026, 7, 15, 23, 30, tzinfo=TZ)
    nxt = AutoAssignmentScheduler._next_business_run(_sched_self(), now)
    assert nxt > now
    assert nxt.weekday() in settings.auto_assignment_weekday_list


def test_next_month_end_run_ultimo_dia():
    now = datetime(2026, 7, 15, 10, 0, tzinfo=TZ)
    nxt = AutoAssignmentScheduler._next_month_end_run(_sched_self(), now, 23, 0)
    # julio tiene 31 días
    assert nxt.day == 31 and nxt.month == 7
    assert nxt.hour == 23 and nxt > now


def test_next_month_end_run_salta_al_mes_siguiente():
    now = datetime(2026, 7, 31, 23, 30, tzinfo=TZ)  # ya pasó el último día a esa hora
    nxt = AutoAssignmentScheduler._next_month_end_run(_sched_self(), now, 23, 0)
    assert nxt.month == 8 and nxt.day == 31  # agosto, último día


def test_sentinel_paths():
    now = datetime(2026, 7, 15, tzinfo=TZ)
    p1 = AutoAssignmentScheduler._monthly_sentinel_path(now)
    p2 = AutoAssignmentScheduler._monthly_close_sentinel_path(now)
    assert p1.name == "cycle_end_report_2026-07.done"
    assert p2.name == "month_end_close_2026-07.done"


def test_cleanup_old_reports(tmp_path, monkeypatch):
    monkeypatch.setattr("app.services.scheduler_service.settings.REPORTS_DIR", str(tmp_path))
    viejo = tmp_path / "viejo.xlsx"
    nuevo = tmp_path / "nuevo.xlsx"
    viejo.write_text("x")
    nuevo.write_text("y")
    # envejecer 'viejo' 48h
    old = time.time() - 48 * 3600
    os.utime(viejo, (old, old))
    AutoAssignmentScheduler._cleanup_old_reports()
    assert not viejo.exists()   # eliminado por antigüedad
    assert nuevo.exists()       # reciente, se conserva


def test_cleanup_sin_directorio(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "app.services.scheduler_service.settings.REPORTS_DIR", str(tmp_path / "no-existe")
    )
    AutoAssignmentScheduler._cleanup_old_reports()  # no lanza
