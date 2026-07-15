"""
Pruebas unitarias adicionales de AssignmentService (sin BD): builders de correo,
política de notificación, limpieza de archivos y get_current_assignments con
sesión falsa.
"""
import types

import pytest

from app.services.assignment_service import AssignmentService

pytestmark = pytest.mark.unit


# --- builders de cuerpo de correo (estáticos, puros) ------------------------
def test_build_cobyser_body():
    html = AssignmentService._build_cobyser_report_body(
        "2026-07-15 10:00:00", 120, 40.0, "Se adjunta base.", "<p>m</p>"
    )
    assert "Cobyser" in html and "120" in html and "40.0%" in html
    assert "Se adjunta base." in html


def test_build_serlefin_body():
    html = AssignmentService._build_serlefin_report_body(
        "2026-07-15", 180, 60.0, "<p>m</p>"
    )
    assert "Serlefin" in html and "180" in html and "60.0%" in html


def test_build_both_body():
    html = AssignmentService._build_both_report_body(
        "2026-07-15", 180, 120, "Se adjuntan bases.", "<p>m</p>"
    )
    assert "180" in html and "120" in html and "Se adjuntan bases." in html


# --- limpieza de reportes (estático, con archivos temporales) ---------------
def test_cleanup_generated_report_files(tmp_path):
    f1 = tmp_path / "a.xlsx"
    f2 = tmp_path / "b.xlsx"
    f1.write_text("x")
    f2.write_text("y")
    missing = str(tmp_path / "no.xlsx")
    # incluye duplicado y vacío para cubrir la normalización
    AssignmentService._cleanup_generated_report_files(
        [str(f1), str(f1), "", str(f2), missing]
    )
    assert not f1.exists() and not f2.exists()


def test_cleanup_lista_vacia():
    AssignmentService._cleanup_generated_report_files([])  # no lanza


# --- política de notificación (usa settings + zona horaria) -----------------
def test_notifications_allowed_today_devuelve_tupla():
    allowed, day = AssignmentService._notifications_allowed_today(types.SimpleNamespace())
    assert isinstance(allowed, bool)
    assert isinstance(day, str) and len(day) == 10  # YYYY-MM-DD


# --- get_current_assignments con sesión falsa -------------------------------
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


def test_get_current_assignments_agrupa_por_usuario():
    rows = [(45, 1), (81, 2), (45, 3), (99, 4)]  # 99 no es casa -> se ignora
    fake_self = types.SimpleNamespace(postgres_session=_Session(rows))
    res = AssignmentService.get_current_assignments(fake_self, "PHONE")
    assert res[45] == {1, 3}
    assert res[81] == {2}
    assert 99 not in res  # solo USER_IDS


def test_get_current_assignments_twist_producto():
    rows = [(45, 10), (81, 20)]
    fake_self = types.SimpleNamespace(postgres_session=_Session(rows))
    res = AssignmentService.get_current_assignments(fake_self, "TWIST1")
    assert res[45] == {10} and res[81] == {20}


# --- _maybe_send_reports / _maybe_send_completion_notification --------------
def test_maybe_send_reports_omitido_por_calendario():
    results = {}
    AssignmentService._maybe_send_reports(types.SimpleNamespace(), results, False, "2026-07-15")
    assert results["report_sent"] is False
    assert results["report_skipped_by_schedule"] is True


def test_maybe_send_reports_enviado():
    results = {}
    fake = types.SimpleNamespace(generate_and_send_reports=lambda: True)
    AssignmentService._maybe_send_reports(fake, results, True, "2026-07-15")
    assert results["report_sent"] is True


def test_maybe_send_completion_omitido():
    results = {}
    AssignmentService._maybe_send_completion_notification(
        types.SimpleNamespace(), results, False, "2026-07-15"
    )
    assert results["completion_notification_sent"] is False
    assert results["completion_notification_skipped_by_schedule"] is True


def test_maybe_send_completion_enviado():
    results = {}
    fake = types.SimpleNamespace(send_completion_notification=lambda r: True)
    AssignmentService._maybe_send_completion_notification(fake, results, True, "2026-07-15")
    assert results["completion_notification_sent"] is True


# --- _run_twist_assignments (aislado, no rompe si falla) --------------------
def test_run_twist_assignments_sin_mysql(monkeypatch):
    monkeypatch.setattr(
        "app.services.twist_assignment_service.settings.TWIST2_ENABLED", False
    )
    results = {}
    fake = types.SimpleNamespace(postgres_session=None, contract_service=None)
    AssignmentService._run_twist_assignments(fake, results)
    assert results["twist1_stats"]["producto"] == "TWIST1"
    assert results["twist2_stats"]["enabled"] is False
