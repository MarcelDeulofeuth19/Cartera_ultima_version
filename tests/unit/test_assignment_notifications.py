"""
Cobertura de send_completion_notification y generate_and_send_reports de
AssignmentService (con email/report services simulados, sin red ni BD).
"""
import types

import pytest

from app.services.assignment_service import AssignmentService

pytestmark = pytest.mark.unit


def _results():
    return {
        "success": True,
        "insert_stats": {"inserted_total": 5, "inserted_cobyser": 2, "inserted_serlefin": 3},
        "balance_stats": {45: 2, 81: 3},
        "estado_actual_update_stats": {"rows_updated": 10, "history_rows_updated": 10,
                                       "contracts_considered": 100},
        "fixed_contracts_count": {"cobyser_45": 1, "serlefin_81": 2},
        "runtime_config": {"min_days": 61, "max_days": 240,
                           "serlefin_percent": 60.0, "cobyser_percent": 40.0},
        "blacklist_enforcement_stats": {"removed_from_contract_advisors": 0},
        "contracts_to_assign": [1, 2, 3],
        "promise_excluded_count": 3,
        "blacklist_contracts_count": 0,
        "report_sent": True,
        "error": None,
        "final_assignments": {45: [2, 4], 81: [1, 3, 5]},
        "started_at": "2026-07-15T05:00:00",
        "finished_at": "2026-07-15T05:01:00",
        "duration_seconds": 60.0,
    }


def test_send_completion_notification_ok(monkeypatch):
    from app.services.email_service import email_service
    monkeypatch.setattr(email_service, "send_assignment_report", lambda **k: True)
    ok = AssignmentService.send_completion_notification(types.SimpleNamespace(), _results())
    assert ok is True


def test_send_completion_notification_email_falla(monkeypatch):
    from app.services.email_service import email_service
    monkeypatch.setattr(email_service, "send_assignment_report", lambda **k: False)
    ok = AssignmentService.send_completion_notification(types.SimpleNamespace(), _results())
    assert ok is False


def test_send_completion_con_error(monkeypatch):
    from app.services.email_service import email_service
    monkeypatch.setattr(email_service, "send_assignment_report", lambda **k: True)
    r = _results()
    r["success"] = False
    r["error"] = "boom"
    r["report_error"] = "no report"
    assert AssignmentService.send_completion_notification(types.SimpleNamespace(), r) is True


def test_generate_and_send_reports_sin_metricas(monkeypatch):
    # calculate_distribution_metrics -> total 0 => retorna False temprano.
    from app.services import report_service_extended as rse_mod
    monkeypatch.setattr(
        rse_mod.report_service_extended, "calculate_distribution_metrics",
        lambda: {"total": 0},
    )
    ok = AssignmentService.generate_and_send_reports(types.SimpleNamespace())
    assert ok is False
