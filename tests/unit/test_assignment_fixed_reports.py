"""
Cobertura de ensure_fixed_contracts_assigned, _persist_fixed_assignments,
_load_dpd_map_for_contracts y generate_and_send_reports (full) con fakes.
"""
import types

import pytest

from app.services.assignment_service import AssignmentService

pytestmark = pytest.mark.unit


class _Q:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *a, **k):
        return self

    def all(self):
        return list(self._rows)


class _Pg:
    def __init__(self, rows):
        self._rows = rows
        self.bulk = []
        self.commits = 0

    def query(self, *a):
        return _Q(self._rows)

    def bulk_insert_mappings(self, m, r):
        self.bulk.append((m, list(r)))

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


def test_load_dpd_map_for_contracts():
    fake = types.SimpleNamespace(
        _require_contract_service=lambda: types.SimpleNamespace(
            get_days_overdue_for_contracts=lambda ids: {1: 70, 2: 95}
        )
    )
    stats = {}
    dpd = AssignmentService._load_dpd_map_for_contracts(fake, [1, 2], stats)
    assert dpd[1] == "61_90" and dpd[2] == "91_120"


def test_persist_fixed_assignments():
    pg = _Pg([])
    fake = types.SimpleNamespace(
        postgres_session=pg,
        _ensure_estado_actual_column=lambda: True,
        _build_history_metadata_from_days=AssignmentService._build_history_metadata_from_days,
        history_service=types.SimpleNamespace(
            register_assignments=lambda na, assignment_metadata=None, default_tipo=None: {
                "total_registered": 1
            }
        ),
    )
    rows = [{"user_id": 45, "contract_id": 1, "estado_actual": "X"}]
    hist = AssignmentService._persist_fixed_assignments(
        fake, rows, {45: [1]}, {1: 70}, {1: "X"}
    )
    assert hist["total_registered"] == 1
    assert pg.commits == 1


def test_ensure_fixed_contracts_assigned():
    # Un contrato fijo (45->contrato 7) que no está asignado -> se inserta.
    pg = _Pg([])  # nada asignado actualmente
    fake = types.SimpleNamespace(
        postgres_session=pg,
        _classify_fixed_contracts=AssignmentService._classify_fixed_contracts,
        _require_contract_service=lambda: types.SimpleNamespace(
            get_days_overdue_for_contracts=lambda ids: {7: 100},
            get_current_state_for_contracts=lambda ids: {7: "MORA"},
        ),
        _persist_fixed_assignments=lambda *a, **k: {"total_registered": 1},
    )
    stats = AssignmentService.ensure_fixed_contracts_assigned(
        fake, {45: {7}, 81: set()}, max_days_threshold=240
    )
    assert stats["inserted_total"] == 1
    assert stats["inserted_cobyser"] == 1


def test_generate_and_send_reports_full(monkeypatch):
    from app.services import report_service_extended as rse_mod
    from app.services.email_service import email_service

    rse = rse_mod.report_service_extended
    monkeypatch.setattr(rse, "calculate_distribution_metrics",
                        lambda: {"total": 100, "serlefin_percent": 60.0, "cobyser_percent": 40.0})
    monkeypatch.setattr(rse, "get_assigned_contracts_for_house", lambda users: [1, 2, 3])
    monkeypatch.setattr(rse, "generate_report_for_user",
                        lambda user_id, user_name, contracts: (f"/tmp/{user_name}.xlsx", None))
    monkeypatch.setattr(rse, "generate_metrics_html", lambda metrics, audience="general": "<p>m</p>")
    monkeypatch.setattr(email_service, "send_assignment_report", lambda **k: True)

    fake = types.SimpleNamespace(
        _build_cobyser_report_body=AssignmentService._build_cobyser_report_body,
        _build_serlefin_report_body=AssignmentService._build_serlefin_report_body,
        _build_both_report_body=AssignmentService._build_both_report_body,
        _cleanup_generated_report_files=AssignmentService._cleanup_generated_report_files,
    )
    ok = AssignmentService.generate_and_send_reports(fake)
    assert ok is True
