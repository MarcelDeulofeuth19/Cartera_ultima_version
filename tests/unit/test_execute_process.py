"""
Cobertura del orquestador AssignmentService.execute_assignment_process con un
`self` totalmente simulado (todas las subrutinas stubbeadas). No toca BD: valida
el flujo/orquestación y el ensamblado de `results`.
"""
import types
from datetime import datetime

import pytest

from app.services.assignment_service import AssignmentService
from app.runtime_config.service import AssignmentRuntimeConfig

pytestmark = pytest.mark.unit


def _runtime_cfg():
    return AssignmentRuntimeConfig(
        serlefin_percent=60.0, cobyser_percent=40.0,
        min_days=61, max_days=240,
        updated_by="test", updated_at=datetime(2026, 7, 15),
    )


def _fake_service(new_assignments=None):
    na = new_assignments or {45: [], 81: []}
    contract_service = types.SimpleNamespace(
        get_contracts_with_arrears=lambda **k: [],
    )
    fake = types.SimpleNamespace(
        _notifications_allowed_today=lambda: (False, "2026-07-15"),
        _load_runtime_assignment_config=lambda: _runtime_cfg(),
        _ensure_producto_column=lambda: True,
        _load_contract_blacklist=lambda: set(),
        _load_customer_document_blacklist=lambda: set(),
        _resolve_blocked_contract_ids_by_documents=lambda docs: set(),
        enforce_blacklist_on_active_assignments=lambda ids: {"removed_from_contract_advisors": 0},
        get_fixed_contracts=lambda: {45: set(), 81: set()},
        enforce_promises_on_active_assignments=lambda ids: {"removed_from_contract_advisors": 0},
        get_current_assignments=lambda: {45: set(), 81: set()},
        _require_contract_service=lambda: contract_service,
        balance_assignments=lambda **k: (na, {}),
        save_assignments=lambda *a, **k: {
            "inserted_total": 0, "inserted_cobyser": 0, "inserted_serlefin": 0
        },
        assign_franja_by_parity=lambda **k: {"assigned": 0},
        _run_twist_assignments=lambda results: results.update(
            {"twist1_stats": {}, "twist2_stats": {}}
        ),
        refresh_estado_actual_for_assignments=lambda ca: {"rows_updated": 0},
        _maybe_send_reports=lambda *a: None,
        _maybe_send_completion_notification=lambda *a: None,
    )
    return fake


def test_execute_assignment_process_flujo_exitoso():
    fake = _fake_service()
    results = AssignmentService.execute_assignment_process(fake)
    assert results["success"] is True
    assert "insert_stats" in results
    assert "franja_cobyser_stats" in results
    assert "duration_seconds" in results
    assert results["error"] is None


def test_execute_assignment_process_con_nuevos():
    fake = _fake_service(new_assignments={45: [1, 2], 81: [3]})
    results = AssignmentService.execute_assignment_process(fake)
    assert results["success"] is True
    assert results["balance_stats"] == {45: 2, 81: 1}
