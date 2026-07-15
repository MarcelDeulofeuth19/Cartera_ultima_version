"""
Cobertura de los métodos de escritura de AssignmentService con dependencias
falsas (sin BD): save_assignments, refresh_estado_actual_for_assignments,
_register_assignments_history y _bulk_sync_estado_actual.
"""
import types

import pytest

from app.services.assignment_service import AssignmentService

pytestmark = pytest.mark.unit


class _Res:
    def __init__(self, rowcount):
        self.rowcount = rowcount


class _PgSession:
    def __init__(self, rowcount=2):
        self.rowcount = rowcount
        self.bulk = []
        self.commits = 0
        self.execs = 0

    def bulk_insert_mappings(self, model, rows):
        self.bulk.append((model, list(rows)))

    def execute(self, *a, **k):
        self.execs += 1
        return _Res(self.rowcount)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


def test_save_assignments_inserta_nuevos():
    sess = _PgSession()
    fake = types.SimpleNamespace(
        postgres_session=sess,
        _query_existing_contract_ids=lambda eligible, prod: set(),
        _require_contract_service=lambda: types.SimpleNamespace(
            get_current_state_for_contracts=lambda ids: {}
        ),
        _register_assignments_history=lambda *a, **k: {"total_registered": 3},
    )
    stats = AssignmentService.save_assignments(fake, {45: [1, 2], 81: [3]})
    assert stats["inserted_total"] == 3
    assert stats["inserted_cobyser"] == 2
    assert stats["inserted_serlefin"] == 1
    assert sess.bulk and len(sess.bulk[0][1]) == 3
    assert sess.commits == 1


def test_save_assignments_salta_existentes():
    sess = _PgSession()
    fake = types.SimpleNamespace(
        postgres_session=sess,
        _query_existing_contract_ids=lambda eligible, prod: {1, 2},  # ya existen
        _require_contract_service=lambda: types.SimpleNamespace(
            get_current_state_for_contracts=lambda ids: {}
        ),
        _register_assignments_history=lambda *a, **k: {"total_registered": 1},
    )
    stats = AssignmentService.save_assignments(fake, {45: [1, 2], 81: [3]})
    assert stats["inserted_total"] == 1  # solo el 3


def test_refresh_estado_actual():
    fake = types.SimpleNamespace(
        _ensure_estado_actual_column=lambda: True,
        _ensure_history_dpd_actual_column=lambda: True,
        _require_contract_service=lambda: types.SimpleNamespace(
            get_current_state_for_contracts=lambda ids: {1: "MORA"},
            get_days_overdue_for_contracts=lambda ids: {1: 70},
        ),
        _load_dpd_map_for_contracts=lambda ids, stats: {1: "61_90"},
        _bulk_sync_estado_actual=lambda params, sync: (1, 1),
    )
    stats = AssignmentService.refresh_estado_actual_for_assignments(fake, {45: {1}, 81: set()})
    assert stats["contracts_considered"] == 1
    assert stats["rows_updated"] == 1
    assert stats["history_rows_updated"] == 1


def test_refresh_estado_actual_sin_contratos():
    fake = types.SimpleNamespace()
    stats = AssignmentService.refresh_estado_actual_for_assignments(fake, {45: set(), 81: set()})
    assert stats["contracts_considered"] == 0
    assert stats["rows_updated"] == 0


def test_register_assignments_history():
    fake = types.SimpleNamespace(
        _require_contract_service=lambda: types.SimpleNamespace(
            get_days_overdue_for_contracts=lambda ids: {}
        ),
        _build_history_metadata_from_days=AssignmentService._build_history_metadata_from_days,
        history_service=types.SimpleNamespace(
            register_assignments=lambda na, assignment_metadata=None, default_tipo=None: {
                "total_registered": 2
            }
        ),
    )
    r = AssignmentService._register_assignments_history(
        fake, [1, 2], {45: [1], 81: [2]}, {1: 70, 2: 80}, {1: "X", 2: "Y"},
        "ASIGNACION", None, "PHONE",
    )
    assert r["total_registered"] == 2


def test_bulk_sync_estado_actual():
    sess = _PgSession(rowcount=2)
    fake = types.SimpleNamespace(postgres_session=sess)
    rows, hist = AssignmentService._bulk_sync_estado_actual(
        fake, [{"contract_id": 1, "estado_actual": "MORA", "dpd_actual": "61_90"}], True
    )
    assert rows == 2 and hist == 2
    assert sess.commits == 1
