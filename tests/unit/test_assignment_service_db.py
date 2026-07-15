"""
Pruebas unitarias de métodos de AssignmentService que tocan BD, usando
SESIÓN e history_service FALSOS (sin BD): enforce_blacklist, enforce_promises y
finalize_all_active_assignments.
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

    def delete(self, synchronize_session=False):
        return len(self._rows)


class _Session:
    """query() entrega, en orden, cada lote encolado (rows para all()/delete())."""

    def __init__(self, queued):
        self.queued = list(queued)
        self.commits = 0
        self.rollbacks = 0

    def query(self, *a):
        return _Q(self.queued.pop(0) if self.queued else [])

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class _History:
    def __init__(self):
        self.closed = None

    def close_assignments(self, contracts_removed, terminal_metadata=None):
        self.closed = contracts_removed
        total = sum(len(v) for v in contracts_removed.values())
        return {"total_closed": total, "updated": total, "inserted": 0}


def test_enforce_blacklist_remueve_y_cierra():
    active = [(45, 1), (81, 2)]
    fake = types.SimpleNamespace(
        postgres_session=_Session(queued=[active, active]),  # activos + delete
        history_service=_History(),
    )
    stats = AssignmentService.enforce_blacklist_on_active_assignments(fake, {1, 2})
    assert stats["blocked_found_active"] == 2
    assert stats["removed_from_contract_advisors"] == 2
    assert stats["history_closed"] == 2
    assert fake.postgres_session.commits == 1


def test_enforce_blacklist_sin_bloqueados():
    fake = types.SimpleNamespace(postgres_session=_Session([]), history_service=_History())
    stats = AssignmentService.enforce_blacklist_on_active_assignments(fake, set())
    assert stats["removed_from_contract_advisors"] == 0


def test_enforce_blacklist_sin_activos():
    fake = types.SimpleNamespace(
        postgres_session=_Session(queued=[[]]),  # ningún activo
        history_service=_History(),
    )
    stats = AssignmentService.enforce_blacklist_on_active_assignments(fake, {999})
    assert stats["blocked_found_active"] == 0


def test_enforce_promises_remueve_y_cierra():
    active = [(45, 10), (81, 20)]
    fake = types.SimpleNamespace(
        postgres_session=_Session(queued=[active, active]),
        history_service=_History(),
    )
    stats = AssignmentService.enforce_promises_on_active_assignments(fake, {10, 20})
    assert stats["promise_found_active"] == 2
    assert stats["removed_from_contract_advisors"] == 2
    assert stats["history_closed"] == 2


def test_finalize_sin_mysql_cierra_todo():
    active = [(45, 1), (81, 2), (45, 3)]
    fake = types.SimpleNamespace(
        postgres_session=_Session(queued=[active, active]),
        history_service=_History(),
        contract_service=None,  # sin MySQL -> cierra sin enriquecer
    )
    stats = AssignmentService.finalize_all_active_assignments(fake)
    assert stats["active_assignments_found"] == 3
    assert stats["deleted_from_contract_advisors"] == 3
    assert stats["history_closed"] == 3
    assert stats["enriched_from_mysql"] == 0


def test_finalize_sin_activos():
    fake = types.SimpleNamespace(
        postgres_session=_Session(queued=[[]]),
        history_service=_History(),
        contract_service=None,
    )
    stats = AssignmentService.finalize_all_active_assignments(fake)
    assert stats["active_assignments_found"] == 0
    assert stats["deleted_from_contract_advisors"] == 0
