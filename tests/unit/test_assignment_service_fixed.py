"""
Más cobertura de AssignmentService sin BD: get_fixed_contracts, _ensure_*_column
y save_assignments en caminos borde, con sesión falsa.
"""
import types

import pytest

from app.services.assignment_service import AssignmentService

pytestmark = pytest.mark.unit


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _ExecSession:
    """execute() devuelve, en orden, cada lote de filas encolado."""

    def __init__(self, queued=None):
        self.queued = list(queued or [])
        self.commits = 0

    def execute(self, *a, **k):
        return _Result(self.queued.pop(0) if self.queued else [])

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


def test_get_fixed_contracts_mapea_por_casa():
    rows = [
        {"target_user": 45, "contract_id": 1},
        {"target_user": 81, "contract_id": 2},
        {"target_user": 45, "contract_id": 3},
    ]
    fake = types.SimpleNamespace(postgres_session=_ExecSession(queued=[rows]))
    res = AssignmentService.get_fixed_contracts(fake)
    assert res[45] == {1, 3}
    assert res[81] == {2}


def test_ensure_estado_actual_column_ya_existe():
    fake = types.SimpleNamespace(
        postgres_session=_ExecSession(queued=[[{"cnt": 1}]]),
        _estado_actual_column_ready=False,
    )
    assert AssignmentService._ensure_estado_actual_column(fake) is True
    assert fake._estado_actual_column_ready is True


def test_ensure_estado_actual_column_cache():
    fake = types.SimpleNamespace(
        postgres_session=_ExecSession(queued=[]),
        _estado_actual_column_ready=True,  # ya listo -> retorna sin consultar
    )
    assert AssignmentService._ensure_estado_actual_column(fake) is True


def test_ensure_producto_column_crea():
    # Sin filas -> ejecuta el ALTER/UPDATE/INDEX y hace commit.
    sess = _ExecSession(queued=[])
    fake = types.SimpleNamespace(
        postgres_session=sess,
        _producto_column_ready=False,
    )
    assert AssignmentService._ensure_producto_column(fake) is True
    assert sess.commits >= 1


def test_ensure_history_dpd_column_ya_existe():
    fake = types.SimpleNamespace(
        postgres_session=_ExecSession(queued=[[{"cnt": 1}]]),
        _history_dpd_actual_column_ready=False,
    )
    assert AssignmentService._ensure_history_dpd_actual_column(fake) is True


def test_save_assignments_sin_contratos():
    fake = types.SimpleNamespace(postgres_session=_ExecSession())
    stats = AssignmentService.save_assignments(fake, {45: [], 81: []})
    assert stats["inserted_total"] == 0
