"""
Pruebas unitarias de HistoryService con SESIÓN FALSA (sin BD).

Cubre register_assignments, close_assignments, get_active_assignments,
get_history_stats y los helpers de resolución de campos.
"""
from datetime import datetime

import pytest

from app.database.models import ContractAdvisorHistory
from app.services.history_service import HistoryService

pytestmark = pytest.mark.unit


class _Q:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *a, **k):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def count(self):
        return len(self._rows)


class _Session:
    """Sesión falsa: query() devuelve, en orden, cada lote encolado."""

    def __init__(self, queued=None):
        self.queued = list(queued or [])
        self.bulk = []
        self.added = []
        self.commits = 0
        self.rollbacks = 0

    def query(self, *a):
        return _Q(self.queued.pop(0) if self.queued else [])

    def bulk_insert_mappings(self, model, rows):
        self.bulk.append((model, list(rows)))

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


# --- helpers puros ----------------------------------------------------------
def test_to_int_or_none():
    assert HistoryService._to_int_or_none(None) is None
    assert HistoryService._to_int_or_none("5") == 5
    assert HistoryService._to_int_or_none("x") is None
    assert HistoryService._to_int_or_none(7) == 7


def test_resolve_initial_fields_defaults():
    svc = HistoryService(_Session())
    fields = svc._resolve_initial_fields(1, None, "ASIGNACION")
    assert fields["tipo"] == "ASIGNACION"
    assert fields["estado_actual"] == "SIN_ESTADO"
    assert fields["producto"] == "PHONE"


def test_resolve_initial_fields_metadata():
    svc = HistoryService(_Session())
    meta = {1: {"dias_atraso_inicial": 70, "tipo": "CEDULAS_IMPAR",
                "estado_actual": "MORA", "producto": "TWIST1"}}
    f = svc._resolve_initial_fields(1, meta, "ASIGNACION")
    assert f["tipo"] == "CEDULAS_IMPAR"
    assert f["dias_atraso_inicial"] == 70
    assert f["dpd_inicial"] == "61_90"
    assert f["producto"] == "TWIST1"


def test_resolve_terminal_fields_defaults():
    svc = HistoryService(_Session())
    f = svc._resolve_terminal_fields(1, None)
    assert f["tipo"] == "REMOVIDO"
    assert f["estado_actual"] == "SIN_ESTADO"


# --- register_assignments ---------------------------------------------------
def test_register_inserta_nuevos():
    sess = _Session(queued=[[]])  # no hay activos existentes
    svc = HistoryService(sess)
    stats = svc.register_assignments(
        {45: [1, 2], 81: [3]},
        assignment_metadata={1: {"dias_atraso_inicial": 70}},
    )
    assert stats["total_registered"] == 3
    assert stats["cobyser"] == 2 and stats["serlefin"] == 1
    assert sess.bulk and len(sess.bulk[0][1]) == 3
    assert sess.commits == 1


def test_register_salta_par_existente():
    sess = _Session(queued=[[(1, 45)]])  # (contract, user) ya activo
    svc = HistoryService(sess)
    stats = svc.register_assignments({45: [1, 2]})
    assert stats["total_registered"] == 1  # solo el contrato 2


def test_register_vacio():
    sess = _Session()
    stats = HistoryService(sess).register_assignments({})
    assert stats["total_registered"] == 0
    assert sess.commits == 0


# --- close_assignments ------------------------------------------------------
def test_close_actualiza_y_crea():
    rec = ContractAdvisorHistory(user_id=45, contract_id=1)
    rec.fecha_terminal = None
    rec.dpd_inicial = None
    rec.dias_atraso_inicial = None
    sess = _Session(queued=[[rec]])  # activo solo (1,45)
    svc = HistoryService(sess)
    stats = svc.close_assignments(
        {45: [1], 81: [2]},
        terminal_metadata={1: {"dias_atraso_terminal": 80, "tipo": "CIERRE"},
                           2: {"dias_atraso_terminal": 90}},
    )
    assert stats["updated"] == 1      # (1,45) actualizado in-place
    assert stats["inserted"] == 1     # (2,81) nuevo
    assert stats["total_closed"] == 2
    assert rec.fecha_terminal is not None
    assert rec.tipo == "CIERRE"
    assert len(sess.added) == 1


def test_close_vacio():
    sess = _Session()
    stats = HistoryService(sess).close_assignments({})
    assert stats["total_closed"] == 0


# --- consultas --------------------------------------------------------------
def test_get_active_assignments():
    r1 = ContractAdvisorHistory(user_id=45, contract_id=1)
    r2 = ContractAdvisorHistory(user_id=45, contract_id=2)
    r3 = ContractAdvisorHistory(user_id=81, contract_id=3)
    sess = _Session(queued=[[r1, r2, r3]])
    res = HistoryService(sess).get_active_assignments()
    assert res[45] == {1, 2} and res[81] == {3}


def test_get_active_assignments_con_filtro_usuarios():
    r1 = ContractAdvisorHistory(user_id=45, contract_id=1)
    sess = _Session(queued=[[r1]])
    res = HistoryService(sess).get_active_assignments(user_ids=[45])
    assert res == {45: {1}}


def test_get_history_stats():
    sess = _Session(queued=[[0, 0, 0], [0], [0, 0]])  # total=3, activos=1, cerrados=2
    stats = HistoryService(sess).get_history_stats()
    assert stats == {
        "total_records": 3,
        "active_assignments": 1,
        "closed_assignments": 2,
    }


# --- builders/updates terminales (estáticos) --------------------------------
def test_build_terminal_history():
    tf = {
        "tipo": "CIERRE", "dpd_terminal": "61_90", "dpd_actual": "61_90",
        "dias_atraso_terminal": 70, "dpd_inicial": None, "dias_atraso_inicial": None,
        "estado_actual": "MORA",
    }
    rec = HistoryService._build_terminal_history(1, 45, tf, datetime(2026, 7, 15))
    assert rec.contract_id == 1 and rec.user_id == 45
    assert rec.tipo == "CIERRE"
    assert rec.dias_atraso_inicial == 70          # fallback al terminal
    assert rec.fecha_terminal == datetime(2026, 7, 15)


def test_apply_terminal_update():
    rec = ContractAdvisorHistory(user_id=45, contract_id=1)
    rec.dpd_inicial = None
    rec.dias_atraso_inicial = None
    tf = {
        "tipo": "REMOVIDO", "dpd_terminal": "91_120", "dpd_actual": "91_120",
        "dias_atraso_terminal": 100, "dpd_inicial": "61_90", "dias_atraso_inicial": 70,
        "estado_actual": "X",
    }
    HistoryService._apply_terminal_update(rec, tf, datetime(2026, 7, 15))
    assert rec.fecha_terminal == datetime(2026, 7, 15)
    assert rec.tipo == "REMOVIDO"
    assert rec.dpd_inicial == "61_90"       # se rellena por estar en None
    assert rec.dias_atraso_inicial == 70
