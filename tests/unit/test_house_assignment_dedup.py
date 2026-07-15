"""
Pruebas unitarias del nucleo de asignacion a CASAS DE COBRANZA (sin BD ni red).

Cubre:
- `_query_existing_contract_ids`: la deduplicacion se limita a los usuarios de
  casa (settings.USER_IDS). Esta es la correccion del bug por el que un contrato
  "ya existente" bajo un usuario que NO es casa de cobranza se saltaba y nunca se
  asignaba a la casa.
- `balance_assignments`: los contratos NUEVOS se reparten a las casas (45/81),
  mientras que los ya asignados a una casa y los bloqueados se excluyen.

Se evita instanciar `AssignmentService` (su __init__ abre la base interna de
configuracion). Los metodos se ejercen con un `self` minimo (fake) y una sesion
falsa, de modo que las pruebas no tocan ninguna base de datos.
"""
import types

import pytest

from app.core.config import settings
from app.services.assignment_service import AssignmentService

pytestmark = pytest.mark.unit


class _FakeQuery:
    """Query falsa que captura los filtros aplicados y devuelve filas fijas."""

    def __init__(self, rows):
        self._rows = rows
        self.filters = []

    def filter(self, *criteria):
        self.filters.extend(criteria)
        return self

    def all(self):
        return list(self._rows)


class _FakeSession:
    """Sesion falsa: `query(...).filter(...).all()` sin base de datos."""

    def __init__(self, rows):
        self._rows = rows
        self.last_query = None

    def query(self, *columns):
        self.last_query = _FakeQuery(self._rows)
        return self.last_query


def _call_query_existing(rows, eligible, producto):
    """Ejecuta _query_existing_contract_ids con un self/sesion falsos."""
    fake_self = types.SimpleNamespace(postgres_session=_FakeSession(rows))
    result = AssignmentService._query_existing_contract_ids(
        fake_self, eligible, producto
    )
    return result, fake_self.postgres_session.last_query


def test_dedup_restringe_a_usuarios_de_casa():
    """El filtro de existencia debe limitarse a los usuarios de casa (USER_IDS)."""
    result, query = _call_query_existing(
        rows=[(101,), (202,)],
        eligible={101, 202, 303},
        producto="PHONE",
    )

    assert result == {101, 202}

    filters_sql = " ".join(str(criterion) for criterion in query.filters)
    # La correccion del bug: se filtra por user_id ademas de contract_id.
    assert "user_id" in filters_sql
    assert "contract_id" in filters_sql


def test_dedup_usa_los_ids_de_casa_configurados():
    """Los ids del filtro user_id deben ser los de settings.USER_IDS (45, 81)."""
    _, query = _call_query_existing(
        rows=[],
        eligible={1, 2},
        producto="PHONE",
    )
    user_filters = [
        str(criterion.compile(compile_kwargs={"literal_binds": True}))
        for criterion in query.filters
        if "user_id" in str(criterion)
    ]
    assert user_filters, "no se aplico ningun filtro por user_id"
    rendered = " ".join(user_filters)
    for house_user in settings.USER_IDS:
        assert str(house_user) in rendered


def test_dedup_phone_incluye_producto_nulo():
    """Para PHONE la existencia considera producto = 'PHONE' o NULL."""
    _, query = _call_query_existing(rows=[], eligible={1}, producto="PHONE")
    filters_sql = " ".join(str(criterion) for criterion in query.filters)
    assert "producto" in filters_sql


def test_dedup_producto_twist_filtra_por_ese_producto():
    """Para un producto distinto de PHONE se filtra por igualdad de producto."""
    _, query = _call_query_existing(rows=[], eligible={1}, producto="TWIST1")
    filters_sql = " ".join(str(criterion) for criterion in query.filters)
    assert "producto" in filters_sql


# ---------------------------------------------------------------------------
# balance_assignments: reparto de contratos NUEVOS a las casas de cobranza.
# ---------------------------------------------------------------------------
def _balance_self():
    """Construye un self minimo capaz de ejecutar balance_assignments sin BD."""
    svc = types.SimpleNamespace()
    svc._compute_house_quotas = AssignmentService._compute_house_quotas
    svc._build_alternating_user_sequence = (
        AssignmentService._build_alternating_user_sequence
    )
    svc._log_balance_summary = AssignmentService._log_balance_summary
    svc._distribute_contracts_by_bucket = types.MethodType(
        AssignmentService._distribute_contracts_by_bucket, svc
    )
    return svc


def test_balance_asigna_nuevos_y_excluye_asignados_y_bloqueados():
    contracts = [
        {"contract_id": 500, "days_overdue": 70},  # ya asignado a Cobyser
        {"contract_id": 600, "days_overdue": 70},  # nuevo -> se asigna
        {"contract_id": 700, "days_overdue": 80},  # nuevo -> se asigna
        {"contract_id": 999, "days_overdue": 70},  # bloqueado -> se excluye
    ]
    current = {45: {500}, 81: set()}

    new_assignments, days_map = AssignmentService.balance_assignments(
        _balance_self(),
        contracts_with_days=contracts,
        current_assignments=current,
        serlefin_ratio=0.6,
        blocked_contract_ids={999},
    )

    assigned = set(new_assignments[45]) | set(new_assignments[81])
    assert assigned == {600, 700}          # solo los nuevos
    assert 500 not in assigned             # ya estaba en la casa
    assert 999 not in assigned             # bloqueado
    # El mapa de dias excluye los bloqueados pero conserva los demas.
    assert 999 not in days_map
    assert days_map[600] == 70 and days_map[700] == 80


def test_balance_sin_contratos_nuevos_no_asigna_nada():
    contracts = [{"contract_id": 500, "days_overdue": 70}]
    current = {45: {500}, 81: set()}

    new_assignments, _ = AssignmentService.balance_assignments(
        _balance_self(),
        contracts_with_days=contracts,
        current_assignments=current,
        serlefin_ratio=0.6,
        blocked_contract_ids=set(),
    )

    assert new_assignments[45] == [] and new_assignments[81] == []


def test_balance_solo_reparte_entre_casas_validas():
    contracts = [
        {"contract_id": i, "days_overdue": 70 + i} for i in range(1, 21)
    ]
    new_assignments, _ = AssignmentService.balance_assignments(
        _balance_self(),
        contracts_with_days=contracts,
        current_assignments={45: set(), 81: set()},
        serlefin_ratio=0.6,
        blocked_contract_ids=set(),
    )
    assert set(new_assignments.keys()) == {45, 81}
    total = len(new_assignments[45]) + len(new_assignments[81])
    assert total == 20


# ---------------------------------------------------------------------------
# _classify_fixed_contracts: fijos por promesa activa por casa (logica pura).
# ---------------------------------------------------------------------------
def test_classify_fixed_contracts_separa_faltantes_bloqueados_y_asignados():
    fixed_contracts = {45: {1, 2}, 81: {3}}
    blocked_ids = {2}
    # El contrato 3 esta fijo para Serlefin (81) pero hoy vive en Cobyser (45):
    # no se mueve (se cuenta como ya asignado). El 1 no esta asignado -> falta.
    assigned_user_by_contract = {3: 45}
    stats = {"already_assigned": 0}

    missing_by_user, missing_all = AssignmentService._classify_fixed_contracts(
        fixed_contracts, blocked_ids, assigned_user_by_contract, stats
    )

    assert missing_by_user[45] == {1}
    assert missing_by_user[81] == set()
    assert missing_all == {1}
    assert 2 not in missing_all              # bloqueado, se ignora
    assert stats["already_assigned"] == 1    # el 3 se mantiene sin mover


def test_classify_fixed_contracts_ya_en_su_casa():
    fixed_contracts = {45: {10}, 81: set()}
    assigned_user_by_contract = {10: 45}     # ya esta en su casa
    stats = {"already_assigned": 0}

    missing_by_user, missing_all = AssignmentService._classify_fixed_contracts(
        fixed_contracts, set(), assigned_user_by_contract, stats
    )

    assert missing_all == set()
    assert stats["already_assigned"] == 1
