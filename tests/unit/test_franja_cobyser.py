"""
Pruebas unitarias de la franja Cobyser (dias 31-60, cedula impar).

Cubre logica pura (sin BD ni red):
- `is_cedula_impar`: paridad sobre el ultimo digito del documento normalizado.
- `get_franja_cobyser_odd_contracts`: filtrado por cedula impar (con stubs).
- `_build_history_metadata_from_days`: etiqueta 'tipo' por contrato (tipo_map).
- Regresion: el reparto 60/40 (`_compute_house_quotas`) no cambia.
"""
import pytest

from app.core.dpd import is_cedula_impar
from app.services.assignment_service import AssignmentService
from app.services.contract_service import ContractService

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "documento, expected",
    [
        ("1", True),
        ("3", True),
        ("5", True),
        ("7", True),
        ("9", True),
        ("0", False),
        ("2", False),
        ("4", False),
        ("6", False),
        ("8", False),
        ("1.234.567", True),   # termina en 7 tras normalizar
        ("1.234.568", False),  # termina en 8
        ("12-345-679", True),  # guiones se ignoran
        ("  123 ", True),      # espacios se ignoran, termina en 3
        ("ABC123", True),      # letras se ignoran, termina en 3
        ("", False),           # vacio -> no impar
        (None, False),         # None -> no impar
        ("abc", False),        # sin digitos -> no impar
    ],
)
def test_is_cedula_impar(documento, expected):
    assert is_cedula_impar(documento) is expected


def _make_contract_service_with_stubs(franja_contracts, document_map):
    """ContractService sin BD: se reemplazan las consultas por stubs."""
    service = ContractService(mysql_session=None)
    service.get_contracts_with_arrears = lambda min_days=None, max_days=None, excluded_contract_ids=None: list(franja_contracts)
    service.get_customer_documents_for_contracts = lambda contract_ids: dict(document_map)
    return service


def test_franja_por_paridad_impar_cobyser_par_serlefin():
    franja = [
        {"contract_id": 1, "days_overdue": 40, "total_debt": 100, "status": "MORA"},
        {"contract_id": 2, "days_overdue": 50, "total_debt": 200, "status": "MORA"},
        {"contract_id": 3, "days_overdue": 55, "total_debt": 300, "status": "MORA"},
    ]
    documents = {1: "100.000.001", 2: "100.000.002", 3: "55"}  # 1,3 impar | 2 par
    service = _make_contract_service_with_stubs(franja, documents)

    result = service.get_franja_contracts_by_parity(min_days=31, max_days=60)
    parity = {c["contract_id"]: c["parity"] for c in result}
    assert parity == {1: "impar", 2: "par", 3: "impar"}
    cedulas = {c["contract_id"]: c["cedula"] for c in result}
    assert cedulas[1] == "100000001" and cedulas[2] == "100000002" and cedulas[3] == "55"


def test_franja_sin_cedula_se_excluye():
    franja = [
        {"contract_id": 10, "days_overdue": 35, "total_debt": 1, "status": "MORA"},
    ]
    documents = {}  # sin cedula -> parity None -> excluido
    service = _make_contract_service_with_stubs(franja, documents)

    assert service.get_franja_contracts_by_parity() == []


def test_franja_vacia_retorna_lista_vacia():
    service = _make_contract_service_with_stubs([], {})
    assert service.get_franja_contracts_by_parity() == []


def test_metadata_tipo_por_contrato():
    days_map = {1: 40, 2: 80}
    tipo_map = {1: "CEDULAS_IMPAR"}
    metadata = AssignmentService._build_history_metadata_from_days(
        days_map,
        tipo="ASIGNACION",
        tipo_map=tipo_map,
    )
    assert metadata[1]["tipo"] == "CEDULAS_IMPAR"
    assert metadata[2]["tipo"] == "ASIGNACION"
    # el dpd inicial sigue calculandose por dias
    assert metadata[1]["dpd_inicial"] == "31_45"
    assert metadata[2]["dpd_inicial"] == "61_90"


def test_metadata_sin_tipo_map_usa_default():
    metadata = AssignmentService._build_history_metadata_from_days({5: 100})
    assert metadata[5]["tipo"] == "ASIGNACION"


def test_reparto_60_40_no_cambia():
    # Regresion: la franja no altera el calculo de cuotas 60/40 de 61-240.
    assert AssignmentService._compute_house_quotas(10, 0.6) == {81: 6, 45: 4}
    assert AssignmentService._compute_house_quotas(100, 0.6) == {81: 60, 45: 40}


def test_metadata_incluye_producto():
    # El historial debe etiquetar el producto (PHONE por defecto / TWIST1).
    meta_default = AssignmentService._build_history_metadata_from_days({1: 70})
    assert meta_default[1]["producto"] == "PHONE"
    meta_twist = AssignmentService._build_history_metadata_from_days(
        {2: 70}, producto="TWIST1"
    )
    assert meta_twist[2]["producto"] == "TWIST1"


def _make_twist_service_with_stubs(franja_contracts, document_map):
    service = ContractService(mysql_session=None)
    service.get_twist1_contracts_with_arrears = (
        lambda min_days=None, max_days=None, excluded_contract_ids=None: list(franja_contracts)
    )
    service.get_twist1_customer_documents_for_contracts = lambda contract_ids: dict(document_map)
    return service


def test_twist1_franja_filtra_solo_impares():
    franja = [
        {"contract_id": 11, "days_overdue": 33, "total_debt": 10, "status": "MORA"},
        {"contract_id": 12, "days_overdue": 48, "total_debt": 20, "status": "MORA"},
    ]
    documents = {11: "77.777.771", 12: "88.888.882"}  # 11 impar, 12 par
    service = _make_twist_service_with_stubs(franja, documents)

    result = service.get_twist1_franja_cobyser_odd_contracts(min_days=31, max_days=60)
    ids = sorted(c["contract_id"] for c in result)
    assert ids == [11]
    assert result[0]["cedula"] == "77777771"


def test_decide_assignments_reglas_twist():
    from app.services.twist_assignment_service import TwistAssignmentService

    contracts = [
        {"line_id": "a", "days_overdue": 20, "cedula": "11111111"},   # 1-30 -> no asigna
        {"line_id": "b", "days_overdue": 40, "cedula": "12345671"},   # franja impar -> Cobyser 45
        {"line_id": "c", "days_overdue": 50, "cedula": "12345672"},   # franja par -> Serlefin 81
        {"line_id": "d", "days_overdue": 70, "cedula": "99999990"},   # 61-90 -> 40/60
        {"line_id": "e", "days_overdue": 80, "cedula": "99999991"},   # 61-90 -> 40/60
    ]
    decisions = TwistAssignmentService._decide_assignments(contracts, serlefin_ratio=0.6)
    by_line = {c["line_id"]: (uid, tipo) for c, uid, tipo in decisions}

    # 1-30 no se asigna
    assert "a" not in by_line
    # franja por paridad: impar -> Cobyser (45), par -> Serlefin (81)
    assert by_line["b"] == (45, "CEDULAS_IMPAR")
    assert by_line["c"] == (81, "CEDULAS_PAR")
    # 61-90 se reparte entre 45/81 (40/60); ambos como ASIGNACION
    assigned_6190 = {by_line["d"][0], by_line["e"][0]}
    assert assigned_6190 == {45, 81}
    assert by_line["d"][1] == "ASIGNACION" and by_line["e"][1] == "ASIGNACION"
