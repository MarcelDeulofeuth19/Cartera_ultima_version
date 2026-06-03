"""
Pruebas de integridad de las listas inmutables de contratos fijos manuales.

Formaliza (con asserts reales) lo que antes verificaban scripts manuales como
`verify_fixed_contracts.py` y `check_duplicate_contracts.py`.
"""
import pytest

from app.data.manual_fixed_contracts import (
    COBYSER_MANUAL_FIXED,
    SERLEFIN_MANUAL_FIXED,
    MANUAL_FIXED_CONTRACTS,
)

pytestmark = pytest.mark.unit

EXPECTED_COBYSER = 79
EXPECTED_SERLEFIN = 415
EXPECTED_TOTAL = 494


def test_cobyser_count():
    assert len(COBYSER_MANUAL_FIXED) == EXPECTED_COBYSER


def test_serlefin_count():
    assert len(SERLEFIN_MANUAL_FIXED) == EXPECTED_SERLEFIN


def test_total_count():
    assert len(COBYSER_MANUAL_FIXED) + len(SERLEFIN_MANUAL_FIXED) == EXPECTED_TOTAL


def test_no_internal_duplicates_cobyser():
    assert len(set(COBYSER_MANUAL_FIXED)) == len(COBYSER_MANUAL_FIXED)


def test_no_internal_duplicates_serlefin():
    assert len(set(SERLEFIN_MANUAL_FIXED)) == len(SERLEFIN_MANUAL_FIXED)


def test_no_overlap_between_houses():
    overlap = set(COBYSER_MANUAL_FIXED) & set(SERLEFIN_MANUAL_FIXED)
    assert overlap == set(), f"Contratos en ambas casas: {sorted(overlap)}"


def test_all_contract_ids_are_positive_ints():
    for cid in COBYSER_MANUAL_FIXED + SERLEFIN_MANUAL_FIXED:
        assert isinstance(cid, int) and cid > 0


def test_mapping_keys_and_values():
    assert set(MANUAL_FIXED_CONTRACTS.keys()) == {45, 81}
    assert MANUAL_FIXED_CONTRACTS[45] is COBYSER_MANUAL_FIXED
    assert MANUAL_FIXED_CONTRACTS[81] is SERLEFIN_MANUAL_FIXED
