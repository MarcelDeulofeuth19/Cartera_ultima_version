"""
Pruebas unitarias del corazon del balanceo de casas de cobranza.

`_compute_house_quotas` y `_build_alternating_user_sequence` son estaticas y
puras: definen el reparto 60/40 (o configurable) entre Serlefin (81) y
Cobyser (45). Se prueban sin tocar BD.
"""
import pytest

from app.services.assignment_service import AssignmentService

pytestmark = pytest.mark.unit

compute_quotas = AssignmentService._compute_house_quotas
build_sequence = AssignmentService._build_alternating_user_sequence


def test_quotas_60_40_exact():
    assert compute_quotas(10, 0.6) == {81: 6, 45: 4}


def test_quotas_zero_total():
    assert compute_quotas(0, 0.6) == {81: 0, 45: 0}


@pytest.mark.parametrize("total", [0, 1, 2, 3, 7, 10, 13, 100, 999])
@pytest.mark.parametrize("ratio", [0.0, 0.4, 0.5, 0.6, 0.75, 1.0])
def test_quotas_always_sum_to_total(total, ratio):
    quotas = compute_quotas(total, ratio)
    assert quotas[81] + quotas[45] == total
    assert quotas[81] >= 0 and quotas[45] >= 0


def test_quotas_ratio_is_clamped():
    # ratio fuera de [0,1] no debe producir cuotas negativas ni exceder el total
    assert compute_quotas(10, 1.5) == {81: 10, 45: 0}
    assert compute_quotas(10, -0.5) == {81: 0, 45: 10}


def test_remainder_goes_to_larger_fraction():
    # total impar con 0.6: 5*0.6=3.0 exacto -> {81:3, 45:2}
    assert compute_quotas(5, 0.6) == {81: 3, 45: 2}
    # total=4 con 0.5: empate de fracciones -> residuo a 81 (>=)
    assert compute_quotas(3, 0.5) == {81: 2, 45: 1}


@pytest.mark.parametrize("total", [0, 1, 5, 10, 13, 50])
def test_sequence_length_and_counts_match_quotas(total):
    ratio = 0.6
    quotas = compute_quotas(total, ratio)
    seq = build_sequence(total, quotas, first_user=81)
    assert len(seq) == total
    assert seq.count(81) == quotas[81]
    assert seq.count(45) == quotas[45]


def test_sequence_starts_with_first_user_when_quota_available():
    quotas = compute_quotas(10, 0.6)
    seq = build_sequence(10, quotas, first_user=81)
    assert seq[0] == 81


def test_sequence_alternates_when_possible():
    # 6/4 con alternancia desde 81: no debe haber largas rachas si hay cuota
    quotas = {81: 5, 45: 5}
    seq = build_sequence(10, quotas, first_user=81)
    assert seq == [81, 45, 81, 45, 81, 45, 81, 45, 81, 45]


def test_sequence_only_contains_valid_houses():
    quotas = compute_quotas(20, 0.6)
    seq = build_sequence(20, quotas)
    assert set(seq).issubset({45, 81})
