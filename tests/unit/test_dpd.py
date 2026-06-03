"""
Pruebas unitarias de la clasificacion de dias de atraso (DPD).

`app/core/dpd.py` es logica pura (sin BD ni red): el candidato ideal para
pruebas de tabla en las fronteras de cada rango.
"""
import pytest

from app.core.dpd import get_dpd_range, get_assignment_dpd_range


pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "days, expected",
    [
        (None, None),
        (-10, "0"),
        (0, "0"),
        (1, "1_3"),
        (3, "1_3"),
        (4, "4_15"),
        (15, "4_15"),
        (16, "16_30"),
        (30, "16_30"),
        (31, "31_45"),
        (45, "31_45"),
        (46, "46_60"),
        (60, "46_60"),
        (61, "61_90"),
        (90, "61_90"),
        (91, "91_120"),
        (120, "91_120"),
        (121, "121_150"),
        (150, "121_150"),
        (151, "151_180"),
        (180, "151_180"),
        (181, "181_209"),
        (209, "181_209"),
        (210, "210_MAS"),
        (5000, "210_MAS"),
    ],
)
def test_get_dpd_range_boundaries(days, expected):
    assert get_dpd_range(days) == expected


@pytest.mark.parametrize(
    "days, expected",
    [
        (None, None),
        (-1, "0"),
        (0, "0"),
        (1, "1_3"),
        (3, "1_3"),
        (4, "4_15"),
        (60, "46_60"),
        (61, "61_90"),
        (209, "181_209"),
        (210, "210_240"),
        (9999, "210_240"),
    ],
)
def test_get_assignment_dpd_range_boundaries(days, expected):
    assert get_assignment_dpd_range(days) == expected


def test_only_difference_is_the_top_bucket():
    """Ambas funciones coinciden salvo en el bucket superior (210+)."""
    for day in [0, 1, 3, 4, 60, 61, 120, 209]:
        assert get_dpd_range(day) == get_assignment_dpd_range(day)
    assert get_dpd_range(210) == "210_MAS"
    assert get_assignment_dpd_range(210) == "210_240"
