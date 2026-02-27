"""
Utilidades para clasificar dias de atraso (DPD) por rangos de negocio.
"""
from typing import Optional


DPD_RANGES = (
    "4_15",
    "16_30",
    "31_45",
    "46_60",
    "61_90",
    "91_120",
    "121_150",
    "151_180",
    "181_209",
    "210_MAS",
)

ASSIGNMENT_DPD_ORDER = (
    "210_MAS",
    "181_209",
    "151_180",
    "121_150",
    "91_120",
    "61_90",
    "46_60",
    "31_45",
    "16_30",
    "4_15",
    "1_3",
    "0",
)


def get_dpd_range(days_overdue: Optional[int]) -> Optional[str]:
    """
    Retorna el rango DPD configurado para un numero de dias de atraso.

    Args:
        days_overdue: Dias de atraso exactos

    Returns:
        Nombre del rango o None si no hay valor
    """
    if days_overdue is None:
        return None

    if days_overdue >= 210:
        return "210_MAS"
    if 181 <= days_overdue <= 209:
        return "181_209"
    if 151 <= days_overdue <= 180:
        return "151_180"
    if 121 <= days_overdue <= 150:
        return "121_150"
    if 91 <= days_overdue <= 120:
        return "91_120"
    if 61 <= days_overdue <= 90:
        return "61_90"
    if 46 <= days_overdue <= 60:
        return "46_60"
    if 31 <= days_overdue <= 45:
        return "31_45"
    if 16 <= days_overdue <= 30:
        return "16_30"
    if 4 <= days_overdue <= 15:
        return "4_15"
    if days_overdue <= 0:
        return "0"
    return "1_3"


def get_assignment_dpd_range(days_overdue: Optional[int]) -> Optional[str]:
    """
    Version para balanceo/asignacion principal.
    Retorna rangos desde 0 hasta 210+.
    """
    dpd = get_dpd_range(days_overdue)
    if dpd in ASSIGNMENT_DPD_ORDER:
        return dpd
    return None
