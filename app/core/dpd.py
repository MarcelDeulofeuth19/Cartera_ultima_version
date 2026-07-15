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
    "210_240",
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

# Buckets de la "franja Cobyser" (dias 31-60): se asignan SOLO a Cobyser (45)
# y SOLO a cedulas cuyo digito final es impar (1, 3, 5, 7, 9). Serlefin 0%.
FRANJA_COBYSER_BUCKETS = ("31_45", "46_60")

# Digitos finales de cedula: impar -> Cobyser, par -> Serlefin (franja 31-60).
ODD_CEDULA_DIGITS = frozenset("13579")
EVEN_CEDULA_DIGITS = frozenset("02468")


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


def is_cedula_impar(documento: Optional[str]) -> bool:
    """
    Indica si una cedula/documento termina en digito impar (1, 3, 5, 7, 9).

    La paridad se evalua sobre el documento normalizado a SOLO digitos, para
    ignorar puntos, guiones o espacios. Documentos vacios o sin digitos se
    consideran NO impares (no entran a la franja Cobyser).
    """
    digits = "".join(ch for ch in str(documento or "") if ch.isdigit())
    if not digits:
        return False
    return digits[-1] in ODD_CEDULA_DIGITS


def is_cedula_par(documento: Optional[str]) -> bool:
    """
    Indica si una cedula/documento termina en digito PAR (0, 2, 4, 6, 8).

    Documentos vacios o sin digitos se consideran NO pares (quedan fuera de la
    franja, igual que las impares vacias).
    """
    digits = "".join(ch for ch in str(documento or "") if ch.isdigit())
    if not digits:
        return False
    return digits[-1] in EVEN_CEDULA_DIGITS


def cedula_parity(documento: Optional[str]) -> Optional[str]:
    """
    Clasifica la cedula para la franja 31-60:
      'impar' -> Cobyser, 'par' -> Serlefin, None -> sin digito (no se asigna).
    """
    digits = "".join(ch for ch in str(documento or "") if ch.isdigit())
    if not digits:
        return None
    return "impar" if digits[-1] in ODD_CEDULA_DIGITS else "par"


def get_assignment_dpd_range(days_overdue: Optional[int]) -> Optional[str]:
    """
    Version para balanceo/asignacion principal.
    Retorna rangos desde 0 hasta 210_240.
    """
    if days_overdue is None:
        return None

    if days_overdue >= 210:
        return "210_240"
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
