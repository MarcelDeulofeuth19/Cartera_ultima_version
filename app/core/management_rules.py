"""
Reglas de negocio basadas en GESTIONES (managements) para la asignación a casas.

Complementa a app/core/assignment_rules.py (exclusión por endoso). Aquí viven las
reglas que dependen de las gestiones y de la CASA a la que pertenece el asesor que
gestionó, usando la fuente de verdad ``alocreditindicators.users.agency_id``:

    agency_id = 1  -> COBYSER   (usuario principal de asignación: 45)
    agency_id = 2  -> SERLEFIN  (usuario principal de asignación: 81)
    agency_id NULL -> NO es casa de cobranza (empleado interno)

Reglas:
  * REGLA 1 (no asignar): si un contrato tiene un ACUERDO/pago VIGENTE gestionado
    por un usuario que NO pertenece a una casa (agency_id NULL), ese contrato NO
    se asigna a las casas de cobranza.
  * REGLA 2 (mantener/mover a su casa): si un contrato tiene un ACUERDO/pago
    VIGENTE gestionado por un usuario de una casa, el contrato debe quedar en ESA
    casa (agency del acuerdo), moviéndolo si hoy estuviera en la otra.

Vigencia de una gestión:
  * ``acuerdo_pago`` (incluye variantes 'acuerdo_de_pago', 'Acuerdo de Pago'):
    vigente si ``promise_date >= hoy``.
  * ``pago_total`` (incluye 'Pago Total'): vigente si ``management_date`` cae en
    el MES CALENDARIO vigente (el del mes pasado NO cuenta).

Aplica a los tres productos con sus tablas de gestión:
``managements`` (Phone), ``managements_twist`` (Twist 1.0),
``managements_twist2`` (Twist 2.0).
"""
import re
from datetime import date, datetime
from typing import Optional, Union

# --- Casas de cobranza (users.agency_id) ------------------------------------
AGENCY_COBYSER = 1
AGENCY_SERLEFIN = 2
HOUSE_AGENCY_IDS = (AGENCY_COBYSER, AGENCY_SERLEFIN)

# Usuario principal de asignación por casa (destino del reparto).
AGENCY_PRIMARY_USER = {
    AGENCY_COBYSER: 45,   # Cobyser
    AGENCY_SERLEFIN: 81,  # Serlefin
}

# --- Efectos de gestión que representan un compromiso de pago ---------------
# Se comparan sobre el efecto NORMALIZADO (minúsculas, espacios -> '_') para
# tolerar las variantes reales de la BD ('acuerdo_pago', 'acuerdo_de_pago',
# 'Acuerdo de Pago', 'pago_total', 'Pago Total', ...).
ACUERDO_PAGO_EFFECTS = frozenset({"acuerdo_pago", "acuerdo_de_pago"})
PAGO_TOTAL_EFFECTS = frozenset({"pago_total"})


def normalize_effect(effect: Optional[str]) -> str:
    """Normaliza un efecto: minúsculas, sin espacios extra, espacios -> '_'."""
    return re.sub(r"\s+", "_", str(effect or "").strip().lower())


def is_acuerdo_pago(effect: Optional[str]) -> bool:
    """True si el efecto es un acuerdo de pago (cualquier variante)."""
    return normalize_effect(effect) in ACUERDO_PAGO_EFFECTS


def is_pago_total(effect: Optional[str]) -> bool:
    """True si el efecto es un pago total (cualquier variante)."""
    return normalize_effect(effect) in PAGO_TOTAL_EFFECTS


def is_agreement_effect(effect: Optional[str]) -> bool:
    """True si el efecto es un compromiso de pago (acuerdo o pago total)."""
    return is_acuerdo_pago(effect) or is_pago_total(effect)


def is_house_agency(agency_id: Optional[int]) -> bool:
    """True si el agency_id corresponde a una casa de cobranza (1 o 2)."""
    return agency_id in HOUSE_AGENCY_IDS


def house_user_for_agency(agency_id: Optional[int]) -> Optional[int]:
    """Usuario principal de asignación (45/81) para un agency_id de casa."""
    if agency_id is None:
        return None
    try:
        return AGENCY_PRIMARY_USER.get(int(agency_id))
    except (TypeError, ValueError):
        return None


def _same_calendar_month(when: Union[date, datetime, None], today: date) -> bool:
    """True si `when` cae en el mismo mes/año que `today`."""
    if when is None:
        return False
    return (when.year, when.month) == (today.year, today.month)


def is_vigente_agreement(
    effect: Optional[str],
    promise_date: Optional[date],
    management_date: Union[date, datetime, None],
    today: Optional[date] = None,
) -> bool:
    """
    Indica si una gestión es un compromiso de pago VIGENTE.

    - acuerdo de pago: vigente si promise_date >= hoy.
    - pago total: vigente si management_date es del mes calendario vigente.
    """
    today = today or date.today()
    if is_acuerdo_pago(effect):
        return promise_date is not None and promise_date >= today
    if is_pago_total(effect):
        return _same_calendar_month(management_date, today)
    return False


def normalized_effect_sql(effect_column: str) -> str:
    """
    Expresión SQL (Postgres) que normaliza un efecto igual que normalize_effect:
    minúsculas, trim y espacios -> '_'.
    """
    return f"regexp_replace(lower(trim({effect_column})), '\\s+', '_', 'g')"


def vigente_agreement_sql(
    effect_column: str,
    promise_date_column: str,
    management_date_column: str,
) -> str:
    """
    Condición SQL (Postgres) que es TRUE cuando la gestión es un compromiso de
    pago VIGENTE (acuerdo con promise_date>=hoy, o pago_total del mes vigente).

    Devuelve una expresión booleana lista para usar en un WHERE/CASE.
    """
    norm = normalized_effect_sql(effect_column)
    acuerdo = "','".join(sorted(ACUERDO_PAGO_EFFECTS))
    pago = "','".join(sorted(PAGO_TOTAL_EFFECTS))
    return (
        f"(({norm} IN ('{acuerdo}') AND {promise_date_column} >= CURRENT_DATE)"
        f" OR ({norm} IN ('{pago}') AND date_trunc('month', {management_date_column})"
        f" = date_trunc('month', CURRENT_DATE)))"
    )
