"""
Reglas de negocio de EXCLUSIÓN para la asignación a casas de cobranza.

Punto ÚNICO y explícito para las reglas que determinan qué contratos NO se
asignan a las casas de cobranza (Cobyser / Serlefin). Antes estas reglas vivían
como fragmentos de SQL repetidos dentro de los servicios; aquí quedan
centralizadas, documentadas y testeables, y es el lugar para agregar futuras
reglas sin volver a esparcir SQL por el código.

Regla actual — CONTRATOS ENDOSADOS A AFIANZADORA (pagaré):
    Un contrato "endosado" está cedido a una afianzadora y NO debe asignarse a
    las casas de cobranza ni aparecer en sus informes. En MySQL (alocreditprod)
    se identifica por ``contract.pagare_status_id`` según la tabla ``pagare_status``:

        1 = Endosado Libraval
        2 = Endosado Fianzavasa
        3 = Endosado Figarantías

    Los IDs excluidos y el on/off salen de la configuración
    (``PAGARE_EXCLUDE_ENABLED`` / ``PAGARE_EXCLUDED_STATUS_IDS``), de modo que la
    regla es configurable por entorno sin tocar código.
"""
from typing import List, Optional

from app.core.config import settings

# Etiquetas oficiales de pagare_status (para mapear / mostrar el tipo de endoso).
ENDOSO_STATUS_LABELS = {
    1: "Endosado Libraval",
    2: "Endosado Fianzavasa",
    3: "Endosado Figarantías",
}


def endorsed_status_ids() -> List[int]:
    """
    IDs de ``pagare_status`` considerados ENDOSADOS (excluibles de asignación).

    Deriva de la configuración: si la regla está deshabilitada
    (``PAGARE_EXCLUDE_ENABLED=False``) devuelve una lista vacía, es decir, no se
    excluye ningún contrato por endoso.
    """
    return list(settings.pagare_excluded_status_id_list)


def is_endorsed(pagare_status_id: Optional[int]) -> bool:
    """
    Indica si un contrato está endosado a afianzadora (regla: NO se asigna).

    ``None`` (sin endoso) se considera asignable. Valores no numéricos se tratan
    como no endosados para no romper el flujo por datos sucios.
    """
    if pagare_status_id is None:
        return False
    try:
        return int(pagare_status_id) in set(endorsed_status_ids())
    except (TypeError, ValueError):
        return False


def endorsed_exclusion_sql(pagare_status_column: str) -> str:
    """
    Fragmento SQL que EXCLUYE los contratos endosados de una consulta.

    Args:
        pagare_status_column: nombre calificado de la columna de estado de pagaré,
            p.ej. ``'c.pagare_status_id'`` (Phone) o ``'c.twist_pagare_status_id'``
            (Twist 1.0).

    Returns:
        Cláusula ``"  AND (<col> IS NULL OR <col> NOT IN (...))\\n"`` lista para
        interpolar, o cadena vacía si la regla está deshabilitada. Un pagaré NULL
        se mantiene asignable (no está endosado).
    """
    ids = endorsed_status_ids()
    if not ids:
        return ""
    in_list = ",".join(str(i) for i in ids)
    return (
        f"  AND ({pagare_status_column} IS NULL "
        f"OR {pagare_status_column} NOT IN ({in_list}))\n"
    )
