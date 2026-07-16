"""
Exportador del JSON de asignaciones para el servicio externo.

Genera en la RAÍZ del proyecto el archivo ``asignaciones_cache_v2_<fecha>.json``
(mismo formato que el endpoint /api/v1/reports/assignments/current) con el conteo
de asignaciones activas por casa y producto.

Pensado para ejecutarse programado (cron) en el HOST, de forma independiente al
contenedor, justo después de la asignación diaria. Es idempotente (sobrescribe
el del día) y tolerante a fallos (loguea y sale con código != 0 si algo falla,
para que el cron lo reporte).

Uso:
    /opt/Cartera_ultima_version/.venv/bin/python scripts/export_assignments_json.py
"""
# --- bootstrap: ejecutable desde cualquier ruta ---
import sys as _sys
import pathlib as _pathlib
for _cand in (_pathlib.Path(__file__).resolve(), *_pathlib.Path(__file__).resolve().parents):
    if (_cand / "app").is_dir() and (_cand / "main.py").exists():
        _sys.path.insert(0, str(_cand))
        break
# --- fin bootstrap ---
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import text

from app.core.config import settings
from app.database.connections import db_manager
from app.services.assignment_service import AssignmentService
from app.api.routes.reports import assignments_json_path, persist_assignments_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - export_assignments_json - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _twist_counts(pg, table: str, ids_csv: str) -> dict:
    try:
        rows = pg.execute(text(
            f"SELECT user_id, COUNT(*) FROM alocreditindicators.{table} "
            f"WHERE user_id IN ({ids_csv}) GROUP BY user_id"
        )).fetchall()
        return {str(int(u)): int(n) for u, n in rows}
    except Exception as error:
        logger.warning("No se pudo contar %s: %s", table, error)
        return {}


def build_payload() -> dict:
    """Construye el payload de asignaciones actuales (Phone + Twist1 + Twist2)."""
    today = datetime.now(ZoneInfo(settings.AUTO_ASSIGNMENT_TIMEZONE)).strftime("%Y-%m-%d")
    cobyser_id = str(settings.FRANJA_COBYSER_USER_ID)
    serlefin_id = str(settings.SERLEFIN_PRIMARY_USER_ID)
    ids_csv = ", ".join(str(int(u)) for u in settings.USER_IDS)

    with db_manager.get_postgres_session() as pg:
        assignments = AssignmentService(
            mysql_session=None, postgres_session=pg
        ).get_current_assignments()
        result = {str(user_id): list(contracts) for user_id, contracts in assignments.items()}
        twist1 = _twist_counts(pg, "contract_advisors_twist", ids_csv)
        twist2 = _twist_counts(pg, "contract_advisors_twist2", ids_csv)

    ph_c, ph_s = len(result.get(cobyser_id, [])), len(result.get(serlefin_id, []))
    t1_c, t1_s = twist1.get(cobyser_id, 0), twist1.get(serlefin_id, 0)
    t2_c, t2_s = twist2.get(cobyser_id, 0), twist2.get(serlefin_id, 0)
    resumen = {
        "phone": {"cobyser": ph_c, "serlefin": ph_s, "total": ph_c + ph_s},
        "twist1": {"cobyser": t1_c, "serlefin": t1_s, "total": t1_c + t1_s},
        "twist2": {"cobyser": t2_c, "serlefin": t2_s, "total": t2_c + t2_s},
        "total": {
            "cobyser": ph_c + t1_c + t2_c,
            "serlefin": ph_s + t1_s + t2_s,
            "total": ph_c + ph_s + t1_c + t1_s + t2_c + t2_s,
        },
    }
    return {
        "success": True, "date": today, "data": result,
        "twist1": twist1, "twist2": twist2, "resumen": resumen,
    }


def main() -> int:
    try:
        payload = build_payload()
        persist_assignments_json(payload["date"], payload, force=True)
        path = assignments_json_path(payload["date"])
        logger.info("JSON de asignaciones exportado: %s (resumen=%s)", path.resolve(), payload["resumen"])
        return 0
    except Exception as error:
        logger.error("Fallo exportando el JSON de asignaciones: %s", error, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
