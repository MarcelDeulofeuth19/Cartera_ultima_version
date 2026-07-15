import json
import logging
from pathlib import Path
from fastapi import APIRouter, HTTPException, status, Request, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from app.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/reports",
    tags=["reports"]
)


def assignments_json_path(today_str: str) -> Path:
    """
    Ruta del JSON de asignaciones que consume un servicio externo.
    Se guarda en la raíz del proyecto: asignaciones_cache_v2_<fecha>.json
    """
    return Path(f"asignaciones_cache_v2_{today_str}.json")


def persist_assignments_json(today_str: str, payload: dict, force: bool = False) -> None:
    """
    Escribe el JSON de asignaciones como ARCHIVO en la raíz del proyecto (para el
    servicio externo que lo consume), además del caché Redis. Tolerante a fallos:
    nunca rompe la respuesta del endpoint. Con force=False solo escribe si falta.
    """
    path = assignments_json_path(today_str)
    try:
        if force or not path.exists():
            path.write_text(
                json.dumps(payload, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
            logger.info("JSON de asignaciones escrito en raíz: %s", path)
    except Exception as error:  # noqa: BLE001 - el archivo es best-effort
        logger.warning("No se pudo escribir el JSON de asignaciones en raíz: %s", error)

def _trigger_report_generation(background_tasks: BackgroundTasks, house_key: str):
    from app.database.connections import db_manager
    from app.services.assignment_service import AssignmentService

    def run_report_generation():
        try:
            with db_manager.get_mysql_session() as mysql_session:
                with db_manager.get_postgres_session() as postgres_session:
                    service = AssignmentService(mysql_session=mysql_session, postgres_session=postgres_session)
                    service.generate_and_send_reports()
        except Exception as e:
            logger.error(f"Error auto-generando reportes: {e}")

    # Lanzar la generacion en background para no bloquear
    background_tasks.add_task(run_report_generation)
    return JSONResponse(
        status_code=404,
        content={"detail": f"No se encontró el reporte de {house_key}. Se acaba de disparar la generación automática. Por favor intenta de nuevo en 15 minutos."}
    )


def _build_json_response(latest_file, product):
    try:
        import pandas as pd

        def _records(frame):
            return frame.replace({float('nan'): None}).to_dict(orient="records")

        requested = (product or "phone").strip().lower()

        # Comportamiento ACTUAL (sin product o 'phone'): primera hoja del Excel.
        if requested == "phone":
            df = pd.read_excel(latest_file)
            return JSONResponse(content=_records(df))

        # NUEVO: todas las hojas en un solo objeto por nombre de hoja.
        if requested == "all":
            sheets = pd.read_excel(latest_file, sheet_name=None)
            return JSONResponse(content={name: _records(frame) for name, frame in sheets.items()})

        # NUEVO: una hoja Twist puntual. Si el archivo es viejo (1 sola hoja)
        # y no existe la hoja, se devuelve lista vacia (no rompe).
        sheet_by_product = {"twist1": "Twist1", "twist2": "Twist2"}
        if requested in sheet_by_product:
            try:
                df = pd.read_excel(latest_file, sheet_name=sheet_by_product[requested])
            except (ValueError, KeyError):
                return JSONResponse(content=[])
            return JSONResponse(content=_records(df))

        return JSONResponse(
            status_code=400,
            content={"detail": "product invalido. Use 'phone', 'twist1', 'twist2' o 'all'."}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Error al leer el archivo Excel: {str(e)}"}
        )


@router.get("/download/{house_key}", summary="Descargar reporte por casa de cobranza")
async def download_report(
    house_key: str,
    background_tasks: BackgroundTasks,
    format: str = "excel",
    product: str = None,
):
    """
    Descarga el reporte Excel generado más recientemente para la casa indicada.
    Si format=json, retorna el contenido del Excel en formato JSON.
    Valores validos para house_key: 'cobyser', 'serlefin'.

    Parametro NUEVO (opcional) product, solo aplica con format=json:
      - (omitido) o 'phone' -> primera hoja (Phone). Comportamiento ACTUAL, sin cambios.
      - 'twist1' / 'twist2'  -> esa hoja del Excel (lista de registros).
      - 'all'                -> {"Phone": [...], "Twist1": [...], "Twist2": [...]}.
    Con format=excel se descarga el archivo completo (las 3 hojas) como hasta ahora.
    """
    valid_houses = {"cobyser", "serlefin"}
    if house_key.lower() not in valid_houses:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="house_key invalido. Debe ser 'cobyser' o 'serlefin'."
        )
        
    reports_dir = Path(settings.REPORTS_DIR)
    
    # Capitalize para buscar Serlefin o Cobyser exacto
    pattern = f"*_INFORME_{house_key.capitalize()}.xlsx"
    
    candidates = sorted(
        reports_dir.glob(pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    
    if not candidates:
        return _trigger_report_generation(background_tasks, house_key)

    latest_file = candidates[0]

    if format == "json":
        return _build_json_response(latest_file, product)

    return FileResponse(
        path=latest_file,
        filename=latest_file.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@router.get("/assignments/current", summary="Consultar asignaciones actuales por usuario (Cache diario)")
async def get_current_assignments():
    """
    Devuelve las asignaciones activas por casa de cobranza (Phone, Twist1, Twist2).
    Usa caché diario en Redis (zona horaria Bogotá): consulta la base de datos una
    sola vez por día y sirve el resultado cacheado en el resto de peticiones. Si
    Redis no está disponible degrada con elegancia y consulta la base de datos.

    Además escribe el resultado como ARCHIVO JSON en la raíz del proyecto
    (asignaciones_cache_v2_<fecha>.json) para el servicio externo que lo consume.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from app.database.connections import db_manager
    from app.services.assignment_service import AssignmentService
    from app.core.cache import cache

    # Fecha actual en Bogotá (Colombia) para el caché diario.
    tz_bogota = ZoneInfo("America/Bogota")
    today_str = datetime.now(tz_bogota).strftime("%Y-%m-%d")

    # Caché diario en Redis (v2: incluye Twist1/Twist2). Degradación elegante:
    # si Redis no está disponible, se consulta la BD sin romper la respuesta.
    cache_key = f"assignments:current:v2:{today_str}"
    cached = cache.get_json(cache_key)
    if cached is not None:
        logger.info("Sirviendo asignaciones desde caché Redis: %s", cache_key)
        # Garantiza que el archivo JSON exista para el servicio externo.
        persist_assignments_json(today_str, cached, force=False)
        return cached

    logger.info("Caché no encontrado para hoy. Consultando la base de datos...")

    # Usuarios principales por casa (sin hardcodear): Cobyser y Serlefín.
    house_user_ids = list(settings.USER_IDS) or [
        settings.FRANJA_COBYSER_USER_ID, settings.SERLEFIN_PRIMARY_USER_ID
    ]
    cobyser_id = str(settings.FRANJA_COBYSER_USER_ID)
    serlefin_id = str(settings.SERLEFIN_PRIMARY_USER_ID)
    ids_csv = ", ".join(str(int(u)) for u in house_user_ids)

    with db_manager.get_postgres_session() as postgres_session:
        from sqlalchemy import text

        service = AssignmentService(mysql_session=None, postgres_session=postgres_session)
        assignments = service.get_current_assignments()

        # Convertir sets a listas para JSON (Phone, sin cambios)
        result = {
            str(user_id): list(contracts)
            for user_id, contracts in assignments.items()
        }

        # Conteo Twist1/Twist2 por casa, aditivo.
        def _twist_counts(table: str) -> dict:
            try:
                rows = postgres_session.execute(text(
                    f"SELECT user_id, COUNT(*) FROM alocreditindicators.{table} "
                    f"WHERE user_id IN ({ids_csv}) GROUP BY user_id"
                )).fetchall()
                return {str(int(u)): int(n) for u, n in rows}
            except Exception as twist_err:
                logger.warning("No se pudo contar %s: %s", table, twist_err)
                return {}

        twist1 = _twist_counts("contract_advisors_twist")
        twist2 = _twist_counts("contract_advisors_twist2")

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

        payload = {
            "success": True,
            "date": today_str,
            "data": result,            # Phone (compatibilidad, sin cambios)
            "twist1": twist1,
            "twist2": twist2,
            "resumen": resumen,         # conteo combinado por casa/producto
        }

        # Guardar en Redis para futuras consultas del día (no-op si Redis no está).
        cache.set_json(cache_key, payload, ttl_seconds=settings.CACHE_ASSIGNMENTS_TTL_SECONDS)
        # Escribir/actualizar el archivo JSON en la raíz para el servicio externo.
        persist_assignments_json(today_str, payload, force=True)
        return payload
