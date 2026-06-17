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
        
    latest_file = candidates[0]
    
    if format == "json":
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
    
    return FileResponse(
        path=latest_file,
        filename=latest_file.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@router.get("/assignments/current", summary="Consultar asignaciones actuales por usuario (Cache diario)")
async def get_current_assignments():
    """
    Devuelve las asignaciones activas.
    Funciona con un esquema de caché estricto: consulta la base de datos 1 sola vez
    por día (zona horaria Bogotá, Colombia) y guarda el resultado en un archivo JSON
    en la raíz del proyecto. Si reciben 500 clics, siempre sirve el archivo cacheado.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import json
    from app.database.connections import db_manager
    from app.services.assignment_service import AssignmentService
    
    # Obtener la fecha actual en Bogota, Colombia
    tz_bogota = ZoneInfo("America/Bogota")
    today_str = datetime.now(tz_bogota).strftime("%Y-%m-%d")
    
    # El archivo se guardara en la raiz del proyecto
    # v2: incluye conteo de Twist1/Twist2 (invalida cache viejo sin Twist).
    cache_filename = f"asignaciones_cache_v2_{today_str}.json"
    cache_path = Path(cache_filename)
    
    # Si el archivo de hoy ya existe, servirlo (evita la consulta a DB)
    if cache_path.exists():
        logger.info(f"Sirviendo asignaciones desde caché: {cache_path}")
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data
        except Exception as e:
            logger.error(f"Error leyendo caché: {e}")
            # Si falla leerlo, continuar para recrearlo
            pass
            
    logger.info("Caché no encontrado para hoy. Consultando la base de datos por primera vez...")
    
    with db_manager.get_postgres_session() as postgres_session:
        from sqlalchemy import text

        service = AssignmentService(mysql_session=None, postgres_session=postgres_session)
        assignments = service.get_current_assignments()

        # Convertir sets a listas para JSON (Phone, sin cambios)
        result = {
            str(user_id): list(contracts)
            for user_id, contracts in assignments.items()
        }

        # NUEVO: conteo Twist1/Twist2 por casa (45/81), aditivo.
        def _twist_counts(table: str) -> dict:
            try:
                rows = postgres_session.execute(text(
                    f"SELECT user_id, COUNT(*) FROM alocreditindicators.{table} "
                    f"WHERE user_id IN (45, 81) GROUP BY user_id"
                )).fetchall()
                return {str(int(u)): int(n) for u, n in rows}
            except Exception as twist_err:
                logger.warning("No se pudo contar %s: %s", table, twist_err)
                return {}

        twist1 = _twist_counts("contract_advisors_twist")
        twist2 = _twist_counts("contract_advisors_twist2")

        ph_45, ph_81 = len(result.get("45", [])), len(result.get("81", []))
        t1_45, t1_81 = twist1.get("45", 0), twist1.get("81", 0)
        t2_45, t2_81 = twist2.get("45", 0), twist2.get("81", 0)

        resumen = {
            "phone": {"cobyser": ph_45, "serlefin": ph_81, "total": ph_45 + ph_81},
            "twist1": {"cobyser": t1_45, "serlefin": t1_81, "total": t1_45 + t1_81},
            "twist2": {"cobyser": t2_45, "serlefin": t2_81, "total": t2_45 + t2_81},
            "total": {
                "cobyser": ph_45 + t1_45 + t2_45,
                "serlefin": ph_81 + t1_81 + t2_81,
                "total": ph_45 + ph_81 + t1_45 + t1_81 + t2_45 + t2_81,
            },
        }

        payload = {
            "success": True,
            "date": today_str,
            "data": result,            # Phone (compatibilidad, sin cambios)
            "twist1": twist1,           # NUEVO
            "twist2": twist2,           # NUEVO
            "resumen": resumen,         # NUEVO: conteo combinado por casa/producto
        }
        
        # Guardar en disco para futuras consultas hoy
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            logger.info(f"Caché diario guardado exitosamente en: {cache_path}")
        except Exception as e:
            logger.error(f"Error guardando caché: {e}")
            
        return payload
