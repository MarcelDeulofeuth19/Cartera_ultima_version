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
    format: str = "excel"
):
    """
    Descarga el reporte Excel generado más recientemente para la casa indicada.
    Si format=json, retorna el contenido del Excel en formato JSON.
    Valores validos para house_key: 'cobyser', 'serlefin'.
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
            import math
            df = pd.read_excel(latest_file)
            df = df.replace({float('nan'): None})
            data = df.to_dict(orient="records")
            return JSONResponse(content=data)
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
    cache_filename = f"asignaciones_cache_{today_str}.json"
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
        service = AssignmentService(mysql_session=None, postgres_session=postgres_session)
        assignments = service.get_current_assignments()
        
        # Convertir sets a listas para JSON
        result = {
            str(user_id): list(contracts)
            for user_id, contracts in assignments.items()
        }
        
        payload = {"success": True, "date": today_str, "data": result}
        
        # Guardar en disco para futuras consultas hoy
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            logger.info(f"Caché diario guardado exitosamente en: {cache_path}")
        except Exception as e:
            logger.error(f"Error guardando caché: {e}")
            
        return payload
