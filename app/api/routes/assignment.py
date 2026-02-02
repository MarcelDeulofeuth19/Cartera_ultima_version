"""
Router de endpoints para el proceso de asignación de contratos.
"""
import logging
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Optional
from datetime import datetime

from app.core.file_lock import acquire_process_lock, ProcessLockError, check_lock_status
from app.database.connections import db_manager
from app.services.assignment_service import AssignmentService
from app.services.report_service import ReportService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["assignment"]
)


class AssignmentResponse(BaseModel):
    """Modelo de respuesta para el proceso de asignación"""
    success: bool
    message: str
    execution_time: Optional[float] = None
    results: Optional[Dict] = None
    reports: Optional[Dict[str, str]] = None
    timestamp: str


class LockStatusResponse(BaseModel):
    """Modelo de respuesta para el estado del lock"""
    lock_file: str
    exists: bool
    is_locked: bool
    message: str


@router.post(
    "/run-assignment",
    response_model=AssignmentResponse,
    summary="Ejecutar proceso de asignación de contratos",
    description="""
    Ejecuta el proceso completo de asignación de contratos:
    1. Adquiere lock para garantizar una única instancia
    2. Consulta contratos con >= 61 días de atraso
    3. Identifica contratos fijos (effect='pago_total')
    4. Limpia asignaciones de contratos con 0-60 días (excepto fijos)
    5. Balancea y asigna contratos 50/50 entre usuarios 45 y 81
    6. Genera reportes TXT y Excel
    
    Este endpoint garantiza transaccionalidad y manejo de errores robusto.
    """,
    responses={
        200: {"description": "Proceso ejecutado exitosamente"},
        409: {"description": "Otra instancia del proceso está en ejecución"},
        500: {"description": "Error interno durante la ejecución"}
    }
)
async def run_assignment_process():
    """
    Endpoint principal que ejecuta el proceso completo de asignación.
    
    Returns:
        AssignmentResponse con resultados detallados del proceso
    """
    start_time = datetime.now()
    logger.info("=" * 100)
    logger.info(f"[{start_time}] NUEVA SOLICITUD DE ASIGNACIÓN RECIBIDA")
    logger.info("=" * 100)
    
    try:
        # Intentar adquirir el lock (garantiza singleton)
        with acquire_process_lock():
            logger.info("✓ Lock adquirido. Iniciando proceso...")
            
            # Obtener sesiones de base de datos
            with db_manager.get_mysql_session() as mysql_session, \
                 db_manager.get_postgres_session() as postgres_session:
                
                # Ejecutar proceso de asignación
                assignment_service = AssignmentService(mysql_session, postgres_session)
                results = assignment_service.execute_assignment_process()
                
                # Generar reportes
                report_service = ReportService()
                report_files = report_service.generate_all_reports(
                    results,
                    postgres_session
                )
                
                # Calcular tiempo de ejecución
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
                logger.info("=" * 100)
                logger.info(f"✓ PROCESO COMPLETADO EXITOSAMENTE en {execution_time:.2f} segundos")
                logger.info("=" * 100)
                
                # Preparar respuesta
                return AssignmentResponse(
                    success=True,
                    message="Proceso de asignación completado exitosamente",
                    execution_time=execution_time,
                    results={
                        'fixed_contracts_count': {
                            'user_45': len(results['fixed_contracts'].get(45, [])),
                            'user_81': len(results['fixed_contracts'].get(81, []))
                        },
                        'contracts_processed': len(results['contracts_to_assign']),
                        'clean_stats': results['clean_stats'],
                        'balance_stats': results['balance_stats'],
                        'insert_stats': results['insert_stats']
                    },
                    reports=report_files,
                    timestamp=end_time.isoformat()
                )
    
    except ProcessLockError as e:
        logger.warning(f"⚠️ Proceso bloqueado: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "Process already running",
                "message": str(e),
                "suggestion": "Espera a que el proceso actual termine o verifica el estado del lock"
            }
        )
    
    except Exception as e:
        logger.error(f"✗ Error crítico en el proceso: {str(e)}", exc_info=True)
        
        # Calcular tiempo hasta el fallo
        error_time = (datetime.now() - start_time).total_seconds()
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": f"Error durante la ejecución: {str(e)}",
                "execution_time": error_time,
                "timestamp": datetime.now().isoformat()
            }
        )


@router.get(
    "/lock-status",
    response_model=LockStatusResponse,
    summary="Verificar estado del lock",
    description="Verifica si hay una instancia del proceso en ejecución"
)
async def get_lock_status():
    """
    Endpoint para consultar el estado del lock sin intentar ejecutar el proceso.
    
    Returns:
        LockStatusResponse con información del estado del lock
    """
    try:
        status_info = check_lock_status()
        
        message = "Proceso disponible para ejecución"
        if status_info['is_locked']:
            message = "⚠️ Proceso en ejecución - Lock activo"
        
        return LockStatusResponse(
            lock_file=status_info['lock_file'],
            exists=status_info['exists'],
            is_locked=status_info['is_locked'],
            message=message
        )
    
    except Exception as e:
        logger.error(f"Error al verificar lock: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al verificar estado del lock: {str(e)}"
        )


@router.get(
    "/health",
    summary="Health check",
    description="Verifica el estado de salud de la API y las conexiones de bases de datos"
)
async def health_check():
    """
    Endpoint de health check para verificar conectividad.
    
    Returns:
        Estado de salud de la API y bases de datos
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "databases": {}
    }
    
    try:
        # Verificar conexión MySQL
        with db_manager.get_mysql_session() as mysql_session:
            mysql_session.execute("SELECT 1")
            health_status["databases"]["mysql"] = "connected"
    except Exception as e:
        logger.error(f"MySQL health check failed: {e}")
        health_status["databases"]["mysql"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    try:
        # Verificar conexión PostgreSQL
        with db_manager.get_postgres_session() as postgres_session:
            postgres_session.execute("SELECT 1")
            health_status["databases"]["postgres"] = "connected"
    except Exception as e:
        logger.error(f"PostgreSQL health check failed: {e}")
        health_status["databases"]["postgres"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    if health_status["status"] == "degraded":
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=health_status
        )
    
    return health_status
