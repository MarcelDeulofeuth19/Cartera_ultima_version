"""
Router de endpoints para el proceso de asignación de contratos.
"""
import logging
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Optional
from datetime import datetime
from sqlalchemy import text

from app.core.file_lock import acquire_process_lock, ProcessLockError, check_lock_status
from app.database.connections import db_manager
from app.services.assignment_service import AssignmentService
from app.services.division_service import DivisionService
from app.services.report_service import ReportService
from app.data.manual_fixed_contracts import MANUAL_FIXED_CONTRACTS

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
    2. Consulta contratos entre 61 y 210 días de atraso
    3. Identifica contratos fijos (effect='pago_total' o 'acuerdo_de_pago')
    4. Limpia asignaciones de contratos con 0-60 días (excepto fijos)
    5. Balancea y asigna contratos 60/40 (SERLEFIN 60%, COBYSER 40%)
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


@router.post(
    "/run-division",
    response_model=AssignmentResponse,
    summary="Ejecutar proceso de división de contratos a 8 usuarios",
    description="""
    Ejecuta el proceso completo de división de contratos a 8 usuarios:
    1. Adquiere lock para garantizar una única instancia
    2. Consulta contratos entre 1 y 60 días de atraso
    3. Identifica contratos fijos (effect='pago_total' o 'acuerdo_de_pago')
    4. Balancea y asigna contratos equitativamente entre 8 usuarios
    5. Genera reportes TXT y Excel
    
    Usuarios de división: 3, 4, 5, 6, 7, 8, 11, 12
    
    Este endpoint garantiza transaccionalidad y manejo de errores robusto.
    """,
    responses={
        200: {"description": "Proceso ejecutado exitosamente"},
        409: {"description": "Otra instancia del proceso está en ejecución"},
        500: {"description": "Error interno durante la ejecución"}
    }
)
async def run_division_process():
    """
    Endpoint para ejecutar el proceso completo de división de contratos a 8 usuarios.
    
    Returns:
        AssignmentResponse con resultados detallados del proceso
    """
    start_time = datetime.now()
    logger.info("=" * 100)
    logger.info(f"[{start_time}] NUEVA SOLICITUD DE DIVISIÓN DE CONTRATOS RECIBIDA")
    logger.info("=" * 100)
    
    try:
        # Intentar adquirir el lock (garantiza singleton)
        with acquire_process_lock():
            logger.info("✓ Lock adquirido. Iniciando proceso de división...")
            
            # Obtener sesiones de base de datos
            with db_manager.get_mysql_session() as mysql_session, \
                 db_manager.get_postgres_session() as postgres_session:
                
                # Ejecutar proceso de división
                division_service = DivisionService(mysql_session, postgres_session)
                results = division_service.execute_division_process()
                
                # Generar reportes para los 8 usuarios
                report_service = ReportService()
                report_files = report_service.generate_division_reports(
                    results,
                    postgres_session
                )
                
                # Calcular tiempo de ejecución
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
                logger.info("=" * 100)
                logger.info(f"✓ PROCESO DE DIVISIÓN COMPLETADO EXITOSAMENTE en {execution_time:.2f} segundos")
                logger.info("=" * 100)
                
                # Preparar respuesta
                return AssignmentResponse(
                    success=True,
                    message="Proceso de división de contratos completado exitosamente",
                    execution_time=execution_time,
                    results={
                        'fixed_contracts_count': {
                            f'user_{user_id}': len(results['fixed_contracts'].get(user_id, []))
                            for user_id in [3, 4, 5, 6, 7, 8, 11, 12]
                        },
                        'contracts_processed': len(results['contracts_to_assign']),
                        'balance_stats': results['balance_stats'],
                        'insert_stats': results['insert_stats']
                    },
                    reports=report_files,
                    timestamp=end_time.isoformat()
                )
    
    except ProcessLockError as e:
        logger.warning(f"⚠️ Proceso de división bloqueado: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "Process already running",
                "message": str(e),
                "suggestion": "Espera a que el proceso actual termine o verifica el estado del lock"
            }
        )
    
    except Exception as e:
        logger.error(f"✗ Error crítico en el proceso de división: {str(e)}", exc_info=True)
        
        # Calcular tiempo hasta el fallo
        error_time = (datetime.now() - start_time).total_seconds()
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": f"Error durante la ejecución de división: {str(e)}",
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
            mysql_session.execute(text("SELECT 1"))
            health_status["databases"]["mysql"] = "connected"
    except Exception as e:
        logger.error(f"MySQL health check failed: {e}")
        health_status["databases"]["mysql"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    try:
        # Verificar conexión PostgreSQL
        with db_manager.get_postgres_session() as postgres_session:
            postgres_session.execute(text("SELECT 1"))
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


@router.post(
    "/process-manual-fixed",
    response_model=AssignmentResponse,
    summary="Procesar e insertar 100% de contratos fijos manuales de Cobyser y Serlefin",
    description="""
    Inserta el 100% de los contratos fijos manuales para ambas casas de cobranza:
    - Cobyser (Usuario 45): 79 contratos fijos manuales
    - Serlefin (Usuario 81): 415 contratos fijos manuales
    - Total: 494 contratos fijos
    
    ⚠️ IMPORTANTE: Este endpoint inserta TODOS los contratos definidos en el código,
    sin aplicar filtros de porcentaje. Los contratos fijos son independientes del
    sistema de balanceo 60/40 de las casas de cobranza.
    
    Validaciones automáticas por lotes:
    1. ✓ Verifica que el contrato exista en MySQL (alocreditprod)
    2. ✓ Verifica que el contrato no esté ya asignado (evita duplicados)
    3. ✓ Valida contra contratos fijos de managements
    4. ✓ Inserta solo contratos nuevos por lotes (1000 contratos por lote)
    5. ✓ Registra automáticamente en historial con fecha_inicial
    
    Este endpoint garantiza integridad de datos y procesamiento eficiente.
    """,
    responses={
        200: {"description": "Contratos procesados exitosamente - Retorna estadísticas detalladas"},
        409: {"description": "Otra instancia del proceso está en ejecución"},
        500: {"description": "Error interno durante la ejecución"}
    }
)
async def process_manual_fixed_contracts():
    """
    Endpoint para procesar contratos fijos manuales de Cobyser y Serlefin con validaciones.
    
    Returns:
        AssignmentResponse con estadísticas del procesamiento
    """
    start_time = datetime.now()
    logger.info("=" * 100)
    logger.info(f"[{start_time}] PROCESANDO CONTRATOS FIJOS MANUALES (COBYSER Y SERLEFIN)")
    logger.info("=" * 100)
    
    try:
        # Intentar adquirir el lock
        with acquire_process_lock():
            logger.info("✓ Lock adquirido. Iniciando procesamiento de contratos manuales...")
            
            # Obtener sesiones de base de datos
            with db_manager.get_mysql_session() as mysql_session, \
                 db_manager.get_postgres_session() as postgres_session:
                
                # Procesar contratos fijos manuales
                assignment_service = AssignmentService(mysql_session, postgres_session)
                stats = assignment_service.process_manual_fixed_contracts(MANUAL_FIXED_CONTRACTS)
                
                # Calcular tiempo de ejecución
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
                logger.info("=" * 100)
                logger.info(f"✓ PROCESAMIENTO COMPLETADO en {execution_time:.2f} segundos")
                logger.info("=" * 100)
                
                # Preparar respuesta
                return AssignmentResponse(
                    success=True,
                    message=f"Contratos fijos manuales procesados: {stats['inserted']} insertados, {stats['already_assigned']} ya existentes (Cobyser: {stats['by_user'].get(45, {}).get('inserted', 0)}, Serlefin: {stats['by_user'].get(81, {}).get('inserted', 0)})",
                    execution_time=execution_time,
                    results={
                        'total_provided': stats['total_provided'],
                        'already_assigned': stats['already_assigned'],
                        'in_managements': stats['in_managements'],
                        'inserted': stats['inserted'],
                        'by_user': stats['by_user']
                    },
                    timestamp=end_time.isoformat()
                )
    
    except ProcessLockError as e:
        logger.warning(f"⚠️ Proceso bloqueado: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "Process already running",
                "message": str(e),
                "suggestion": "Espera a que el proceso actual termine"
            }
        )
    
    except Exception as e:
        logger.error(f"✗ Error crítico en procesamiento de contratos manuales: {str(e)}", exc_info=True)
        
        error_time = (datetime.now() - start_time).total_seconds()
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": f"Error durante el procesamiento: {str(e)}",
                "execution_time": error_time,
                "timestamp": datetime.now().isoformat()
            }
        )

