"""
Script para ejecutar el proceso de división de contratos y generar Excel de asignaciones.
Se ejecuta directamente para probar el sistema de división entre 14 usuarios (día 1-60).
"""
# --- bootstrap: ejecutable desde cualquier ruta (anade la raiz del repo al path) ---
import sys as _sys, pathlib as _pathlib
for _cand in (_pathlib.Path(__file__).resolve(), *_pathlib.Path(__file__).resolve().parents):
    if (_cand / "app").is_dir() and (_cand / "main.py").exists():
        _sys.path.insert(0, str(_cand)); break
# --- fin bootstrap ---
import asyncio
import sys
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('division_process.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)


async def run_division_and_generate_excel():
    """
    Ejecuta el proceso completo de división y genera el Excel con las asignaciones.
    """
    from app.database.connections import db_manager
    from app.services.division_service import DivisionService
    from app.services.report_service import ReportService
    
    logger.info("=" * 100)
    logger.info("INICIANDO PROCESO DE DIVISIÓN DE CONTRATOS (14 USUARIOS)")
    logger.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 100)
    
    try:
        # Obtener sesiones de base de datos
        with db_manager.get_mysql_session() as mysql_session, \
             db_manager.get_postgres_session() as postgres_session:
            
            # Paso 1: Ejecutar proceso de división
            logger.info("\nPASO 1: Ejecutando proceso de división...")
            division_service = DivisionService(mysql_session, postgres_session)
            results = division_service.execute_division_process()
            
            if not results['success']:
                logger.error(f"Error en proceso de división: {results.get('error')}")
                return False
            
            # Paso 2: Generar reportes
            logger.info("\nPASO 2: Generando reportes (TXT y Excel)...")
            report_service = ReportService()
            report_files = report_service.generate_division_reports(
                results,
                postgres_session
            )
            
            # Paso 3: Mostrar resumen
            logger.info("\n" + "=" * 100)
            logger.info("✓ PROCESO COMPLETADO EXITOSAMENTE")
            logger.info("=" * 100)
            logger.info("\nRESUMEN DE ASIGNACIONES:")
            logger.info(f"  - Contratos procesados (1-60 días): {len(results['contracts_to_assign'])}")
            logger.info(f"  - Contratos fijos insertados: {results['fixed_inserted_stats']['inserted_total']}")
            logger.info(f"  - Contratos nuevos asignados: {results['insert_stats']['inserted_total']}")
            
            logger.info("\nASIGNACIÓN POR USUARIO:")
            for user_id in [4, 7, 36, 58, 60, 62, 71, 77, 89, 90, 91, 114, 116, 113]:
                count = results['balance_stats'].get(user_id, 0)
                logger.info(f"  - Usuario {user_id}: {count} contratos")
            
            logger.info("\nARCHIVOS GENERADOS:")
            for key, path in report_files.items():
                logger.info(f"  - {key}: {path}")
            
            logger.info("\n" + "=" * 100)
            logger.info("EXCEL DE ASIGNACIONES GENERADO:")
            logger.info(f"  📊 {report_files.get('excel_division', 'N/A')}")
            logger.info("=" * 100)
            
            return True
    
    except Exception as e:
        logger.error(f"✗ Error durante el proceso: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    logger.info("Iniciando script de división de contratos...")
    success = asyncio.run(run_division_and_generate_excel())
    
    if success:
        logger.info("\n✓ Script ejecutado exitosamente")
        sys.exit(0)
    else:
        logger.error("\n✗ Script falló")
        sys.exit(1)
