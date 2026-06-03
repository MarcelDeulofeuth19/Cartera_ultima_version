import logging
import sys
from pathlib import Path

# Agregar el directorio raiz al path
sys.path.append(str(Path(__file__).parent))

from app.database.connections import db_manager
from app.services.assignment_service import AssignmentService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Iniciando reset de asignaciones...")
    with db_manager.get_mysql_session() as mysql_session:
        with db_manager.get_postgres_session() as postgres_session:
            service = AssignmentService(mysql_session, postgres_session)
            
            # 1. Cierre masivo de asignaciones
            logger.info("Cerrando todas las asignaciones activas (limpiando contract_advisors)...")
            close_stats = service.finalize_all_active_assignments()
            logger.info(f"Estadisticas de cierre: {close_stats}")
            
            # 2. Re-asignar bases
            logger.info("Reasignando bases (execute_assignment_process)...")
            assign_results = service.execute_assignment_process()
            logger.info(f"Resultado de asignacion: {assign_results.get('success')}")

if __name__ == "__main__":
    main()
