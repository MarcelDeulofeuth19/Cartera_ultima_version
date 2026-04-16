"""
Script para generar y enviar informes de asignacion por correo electronico.
Se puede ejecutar manualmente fuera del scheduler.
"""
import logging
import sys
from pathlib import Path

# Agregar el directorio raiz al path
sys.path.append(str(Path(__file__).parent))

from app.core.config import settings
from app.services.email_service import email_service
from app.services.report_service_extended import report_service_extended

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    print("=" * 80)
    print("GENERACION Y ENVIO DE INFORMES DE ASIGNACION")
    print("=" * 80)

    logger.info("Iniciando generacion completa y envio a todos los destinatarios mediante AssignmentService.")
    
    from app.services.assignment_service import AssignmentService
    from app.database.connections import db_manager
    
    with db_manager.get_mysql_session() as mysql_session:
        with db_manager.get_postgres_session() as postgres_session:
            service = AssignmentService(mysql_session=mysql_session, postgres_session=postgres_session)
            success = service.generate_and_send_reports()
    
    if success:
        print("\n" + "=" * 80)
        print("PROCESO DE GENERACION Y ENVIO GLOBAL COMPLETADO EXITOSAMENTE")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("HUBO ERRORES EN EL PROCESO DE GENERACION O ENVIO global")
        print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logger.error(f"Error critico: {error}", exc_info=True)
        sys.exit(1)
