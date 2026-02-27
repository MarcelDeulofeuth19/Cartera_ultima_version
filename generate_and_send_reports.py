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

    logger.info("Calculando metricas de distribucion...")
    metrics = report_service_extended.calculate_distribution_metrics()

    if not metrics or metrics.get("total", 0) == 0:
        logger.warning("No hay contratos asignados. No se generaran informes.")
        return

    print("\nMETRICAS DE DISTRIBUCION:")
    print(f"   Total contratos: {metrics['total']}")
    print(f"   Serlefin (81):   {metrics['serlefin']} ({metrics['serlefin_percent']}%)")
    print(f"   Cobyser (45):    {metrics['cobyser']} ({metrics['cobyser_percent']}%)")

    logger.info("Generando informe Serlefin...")
    contracts_81 = report_service_extended.get_assigned_contracts(81)
    file_81, _ = report_service_extended.generate_report_for_user(
        user_id=81,
        user_name="Serlefin",
        contracts=contracts_81,
    )

    logger.info("Generando informe Cobyser...")
    contracts_45 = report_service_extended.get_assigned_contracts(45)
    file_45, _ = report_service_extended.generate_report_for_user(
        user_id=45,
        user_name="Cobyser",
        contracts=contracts_45,
    )

    if not file_81 and not file_45:
        logger.error("No se pudieron generar los informes")
        return

    metrics_html = report_service_extended.generate_metrics_html(metrics)

    recipients = settings.notification_recipients
    if not recipients:
        logger.error("No hay destinatarios configurados en NOTIFICATION_RECIPIENTS")
        return

    logger.info("Enviando informes por correo...")
    sent_ok = 0

    for recipient in recipients:
        success = email_service.send_multiple_reports(
            recipient=recipient,
            serlefin_file=file_81 if file_81 else "",
            cobyser_file=file_45 if file_45 else "",
            metrics_html=metrics_html,
            attach_serlefin_file=False,
            attach_cobyser_file=True,
        )
        if success:
            sent_ok += 1

    if sent_ok == len(recipients):
        print("\nINFORMES ENVIADOS EXITOSAMENTE")
        print(f"   Destinatarios: {', '.join(recipients)}")
        if file_81:
            print(f"   Archivo Serlefin: {Path(file_81).name}")
        if file_45:
            print(f"   Archivo Cobyser:  {Path(file_45).name}")
    else:
        print("\nENVIO PARCIAL O FALLIDO")
        print(f"   Correos enviados: {sent_ok}/{len(recipients)}")

    print("\n" + "=" * 80)
    print("PROCESO COMPLETADO")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logger.error(f"Error critico: {error}", exc_info=True)
        sys.exit(1)
