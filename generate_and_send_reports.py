"""
Script para generar y enviar informes de asignación por correo electrónico
Se ejecuta al finalizar el proceso de asignación de contratos
"""
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent))

from app.services.report_service_extended import report_service_extended
from app.services.email_service import email_service
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Función principal para generar y enviar informes"""
    
    print("=" * 80)
    print("📊 GENERACIÓN Y ENVÍO DE INFORMES DE ASIGNACIÓN")
    print("=" * 80)
    print()
    
    # 1. Calcular métricas de distribución
    logger.info("📈 Calculando métricas de distribución...")
    metrics = report_service_extended.calculate_distribution_metrics()
    
    if not metrics or metrics.get('total', 0) == 0:
        logger.warning("⚠️ No hay contratos asignados. No se generarán informes.")
        return
    
    print(f"\n📊 MÉTRICAS DE DISTRIBUCIÓN:")
    print(f"   Total contratos: {metrics['total']}")
    print(f"   Serlefin (81):   {metrics['serlefin']} ({metrics['serlefin_percent']}%)")
    print(f"   Cobyser (45):    {metrics['cobyser']} ({metrics['cobyser_percent']}%)")
    print(f"   Cumple 60/40:    {'✅ SÍ' if metrics['cumple_60_40'] else '⚠️ NO'}")
    print()
    
    # 2. Generar informes para Serlefin (Usuario 81)
    logger.info("📄 Generando informe Serlefin...")
    contracts_81 = report_service_extended.get_assigned_contracts(81)
    file_81, df_81 = report_service_extended.generate_report_for_user(
        user_id=81,
        user_name="Serlefin",
        contracts=contracts_81
    )
    
    # 3. Generar informes para Cobyser (Usuario 45)
    logger.info("📄 Generando informe Cobyser...")
    contracts_45 = report_service_extended.get_assigned_contracts(45)
    file_45, df_45 = report_service_extended.generate_report_for_user(
        user_id=45,
        user_name="Cobyser",
        contracts=contracts_45
    )
    
    if not file_81 and not file_45:
        logger.error("❌ No se pudieron generar los informes")
        return
    
    # 4. Generar HTML de métricas
    metrics_html = report_service_extended.generate_metrics_html(metrics)
    
    # 5. Enviar por correo electrónico
    logger.info("📧 Enviando informes por correo...")
    recipient = "mdeulofeuth@alocredit.co"
    
    success = email_service.send_multiple_reports(
        recipient=recipient,
        serlefin_file=file_81 if file_81 else "",
        cobyser_file=file_45 if file_45 else "",
        metrics_html=metrics_html
    )
    
    if success:
        print("\n✅ INFORMES ENVIADOS EXITOSAMENTE")
        print(f"   Destinatario: {recipient}")
        if file_81:
            print(f"   Archivo 1: {Path(file_81).name}")
        if file_45:
            print(f"   Archivo 2: {Path(file_45).name}")
    else:
        print("\n❌ ERROR AL ENVIAR INFORMES")
        print("   Los archivos fueron generados pero no se enviaron por correo")
    
    print("\n" + "=" * 80)
    print("🔥 PROCESO COMPLETADO")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Error crítico: {e}", exc_info=True)
        sys.exit(1)
