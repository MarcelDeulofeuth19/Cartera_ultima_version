"""
Endpoints para informes de casa de cobranza (SERLEFIN y COBYSER)
"""
import logging
import os
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.database.connections import get_mysql_session, get_postgres_session
from app.services.collection_agency_report_service import CollectionAgencyReportService

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "/informe-casa-cobranza",
    summary="Generar informe de casa de cobranza",
    description="""
    Genera informes Excel para casas de cobranza (SERLEFIN y COBYSER).
    
    - **SERLEFIN**: Informe para usuario 81
    - **COBYSER**: Informe para usuario 45
    
    Los archivos se generan con la fecha actual y se guardan en el directorio de reportes.
    
    **Lógica del informe:**
    - Obtiene contratos asignados a cada usuario
    - Calcula capital pendiente, gastos vencidos, deuda actual
    - Genera opciones de pago con descuentos
    - Calcula comisiones según días de mora
    - Incluye información del cliente y estado de cuotas
    
    **Archivos generados:**
    - `AloCredit-Phone-{fecha} INFORME MARTES Y JUEVES.xlsx` (SERLEFIN)
    - `AloCredit-Phone-{fecha} INFORME MARTES Y JUEVES Cobyser.xlsx` (COBYSER)
    """
)
def generate_collection_agency_report(
    mysql_session: Session = Depends(get_mysql_session),
    postgres_session: Session = Depends(get_postgres_session)
):
    """
    Genera informes de casa de cobranza para SERLEFIN (user 81) y COBYSER (user 45)
    
    Returns:
        JSON con información de los archivos generados y estadísticas
    """
    try:
        logger.info("=" * 80)
        logger.info("INICIANDO GENERACIÓN DE INFORMES DE CASA DE COBRANZA")
        logger.info("=" * 80)
        
        # Crear servicio
        service = CollectionAgencyReportService(postgres_session, mysql_session)
        
        # Generar informes
        result = service.generate_reports()
        
        # Preparar respuesta
        response = {
            "success": True,
            "message": "Informes generados exitosamente",
            "serlefin": {
                "contracts_count": result['serlefin_contracts'],
                "file_generated": result['serlefin_file'] is not None,
                "file_path": result['serlefin_file']
            },
            "cobyser": {
                "contracts_count": result['cobyser_contracts'],
                "file_generated": result['cobyser_file'] is not None,
                "file_path": result['cobyser_file']
            },
            "total_contracts": result['serlefin_contracts'] + result['cobyser_contracts']
        }
        
        if not result['serlefin_file'] and not result['cobyser_file']:
            response["success"] = False
            response["message"] = "No se generaron informes. No hay contratos asignados a los usuarios 81 y 45."
        
        logger.info(f"✅ Respuesta: {response}")
        return response
        
    except Exception as e:
        logger.error(f"❌ Error al generar informes: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error al generar informes de casa de cobranza: {str(e)}"
        )


@router.get(
    "/descargar-informe/{tipo}",
    summary="Descargar informe de casa de cobranza",
    description="""
    Descarga el último informe generado para una casa de cobranza específica.
    
    **Parámetros:**
    - `tipo`: Tipo de casa de cobranza
      - `serlefin`: Informe para SERLEFIN (usuario 81)
      - `cobyser`: Informe para COBYSER (usuario 45)
    
    **Nota:** El archivo debe haber sido generado previamente usando el endpoint POST `/informe-casa-cobranza`
    """
)
def download_collection_agency_report(tipo: str):
    """
    Descarga el último informe generado para una casa de cobranza
    
    Args:
        tipo: Tipo de casa de cobranza (serlefin o cobyser)
    
    Returns:
        Archivo Excel para descarga
    """
    try:
        from datetime import datetime
        from app.core.config import settings
        
        fecha_actual = datetime.now().strftime('%d-%m-%y')
        reports_dir = settings.REPORTS_DIR
        
        # Determinar nombre de archivo según tipo
        if tipo.lower() == 'serlefin':
            file_name = f"AloCredit-Phone-{fecha_actual}  INFORME MARTES Y JUEVES.xlsx"
        elif tipo.lower() == 'cobyser':
            file_name = f"AloCredit-Phone-{fecha_actual}  INFORME MARTES Y JUEVES Cobyser.xlsx"
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Tipo inválido: {tipo}. Usa 'serlefin' o 'cobyser'"
            )
        
        file_path = os.path.join(reports_dir, file_name)
        
        # Verificar si existe el archivo
        if not os.path.exists(file_path):
            raise HTTPException(
                status_code=404,
                detail=f"Informe no encontrado. Genera primero el informe usando POST /informe-casa-cobranza"
            )
        
        logger.info(f"📥 Descargando informe: {file_name}")
        
        return FileResponse(
            path=file_path,
            filename=file_name,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error al descargar informe: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error al descargar informe: {str(e)}"
        )


@router.get(
    "/listar-informes",
    summary="Listar informes disponibles",
    description="""
    Lista todos los informes de casa de cobranza disponibles en el directorio de reportes.
    
    Incluye información de:
    - Nombre del archivo
    - Fecha de creación
    - Tamaño del archivo
    - Tipo de casa de cobranza
    """
)
def list_collection_agency_reports():
    """
    Lista todos los informes de casa de cobranza disponibles
    
    Returns:
        Lista de informes con metadata
    """
    try:
        from app.core.config import settings
        import os
        from datetime import datetime
        
        reports_dir = settings.REPORTS_DIR
        
        if not os.path.exists(reports_dir):
            return {
                "success": True,
                "message": "No hay informes disponibles",
                "reports": []
            }
        
        # Listar archivos Excel que coincidan con el patrón
        reports = []
        for file_name in os.listdir(reports_dir):
            if file_name.startswith("AloCredit-Phone-") and file_name.endswith(".xlsx"):
                file_path = os.path.join(reports_dir, file_name)
                file_stats = os.stat(file_path)
                
                # Determinar tipo
                tipo = "cobyser" if "Cobyser" in file_name else "serlefin"
                
                reports.append({
                    "file_name": file_name,
                    "type": tipo,
                    "size_bytes": file_stats.st_size,
                    "size_mb": round(file_stats.st_size / (1024 * 1024), 2),
                    "created_at": datetime.fromtimestamp(file_stats.st_ctime).isoformat(),
                    "modified_at": datetime.fromtimestamp(file_stats.st_mtime).isoformat()
                })
        
        # Ordenar por fecha de modificación (más reciente primero)
        reports.sort(key=lambda x: x['modified_at'], reverse=True)
        
        return {
            "success": True,
            "message": f"Se encontraron {len(reports)} informes",
            "total_reports": len(reports),
            "reports": reports
        }
        
    except Exception as e:
        logger.error(f"❌ Error al listar informes: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error al listar informes: {str(e)}"
        )
