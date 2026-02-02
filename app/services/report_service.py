"""
Servicio de generación de reportes.
Crea archivos TXT y Excel con los resultados de la asignación.
"""
import logging
import os
from typing import Dict, List
from datetime import datetime
import pandas as pd
from app.core.config import settings

logger = logging.getLogger(__name__)


class ReportService:
    """
    Servicio para generar reportes de asignación en formato TXT y Excel.
    """
    
    def __init__(self):
        """Inicializa el servicio y verifica el directorio de reportes"""
        self.reports_dir = settings.REPORTS_DIR
        self._ensure_reports_directory()
    
    def _ensure_reports_directory(self):
        """Crea el directorio de reportes si no existe"""
        if not os.path.exists(self.reports_dir):
            os.makedirs(self.reports_dir)
            logger.info(f"✓ Directorio de reportes creado: {self.reports_dir}")
    
    def generate_assignment_txt_files(
        self, 
        assignments: Dict[int, List[int]]
    ) -> Dict[str, str]:
        """
        Genera archivos TXT con los IDs de contratos asignados a cada usuario.
        
        Args:
            assignments: Diccionario {user_id: [contract_ids]}
        
        Returns:
            Diccionario con las rutas de los archivos generados
        """
        logger.info("Generando archivos TXT de asignación...")
        
        file_paths = {}
        
        try:
            # Archivo para usuario 45
            file_45 = os.path.join(self.reports_dir, settings.REPORT_FILE_USER_45)
            with open(file_45, 'w', encoding='utf-8') as f:
                f.write(f"Asignación de Contratos - Usuario 45\n")
                f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total de contratos: {len(assignments.get(45, []))}\n")
                f.write("=" * 50 + "\n\n")
                
                for contract_id in sorted(assignments.get(45, [])):
                    f.write(f"{contract_id}\n")
            
            file_paths['user_45'] = file_45
            logger.info(f"✓ Archivo generado: {file_45}")
            
            # Archivo para usuario 81
            file_81 = os.path.join(self.reports_dir, settings.REPORT_FILE_USER_81)
            with open(file_81, 'w', encoding='utf-8') as f:
                f.write(f"Asignación de Contratos - Usuario 81\n")
                f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total de contratos: {len(assignments.get(81, []))}\n")
                f.write("=" * 50 + "\n\n")
                
                for contract_id in sorted(assignments.get(81, [])):
                    f.write(f"{contract_id}\n")
            
            file_paths['user_81'] = file_81
            logger.info(f"✓ Archivo generado: {file_81}")
            
            return file_paths
        
        except Exception as e:
            logger.error(f"✗ Error al generar archivos TXT: {e}")
            raise
    
    def generate_fixed_contracts_excel(
        self, 
        fixed_contracts: Dict[int, List[int]],
        postgres_session
    ) -> str:
        """
        Genera un Excel detallado con los contratos fijos (effect='pago_total').
        
        Args:
            fixed_contracts: Diccionario {user_id: [contract_ids]}
            postgres_session: Sesión de PostgreSQL para consultar detalles
        
        Returns:
            Ruta del archivo Excel generado
        """
        logger.info("Generando reporte Excel de contratos fijos...")
        
        try:
            from app.database.models import Management
            
            # Recopilar datos detallados de contratos fijos
            data = []
            
            for user_id, contract_ids in fixed_contracts.items():
                if not contract_ids:
                    continue
                
                # Consultar detalles de managements
                managements = postgres_session.query(Management).filter(
                    Management.contract_id.in_(contract_ids),
                    Management.advisor_id == user_id,
                    Management.effect == settings.FIXED_CONTRACT_EFFECT
                ).all()
                
                for mgmt in managements:
                    data.append({
                        'Contract ID': mgmt.contract_id,
                        'Advisor ID': mgmt.advisor_id,
                        'Effect': mgmt.effect,
                        'Management Date': mgmt.management_date.strftime('%Y-%m-%d %H:%M:%S') if mgmt.management_date else 'N/A',
                        'Notes': mgmt.notes if mgmt.notes else ''
                    })
            
            # Crear DataFrame
            if data:
                df = pd.DataFrame(data)
                df = df.sort_values(['Advisor ID', 'Contract ID'])
            else:
                # DataFrame vacío con columnas
                df = pd.DataFrame(columns=['Contract ID', 'Advisor ID', 'Effect', 'Management Date', 'Notes'])
            
            # Generar archivo Excel
            excel_path = os.path.join(self.reports_dir, settings.REPORT_EXCEL_FIXED)
            
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Hoja principal con todos los datos
                df.to_excel(writer, sheet_name='Contratos Fijos', index=False)
                
                # Hoja resumen
                summary_data = {
                    'Usuario': [],
                    'Total Contratos Fijos': []
                }
                
                for user_id in settings.USER_IDS:
                    user_contracts = len(fixed_contracts.get(user_id, []))
                    summary_data['Usuario'].append(f"Usuario {user_id}")
                    summary_data['Total Contratos Fijos'].append(user_contracts)
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Resumen', index=False)
                
                # Metadata
                metadata = pd.DataFrame({
                    'Campo': ['Fecha de Generación', 'Effect Filtrado', 'Total General'],
                    'Valor': [
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        settings.FIXED_CONTRACT_EFFECT,
                        len(data)
                    ]
                })
                metadata.to_excel(writer, sheet_name='Metadata', index=False)
            
            logger.info(f"✓ Excel generado: {excel_path} ({len(data)} registros)")
            return excel_path
        
        except Exception as e:
            logger.error(f"✗ Error al generar Excel: {e}")
            raise
    
    def generate_all_reports(
        self, 
        assignment_results: Dict,
        postgres_session
    ) -> Dict[str, str]:
        """
        Genera todos los reportes (TXT y Excel) a partir de los resultados.
        
        Args:
            assignment_results: Diccionario con resultados del proceso de asignación
            postgres_session: Sesión de PostgreSQL
        
        Returns:
            Diccionario con rutas de todos los archivos generados
        """
        logger.info("=" * 80)
        logger.info("GENERANDO REPORTES FINALES")
        logger.info("=" * 80)
        
        all_files = {}
        
        try:
            # 1. Archivos TXT de asignación
            txt_files = self.generate_assignment_txt_files(
                assignment_results['final_assignments']
            )
            all_files.update(txt_files)
            
            # 2. Excel de contratos fijos
            fixed_contracts_dict = {
                int(k): v for k, v in assignment_results['fixed_contracts'].items()
            }
            excel_file = self.generate_fixed_contracts_excel(
                fixed_contracts_dict,
                postgres_session
            )
            all_files['excel_fixed'] = excel_file
            
            logger.info("=" * 80)
            logger.info("✓ TODOS LOS REPORTES GENERADOS EXITOSAMENTE")
            logger.info("=" * 80)
            logger.info("Archivos generados:")
            for key, path in all_files.items():
                logger.info(f"  - {key}: {path}")
            
            return all_files
        
        except Exception as e:
            logger.error(f"✗ Error al generar reportes: {e}")
            raise
