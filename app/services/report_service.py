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
        assignments: Dict[int, List[int]],
        contracts_days_map: Dict[int, int] = None
    ) -> Dict[str, str]:
        """
        Genera archivos TXT con los IDs de contratos asignados a cada usuario.
        Incluye número secuencial y días de atraso.
        
        Args:
            assignments: Diccionario {user_id: [contract_ids]}
            contracts_days_map: Diccionario {contract_id: days_overdue} (opcional)
        
        Returns:
            Diccionario con las rutas de los archivos generados
        """
        logger.info("Generando archivos TXT de asignación...")
        
        file_paths = {}
        contracts_days_map = contracts_days_map or {}
        
        try:
            # Archivo para usuario 45
            file_45 = os.path.join(self.reports_dir, settings.REPORT_FILE_USER_45)
            with open(file_45, 'w', encoding='utf-8') as f:
                f.write(f"Asignación de Contratos - Usuario 45 (COBYSER)\n")
                f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total de contratos: {len(assignments.get(45, []))}\n")
                f.write("=" * 70 + "\n")
                f.write(f"{'#':<6} {'Contrato ID':<15} {'Días Atraso':<15}\n")
                f.write("=" * 70 + "\n\n")
                
                for index, contract_id in enumerate(assignments.get(45, []), start=1):
                    days = contracts_days_map.get(contract_id, 'N/A')
                    f.write(f"{index:<6} {contract_id:<15} {days:<15}\n")
            
            file_paths['user_45'] = file_45
            logger.info(f"✓ Archivo generado: {file_45}")
            
            # Archivo para usuario 81
            file_81 = os.path.join(self.reports_dir, settings.REPORT_FILE_USER_81)
            with open(file_81, 'w', encoding='utf-8') as f:
                f.write(f"Asignación de Contratos - Usuario 81 (SERLEFIN)\n")
                f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total de contratos: {len(assignments.get(81, []))}\n")
                f.write("=" * 70 + "\n")
                f.write(f"{'#':<6} {'Contrato ID':<15} {'Días Atraso':<15}\n")
                f.write("=" * 70 + "\n\n")
                
                for index, contract_id in enumerate(assignments.get(81, []), start=1):
                    days = contracts_days_map.get(contract_id, 'N/A')
                    f.write(f"{index:<6} {contract_id:<15} {days:<15}\n")
            
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
        Genera un Excel detallado con los contratos fijos.
        Incluye ambos effects: 'acuerdo_de_pago' y 'pago_total'.
        Muestra la fecha de promesa cuando es acuerdo_de_pago.
        
        Args:
            fixed_contracts: Diccionario {user_id: [contract_ids]}
            postgres_session: Sesión de PostgreSQL para consultar detalles
        
        Returns:
            Ruta del archivo Excel generado
        """
        from datetime import datetime, timedelta
        
        logger.info("Generando reporte Excel de contratos fijos...")
        
        try:
            from app.database.models import Management
            from sqlalchemy import or_
            
            # Calcular fechas de validación
            today = datetime.now().date()
            validity_datetime = datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=None
            ) - timedelta(days=settings.PAGO_TOTAL_VALIDITY_DAYS)
            hoy_naive = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=None)
            
            # Recopilar datos detallados de contratos fijos
            data = []
            
            # Obtener todos los usuarios que participan (no solo 45 y 81)
            all_contract_ids = []
            user_contracts_map = {}
            
            for user_id, contract_ids in fixed_contracts.items():
                if contract_ids:
                    all_contract_ids.extend(contract_ids)
                    for contract_id in contract_ids:
                        user_contracts_map[contract_id] = user_id
            
            if not all_contract_ids:
                logger.warning("No hay contratos fijos para generar reporte")
                # DataFrame vacío con columnas
                df = pd.DataFrame(columns=['Contract ID', 'Advisor ID', 'Casa Cobranza', 'Effect', 'Management Date', 'Promise Date'])
            else:
                # Consultar detalles de managements (AMBOS effects)
                managements = postgres_session.query(Management).filter(
                    Management.contract_id.in_(all_contract_ids),
                    or_(
                        Management.effect == settings.EFFECT_ACUERDO_PAGO,
                        Management.effect == settings.EFFECT_PAGO_TOTAL
                    )
                ).all()
                
                logger.info(f"Registros encontrados en managements: {len(managements)}")
                
                # Aplicar los mismos filtros que get_fixed_contracts
                for mgmt in managements:
                    is_valid = False
                    
                    # FILTRO: acuerdo_de_pago - solo si promise_date >= hoy
                    if mgmt.effect == settings.EFFECT_ACUERDO_PAGO:
                        if mgmt.promise_date and mgmt.promise_date >= today:
                            is_valid = True
                    
                    # FILTRO: pago_total - solo si management_date en rango [hace 30 días, hoy]
                    elif mgmt.effect == settings.EFFECT_PAGO_TOTAL:
                        if mgmt.management_date:
                            mgmt_date = mgmt.management_date
                            if mgmt_date.tzinfo is not None:
                                mgmt_date = mgmt_date.replace(tzinfo=None)
                            
                            if validity_datetime <= mgmt_date <= hoy_naive:
                                is_valid = True
                    
                    # Solo agregar si es válido
                    if is_valid:
                        # Determinar casa de cobranza
                        casa = 'COBYSER' if mgmt.user_id in settings.COBYSER_USERS else 'SERLEFIN'
                        
                        # Formatear fechas
                        mgmt_date = mgmt.management_date.strftime('%Y-%m-%d %H:%M:%S') if mgmt.management_date else 'N/A'
                        promise_date = mgmt.promise_date.strftime('%Y-%m-%d') if mgmt.promise_date else 'N/A'
                        
                        data.append({
                            'Contract ID': mgmt.contract_id,
                            'Advisor ID': mgmt.user_id,
                            'Casa Cobranza': casa,
                            'Effect': mgmt.effect,
                            'Management Date': mgmt_date,
                            'Promise Date': promise_date if mgmt.effect == settings.EFFECT_ACUERDO_PAGO else 'N/A'
                        })
                
                # Crear DataFrame
                if data:
                    df = pd.DataFrame(data)
                    df = df.sort_values(['Casa Cobranza', 'Advisor ID', 'Contract ID'])
                else:
                    df = pd.DataFrame(columns=['Contract ID', 'Advisor ID', 'Casa Cobranza', 'Effect', 'Management Date', 'Promise Date'])
            
            # Generar archivo Excel
            excel_path = os.path.join(self.reports_dir, settings.REPORT_EXCEL_FIXED)
            
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Hoja principal con todos los datos
                df.to_excel(writer, sheet_name='Contratos Fijos', index=False)
                
                # Hoja resumen por usuario
                summary_data = {
                    'Casa Cobranza': [],
                    'Usuario': [],
                    'Total Contratos Fijos': [],
                    'Acuerdo de Pago': [],
                    'Pago Total': []
                }
                
                # Resumen para COBYSER (usuario principal 45)
                cobyser_total = len(fixed_contracts.get(45, []))
                cobyser_acuerdo = len([d for d in data if d.get('Advisor ID') in settings.COBYSER_USERS and d.get('Effect') == settings.EFFECT_ACUERDO_PAGO])
                cobyser_pago = len([d for d in data if d.get('Advisor ID') in settings.COBYSER_USERS and d.get('Effect') == settings.EFFECT_PAGO_TOTAL])
                
                summary_data['Casa Cobranza'].append('COBYSER')
                summary_data['Usuario'].append('45 (principal)')
                summary_data['Total Contratos Fijos'].append(cobyser_total)
                summary_data['Acuerdo de Pago'].append(cobyser_acuerdo)
                summary_data['Pago Total'].append(cobyser_pago)
                
                # Resumen para SERLEFIN (usuario principal 81)
                serlefin_total = len(fixed_contracts.get(81, []))
                serlefin_acuerdo = len([d for d in data if d.get('Advisor ID') in settings.SERLEFIN_USERS and d.get('Effect') == settings.EFFECT_ACUERDO_PAGO])
                serlefin_pago = len([d for d in data if d.get('Advisor ID') in settings.SERLEFIN_USERS and d.get('Effect') == settings.EFFECT_PAGO_TOTAL])
                
                summary_data['Casa Cobranza'].append('SERLEFIN')
                summary_data['Usuario'].append('81 (principal)')
                summary_data['Total Contratos Fijos'].append(serlefin_total)
                summary_data['Acuerdo de Pago'].append(serlefin_acuerdo)
                summary_data['Pago Total'].append(serlefin_pago)
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Resumen', index=False)
                
                # Metadata
                metadata = pd.DataFrame({
                    'Campo': ['Fecha de Generación', 'Effects Incluidos', 'Total General', 'COBYSER Total', 'SERLEFIN Total'],
                    'Valor': [
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        f"{settings.EFFECT_ACUERDO_PAGO}, {settings.EFFECT_PAGO_TOTAL}",
                        len(data),
                        cobyser_total,
                        serlefin_total
                    ]
                })
                metadata.to_excel(writer, sheet_name='Metadata', index=False)
            
            logger.info(f"✓ Excel generado: {excel_path}")
            logger.info(f"  - Total registros: {len(data)}")
            logger.info(f"  - COBYSER (45): {cobyser_total} contratos ({cobyser_acuerdo} acuerdos, {cobyser_pago} pagos)")
            logger.info(f"  - SERLEFIN (81): {serlefin_total} contratos ({serlefin_acuerdo} acuerdos, {serlefin_pago} pagos)")
            
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
            # 1. Archivos TXT de asignación (con días de atraso)
            contracts_days_map = assignment_results.get('contracts_days_map', {})
            txt_files = self.generate_assignment_txt_files(
                assignment_results['final_assignments'],
                contracts_days_map
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
    
    def generate_division_txt_files(
        self, 
        assignments: Dict[int, List[int]],
        contracts_days_map: Dict[int, int] = None
    ) -> Dict[str, str]:
        """
        Genera archivos TXT con los IDs de contratos asignados a cada usuario de división.
        Incluye número secuencial y días de atraso.
        
        Args:
            assignments: Diccionario {user_id: [contract_ids]} para los 8 usuarios
            contracts_days_map: Diccionario {contract_id: days_overdue} (opcional)
        
        Returns:
            Diccionario con las rutas de los archivos generados
        """
        logger.info("Generando archivos TXT de división de contratos...")
        
        file_paths = {}
        contracts_days_map = contracts_days_map or {}
        
        try:
            # Generar un archivo para cada uno de los 8 usuarios
            for user_id in settings.DIVISION_USER_IDS:
                filename = settings.REPORT_FILE_DIVISION.format(user_id=user_id)
                file_path = os.path.join(self.reports_dir, filename)
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(f"División de Contratos - Usuario {user_id}\n")
                    f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Total de contratos: {len(assignments.get(user_id, []))}\n")
                    f.write(f"Rango: {settings.DIVISION_MIN_DAYS} - {settings.DIVISION_MAX_DAYS} días de atraso\n")
                    f.write("=" * 70 + "\n")
                    f.write(f"{'#':<6} {'Contrato ID':<15} {'Días Atraso':<15}\n")
                    f.write("=" * 70 + "\n\n")
                    
                    for index, contract_id in enumerate(assignments.get(user_id, []), start=1):
                        days = contracts_days_map.get(contract_id, 'N/A')
                        f.write(f"{index:<6} {contract_id:<15} {days:<15}\n")
                
                file_paths[f'user_{user_id}'] = file_path
                logger.info(f"✓ Archivo generado: {file_path}")
            
            return file_paths
        
        except Exception as e:
            logger.error(f"✗ Error al generar archivos TXT de división: {e}")
            raise
    
    def generate_division_excel(
        self, 
        assignments: Dict[int, List[int]],
        fixed_contracts: Dict[int, List[int]],
        contracts_days_map: Dict[int, int],
        postgres_session
    ) -> str:
        """
        Genera un Excel detallado con la división de contratos entre los 8 usuarios.
        Incluye contratos fijos y estadísticas por usuario.
        
        Args:
            assignments: Diccionario {user_id: [contract_ids]}
            fixed_contracts: Diccionario {user_id: [contract_ids]} con contratos fijos
            contracts_days_map: Diccionario {contract_id: days_overdue}
            postgres_session: Sesión de PostgreSQL
        
        Returns:
            Ruta del archivo Excel generado
        """
        from app.database.models import Management
        from sqlalchemy import or_
        
        logger.info("Generando Excel de división de contratos...")
        
        excel_path = os.path.join(self.reports_dir, settings.REPORT_EXCEL_DIVISION)
        
        try:
            # Preparar datos de todos los contratos asignados
            data = []
            
            for user_id in settings.DIVISION_USER_IDS:
                user_contracts = assignments.get(user_id, [])
                
                for contract_id in user_contracts:
                    # Buscar si es contrato fijo
                    is_fixed = contract_id in fixed_contracts.get(user_id, [])
                    days_overdue = contracts_days_map.get(contract_id, 'N/A')
                    
                    # Buscar detalles de managements si es fijo
                    effect = None
                    promise_date = None
                    management_date = None
                    
                    if is_fixed:
                        mgmt = postgres_session.query(Management).filter(
                            Management.contract_id == contract_id,
                            Management.user_id == user_id,
                            or_(
                                Management.effect == settings.EFFECT_ACUERDO_PAGO,
                                Management.effect == settings.EFFECT_PAGO_TOTAL
                            )
                        ).first()
                        
                        if mgmt:
                            effect = mgmt.effect
                            promise_date = mgmt.promise_date
                            management_date = mgmt.management_date
                    
                    data.append({
                        'Usuario': user_id,
                        'Contrato ID': contract_id,
                        'Días Atraso': days_overdue,
                        'Es Fijo': 'Sí' if is_fixed else 'No',
                        'Effect': effect or 'N/A',
                        'Promise Date': promise_date.strftime('%Y-%m-%d') if promise_date else 'N/A',
                        'Management Date': management_date.strftime('%Y-%m-%d %H:%M:%S') if management_date else 'N/A'
                    })
            
            df = pd.DataFrame(data)
            
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Hoja principal con todos los datos
                df.to_excel(writer, sheet_name='División Contratos', index=False)
                
                # Hoja resumen por usuario
                summary_data = {
                    'Usuario': [],
                    'Total Contratos': [],
                    'Contratos Fijos': [],
                    'Contratos Nuevos': [],
                    '% Fijos': []
                }
                
                for user_id in settings.DIVISION_USER_IDS:
                    user_total = len(assignments.get(user_id, []))
                    user_fixed = len(fixed_contracts.get(user_id, []))
                    user_new = user_total - user_fixed
                    pct_fixed = (user_fixed / user_total * 100) if user_total > 0 else 0
                    
                    summary_data['Usuario'].append(user_id)
                    summary_data['Total Contratos'].append(user_total)
                    summary_data['Contratos Fijos'].append(user_fixed)
                    summary_data['Contratos Nuevos'].append(user_new)
                    summary_data['% Fijos'].append(f"{pct_fixed:.1f}%")
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Resumen por Usuario', index=False)
                
                # Metadata
                metadata = pd.DataFrame({
                    'Campo': [
                        'Fecha de Generación',
                        'Effects Incluidos',
                        'Rango de Días',
                        'Total Usuarios',
                        'Total Contratos',
                        'Total Contratos Fijos',
                        'Total Contratos Nuevos'
                    ],
                    'Valor': [
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        f"{settings.EFFECT_ACUERDO_PAGO}, {settings.EFFECT_PAGO_TOTAL}",
                        f"{settings.DIVISION_MIN_DAYS} - {settings.DIVISION_MAX_DAYS} días",
                        len(settings.DIVISION_USER_IDS),
                        len(data),
                        sum(len(fixed_contracts.get(uid, [])) for uid in settings.DIVISION_USER_IDS),
                        sum(len(assignments.get(uid, [])) for uid in settings.DIVISION_USER_IDS) - 
                        sum(len(fixed_contracts.get(uid, [])) for uid in settings.DIVISION_USER_IDS)
                    ]
                })
                metadata.to_excel(writer, sheet_name='Metadata', index=False)
            
            logger.info(f"✓ Excel de división generado: {excel_path}")
            logger.info(f"  - Total registros: {len(data)}")
            for user_id in settings.DIVISION_USER_IDS:
                user_total = len(assignments.get(user_id, []))
                user_fixed = len(fixed_contracts.get(user_id, []))
                logger.info(f"  - Usuario {user_id}: {user_total} contratos ({user_fixed} fijos)")
            
            return excel_path
        
        except Exception as e:
            logger.error(f"✗ Error al generar Excel de división: {e}")
            raise
    
    def generate_division_reports(
        self, 
        division_results: Dict,
        postgres_session
    ) -> Dict[str, str]:
        """
        Genera todos los reportes (TXT y Excel) para la división de contratos.
        
        Args:
            division_results: Diccionario con resultados del proceso de división
            postgres_session: Sesión de PostgreSQL
        
        Returns:
            Diccionario con rutas de todos los archivos generados
        """
        logger.info("=" * 80)
        logger.info("GENERANDO REPORTES DE DIVISIÓN DE CONTRATOS")
        logger.info("=" * 80)
        
        all_files = {}
        
        try:
            # 1. Archivos TXT para cada usuario
            contracts_days_map = division_results.get('contracts_days_map', {})
            txt_files = self.generate_division_txt_files(
                division_results['final_assignments'],
                contracts_days_map
            )
            all_files.update(txt_files)
            
            # 2. Excel de división de contratos
            fixed_contracts_dict = {
                int(k): v for k, v in division_results['fixed_contracts'].items()
            }
            assignments_dict = {
                int(k): v for k, v in division_results['final_assignments'].items()
            }
            
            excel_file = self.generate_division_excel(
                assignments_dict,
                fixed_contracts_dict,
                contracts_days_map,
                postgres_session
            )
            all_files['excel_division'] = excel_file
            
            logger.info("=" * 80)
            logger.info("✓ TODOS LOS REPORTES DE DIVISIÓN GENERADOS EXITOSAMENTE")
            logger.info("=" * 80)
            logger.info("Archivos generados:")
            for key, path in all_files.items():
                logger.info(f"  - {key}: {path}")
            
            return all_files
        
        except Exception as e:
            logger.error(f"✗ Error al generar reportes de división: {e}")
            raise

