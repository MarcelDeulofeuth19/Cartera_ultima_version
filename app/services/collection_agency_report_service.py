"""
Servicio para generar informes de casa de cobranza (SERLEFIN y COBYSER)
Mantiene la lógica original exacta del script
"""
import logging
import os
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
import psycopg2
from sqlalchemy.orm import Session

from app.core.config import settings

logger = logging.getLogger(__name__)


class CollectionAgencyReportService:
    """Servicio para generar informes de casa de cobranza"""
    
    def __init__(self, postgres_session: Session, mysql_session: Session):
        self.postgres_session = postgres_session
        self.mysql_session = mysql_session
        
        # Configuración de bases de datos (exacta del script original)
        self.DB_CONFIG_PROD = {
            'host': '3.95.195.63',
            'user': 'nexus_dev_84',
            'password': 'ZehK7wQTpq95eU8r',
            'database': 'alocreditprod',
            'port': 5432,
            'options': '-csearch_path=alocreditprod',
            'driver': 'psycopg2'
        }
        
        self.DB_CONFIG_IND = {
            'host': '3.95.195.63',
            'user': 'nexus',
            'password': 'AloCredit2025**',
            'database': 'nexus_db',
            'port': 5432,
            'options': '-csearch_path=alocreditindicators',
            'driver': 'psycopg2'
        }
    
    def _get_assigned_contracts(self, user_id: int) -> List[int]:
        """Obtener contratos asignados a un usuario específico"""
        query = f"""
        SELECT contract_id
        FROM contract_advisors
        WHERE user_id = {user_id};
        """
        
        conn_ind = psycopg2.connect(
            host=self.DB_CONFIG_IND['host'],
            user=self.DB_CONFIG_IND['user'],
            password=self.DB_CONFIG_IND['password'],
            dbname=self.DB_CONFIG_IND['database'],
            port=self.DB_CONFIG_IND['port'],
            options=self.DB_CONFIG_IND['options']
        )
        
        try:
            df = pd.read_sql(query, conn_ind)
            return df["contract_id"].tolist() if not df.empty else []
        finally:
            conn_ind.close()
    
    def _generar_query(self, lista_contratos: str) -> str:
        """Generar query SQL para el informe (exacta del script original)"""
        return f"""
WITH 
PagosCombinadosPhone AS (
    SELECT contract_id AS Contrato,
           to_char(created_at::date, 'YYYY-MM-DD') AS FechaConvertida,
           amount AS Monto
    FROM payment_bancocolombia_confirmation
    WHERE contract_id IN ({lista_contratos})
      AND (origin IS NULL OR origin = '' OR origin = 'PHONE')

    UNION ALL

    SELECT id_reference AS Contrato,
           to_char(created_at::date, 'YYYY-MM-DD') AS FechaConvertida,
           amount
    FROM efecty_payment_confirmation
    WHERE id_reference IN ({lista_contratos})
      AND (origin IS NULL OR origin = '' OR origin = 'PHONE')

    UNION ALL

    SELECT id_reference AS Contrato,
           to_char(created_at::date, 'YYYY-MM-DD') AS FechaConvertida,
           amount
    FROM pse_payment_confirmation
    WHERE id_reference IN ({lista_contratos})
      AND (origin IS NULL OR origin = '' OR origin = 'PHONE')

    UNION ALL

    SELECT id_reference AS Contrato,
           to_char(created_at::date, 'YYYY-MM-DD') AS FechaConvertida,
           amount
    FROM puntored_payment_confirmation
    WHERE id_reference IN ({lista_contratos})
      AND (origin IS NULL OR origin = '' OR origin = 'PHONE')
),

AccesoriosPhone AS (
    SELECT
        al.application_id, 
        MAX(al.id) AS max_loan_id,
        COALESCE((
            SELECT SUM(aa.price::numeric)
            FROM application_accessory aa
            WHERE aa.application_id = al.application_id
        ), 0::numeric) AS total_precio_accesorios
    FROM application_loan al
    GROUP BY al.application_id
),

UltimaCuotaPagadaPhone AS (
    SELECT contract_id,
           outstanding_principal AS capital_ultima_pagada
    FROM (
        SELECT
            ca.*,
            ROW_NUMBER() OVER (PARTITION BY ca.contract_id ORDER BY ca.period_number DESC) AS rn
        FROM contract_amortization ca
        WHERE ca.contract_id IN ({lista_contratos})
          AND ca.contract_amortization_payment_status_id IN (1,5)
    ) x
    WHERE rn = 1
),

DiasInicialesCalculadosPhone AS (
    SELECT
        c.id AS contract_id,
        COALESCE(
            GREATEST(
                (
                    date_trunc('month', CURRENT_DATE)::date
                    - MIN(ca.expiration_date)::date
                ),
                0
            ),
            0
        )::int AS Dias_iniciales_Mes
    FROM contract c
    LEFT JOIN contract_amortization ca 
           ON ca.contract_id = c.id
          AND ca.contract_amortization_payment_status_id = 4
    WHERE c.id IN ({lista_contratos})
    GROUP BY c.id
),

Gastos AS (
    SELECT
        contract_id,
        SUM(
            COALESCE(interest_payment,0) +
            COALESCE(endorsement,0) +
            COALESCE(vat,0) +
            COALESCE(seguro_vida,0) +
            COALESCE(seguro,0) +
            COALESCE(digital_sign,0) +
            COALESCE(digital_sign_iva,0)
        ) AS gastos_vencidos
    FROM contract_amortization
    WHERE contract_id IN ({lista_contratos})
      AND contract_amortization_payment_status_id = 4
    GROUP BY contract_id
),

CuotasAtrasadas AS (
    SELECT contract_id,
           COUNT(*) AS cuotas_atrasadas
    FROM contract_amortization
    WHERE contract_id IN ({lista_contratos})
      AND contract_amortization_payment_status_id = 4
    GROUP BY contract_id
),

CuotasPagadas AS (
    SELECT contract_id,
           COUNT(*) AS cantidad_cuotas_pagados
    FROM contract_amortization
    WHERE contract_id IN ({lista_contratos})
      AND contract_amortization_payment_status_id IN (1,5)
    GROUP BY contract_id
),

CapitalPendiente AS (
    SELECT
        c.id AS contract_id,
        COALESCE(
            ucp.capital_ultima_pagada::numeric,
            (
                al.device_price::numeric
                - al.initial_pay::numeric
                + COALESCE(acc.total_precio_accesorios,0::numeric)
            )
        ) AS capital_pendiente,
        al.quota::numeric AS quota
    FROM contract c
    LEFT JOIN application a ON a.id = c.application_id
    LEFT JOIN AccesoriosPhone acc ON a.id = acc.application_id
    LEFT JOIN application_loan al 
           ON al.application_id = a.id
          AND al.id = acc.max_loan_id
    LEFT JOIN UltimaCuotaPagadaPhone ucp ON c.id = ucp.contract_id
    WHERE c.id IN ({lista_contratos})
),

DeudaActual AS (
    SELECT
        cp.contract_id,
        cp.capital_pendiente,
        cp.quota,
        COALESCE(g.gastos_vencidos::numeric, 0::numeric) AS gastos_vencidos,
        cp.capital_pendiente + COALESCE(g.gastos_vencidos::numeric, 0::numeric) AS deuda_actual
    FROM CapitalPendiente cp
    LEFT JOIN Gastos g ON g.contract_id = cp.contract_id
),

Descuentos AS (
    SELECT
        dic.contract_id,
        dic.Dias_iniciales_Mes,
        CASE 
            WHEN dic.Dias_iniciales_Mes IS NULL THEN 1::numeric
            WHEN dic.Dias_iniciales_Mes BETWEEN 0 AND 150 THEN 1::numeric
            WHEN dic.Dias_iniciales_Mes BETWEEN 151 AND 180 THEN 0.95::numeric
            WHEN dic.Dias_iniciales_Mes BETWEEN 181 AND 300 THEN 0.90::numeric
            ELSE 0.75::numeric
        END AS factor_capital,
        CASE 
            WHEN dic.Dias_iniciales_Mes IS NULL THEN 0.70::numeric
            WHEN dic.Dias_iniciales_Mes BETWEEN 0 AND 90 THEN 0.70::numeric
            WHEN dic.Dias_iniciales_Mes BETWEEN 91 AND 120 THEN 0.60::numeric
            WHEN dic.Dias_iniciales_Mes BETWEEN 121 AND 150 THEN 0.50::numeric
            WHEN dic.Dias_iniciales_Mes BETWEEN 151 AND 365 THEN 0.40::numeric
            ELSE 0::numeric
        END AS factor_gastos
    FROM DiasInicialesCalculadosPhone dic
),

ValorFinalDescuento AS (
    SELECT
        da.contract_id,
        ROUND((
            da.capital_pendiente * d.factor_capital +
            COALESCE(da.gastos_vencidos,0::numeric) * d.factor_gastos
        )::numeric, 0) AS valor_final_descuento
    FROM DeudaActual da
    LEFT JOIN Descuentos d ON d.contract_id = da.contract_id
),

OpcionesPago AS (
    SELECT
        da.contract_id,
        da.quota AS valor_opcion_1,
        da.deuda_actual AS valor_1_cuota_opcion_2,
        ROUND((da.deuda_actual / 2)::numeric, 0) AS valor_2_cuotas_opcion_2,
        CASE WHEN da.deuda_actual > 600000 THEN ROUND((da.deuda_actual / 3)::numeric, 0) END AS valor_3_cuotas_opcion_2,
        vfd.valor_final_descuento AS valor_1_cuota_opcion_3,
        ROUND((vfd.valor_final_descuento / 2)::numeric, 0) AS valor_2_cuotas_opcion_3,
        CASE WHEN vfd.valor_final_descuento > 600000 THEN ROUND((vfd.valor_final_descuento / 3)::numeric, 0) END AS valor_3_cuotas_opcion_3,
        da.capital_pendiente AS valor_1_cuota_opcion_4,
        ROUND((da.capital_pendiente / 2)::numeric, 0) AS valor_2_cuotas_opcion_4,
        CASE WHEN da.capital_pendiente > 600000 THEN ROUND((da.capital_pendiente / 3)::numeric, 0) END AS valor_3_cuotas_opcion_4
    FROM DeudaActual da
    LEFT JOIN ValorFinalDescuento vfd ON vfd.contract_id = da.contract_id
)

SELECT 
    CONCAT('PHONE', c.id) AS Llave,
    'PHONE' AS Producto,
    c.id AS Contrato_x,
    concat_ws(' ', c2.name, c2.name2, c2.last_name, c2.last_name2) AS cliente,
    c2.phone AS telefono,
    c2.email AS correo,
    c2.dni AS cedula,
    c2.departament_reference AS ciudad,
    da.capital_pendiente,
    da.gastos_vencidos,
    da.deuda_actual,
    dsc.Dias_iniciales_Mes,
    CONCAT(ROUND((dsc.factor_capital * 100)::numeric, 0), '%') AS "%_Pago_capital",
    CONCAT(ROUND((dsc.factor_gastos * 100)::numeric, 0), '%') AS "%_Descuento_gastos",
    vfd.valor_final_descuento,

    op.valor_opcion_1,
    op.valor_1_cuota_opcion_2,
    op.valor_2_cuotas_opcion_2,
    op.valor_3_cuotas_opcion_2,
    op.valor_1_cuota_opcion_3,
    op.valor_2_cuotas_opcion_3,
    op.valor_3_cuotas_opcion_3,
    op.valor_1_cuota_opcion_4,
    op.valor_2_cuotas_opcion_4,
    op.valor_3_cuotas_opcion_4,

    COALESCE(ca.cuotas_atrasadas, 0) AS "Cuotas Atrasadas",

    CASE
        WHEN dsc.Dias_iniciales_Mes BETWEEN 1 AND 30 THEN '4%'
        WHEN dsc.Dias_iniciales_Mes BETWEEN 31 AND 60 THEN '4%'
        WHEN dsc.Dias_iniciales_Mes BETWEEN 61 AND 90 THEN '6%'
        WHEN dsc.Dias_iniciales_Mes BETWEEN 91 AND 150 THEN '8%'
        WHEN dsc.Dias_iniciales_Mes BETWEEN 151 AND 210 THEN '11%'
        WHEN dsc.Dias_iniciales_Mes BETWEEN 151 AND 211 THEN '13%'
        WHEN dsc.Dias_iniciales_Mes >= 212 THEN '15%'
        ELSE '0%'
    END AS Comision,

    CASE
        WHEN dsc.Dias_iniciales_Mes BETWEEN 1 AND 30 THEN '1_30'
        WHEN dsc.Dias_iniciales_Mes BETWEEN 31 AND 60 THEN '31_60'
        WHEN dsc.Dias_iniciales_Mes BETWEEN 61 AND 90 THEN '61_90'
        WHEN dsc.Dias_iniciales_Mes BETWEEN 91 AND 150 THEN '91_150'
        WHEN dsc.Dias_iniciales_Mes BETWEEN 151 AND 210 THEN '151_210'
        WHEN dsc.Dias_iniciales_Mes BETWEEN 211 AND 211 THEN '211'
        WHEN dsc.Dias_iniciales_Mes >= 212 THEN 'Cartera Castigada'
        ELSE '0'
    END AS Rango,
    
    'Pagar_1_cuota__para_normalizar' AS Descripcion_opcion_1,
    'Pagar_de_1_a_3_cuotas' AS Descripcion_opcion_2,
    'descuento_1_cta_100%_2ctas<=$600k__3ctas>$600k' AS Descripcion_opcion_3,
    'cap_pendiente_1_cta_100%_2ctas<=$600k__3ctas>$600k' AS Descripcion_opcion_4

FROM contract c
JOIN application a ON a.id = c.application_id
JOIN customer c2 ON c2.id = a.customer_id
LEFT JOIN DeudaActual da ON da.contract_id = c.id
LEFT JOIN Descuentos dsc ON dsc.contract_id = c.id
LEFT JOIN ValorFinalDescuento vfd ON vfd.contract_id = c.id
LEFT JOIN OpcionesPago op ON op.contract_id = c.id
LEFT JOIN CuotasAtrasadas ca ON ca.contract_id = c.id
LEFT JOIN CuotasPagadas cp ON cp.contract_id = c.id
WHERE c.id IN ({lista_contratos})
ORDER BY c.id ASC;
"""
    
    def generate_reports(self) -> Dict[str, str]:
        """
        Genera informes de casa de cobranza para usuarios 81 (SERLEFIN) y 45 (COBYSER)
        
        Returns:
            Dict con las rutas de los archivos generados
        """
        logger.info("=" * 80)
        logger.info("GENERANDO INFORMES DE CASA DE COBRANZA")
        logger.info("=" * 80)
        
        result = {
            'serlefin_file': None,
            'cobyser_file': None,
            'serlefin_contracts': 0,
            'cobyser_contracts': 0
        }
        
        # Obtener contratos asignados
        logger.info("1) Obteniendo contratos asignados...")
        contracts_81 = self._get_assigned_contracts(81)  # SERLEFIN
        contracts_45 = self._get_assigned_contracts(45)  # COBYSER
        
        result['serlefin_contracts'] = len(contracts_81)
        result['cobyser_contracts'] = len(contracts_45)
        
        logger.info(f"  - USER 81 (SERLEFIN): {len(contracts_81)} contratos")
        logger.info(f"  - USER 45 (COBYSER): {len(contracts_45)} contratos")
        
        if not contracts_81 and not contracts_45:
            logger.warning("No hay contratos asignados a los usuarios 81 y 45")
            return result
        
        # Fecha actual para nombres de archivo
        fecha_actual = datetime.now().strftime('%d-%m-%y')
        reports_dir = settings.REPORTS_DIR
        os.makedirs(reports_dir, exist_ok=True)
        
        # GENERAR INFORME USER 81 (SERLEFIN)
        if contracts_81:
            logger.info(f"\n📊 Generando reporte para USER 81 - SERLEFIN ({len(contracts_81)} contratos)...")
            lista_contratos_81 = ",".join(str(x) for x in contracts_81)
            
            conn_prod = psycopg2.connect(
                host=self.DB_CONFIG_PROD['host'],
                user=self.DB_CONFIG_PROD['user'],
                password=self.DB_CONFIG_PROD['password'],
                dbname=self.DB_CONFIG_PROD['database'],
                port=self.DB_CONFIG_PROD['port'],
                options=self.DB_CONFIG_PROD['options']
            )
            
            try:
                query_81 = self._generar_query(lista_contratos_81)
                df_81 = pd.read_sql(query_81, conn_prod)
                
                # Eliminar campos no deseados
                for col in ['cantidad_cuotas_pagados', 'Marca']:
                    if col in df_81.columns:
                        df_81 = df_81.drop(columns=[col])
                
                # Agregar campo NIT al inicio
                df_81.insert(0, 'NIT', '901546410-9')
                
                # Guardar Excel
                file_name_81 = f"AloCredit-Phone-{fecha_actual}  INFORME MARTES Y JUEVES.xlsx"
                file_path_81 = os.path.join(reports_dir, file_name_81)
                df_81.to_excel(file_path_81, index=False)
                
                result['serlefin_file'] = file_path_81
                logger.info(f"✅ INFORME USER 81 (SERLEFIN) GENERADO: {file_name_81}")
                
            finally:
                conn_prod.close()
        
        # GENERAR INFORME USER 45 (COBYSER)
        if contracts_45:
            logger.info(f"\n📊 Generando reporte para USER 45 - COBYSER ({len(contracts_45)} contratos)...")
            lista_contratos_45 = ",".join(str(x) for x in contracts_45)
            
            conn_prod = psycopg2.connect(
                host=self.DB_CONFIG_PROD['host'],
                user=self.DB_CONFIG_PROD['user'],
                password=self.DB_CONFIG_PROD['password'],
                dbname=self.DB_CONFIG_PROD['database'],
                port=self.DB_CONFIG_PROD['port'],
                options=self.DB_CONFIG_PROD['options']
            )
            
            try:
                query_45 = self._generar_query(lista_contratos_45)
                df_45 = pd.read_sql(query_45, conn_prod)
                
                # Eliminar campos no deseados
                for col in ['cantidad_cuotas_pagados', 'Marca']:
                    if col in df_45.columns:
                        df_45 = df_45.drop(columns=[col])
                
                # Cambiar el campo Comision a 30 en todos los registros
                if 'Comision' in df_45.columns:
                    df_45['Comision'] = 30
                
                # Agregar campo NIT al inicio
                df_45.insert(0, 'NIT', '901546410-9')
                
                # Guardar Excel
                file_name_45 = f"AloCredit-Phone-{fecha_actual}  INFORME MARTES Y JUEVES Cobyser.xlsx"
                file_path_45 = os.path.join(reports_dir, file_name_45)
                df_45.to_excel(file_path_45, index=False)
                
                result['cobyser_file'] = file_path_45
                logger.info(f"✅ INFORME USER 45 (COBYSER) GENERADO: {file_name_45}")
                
            finally:
                conn_prod.close()
        
        logger.info("\n🔥 PROCESO COMPLETADO")
        return result
