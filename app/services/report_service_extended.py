"""
Servicio extendido para generación de reportes detallados de asignación.
"""
import psycopg2
import pandas as pd
import math
from datetime import datetime
from pathlib import Path
from typing import Tuple, Dict, List, Optional
import logging
from app.core.config import settings
from app.core.dpd import ASSIGNMENT_DPD_ORDER, get_assignment_dpd_range, get_dpd_range
from app.data.manual_fixed_contracts import MANUAL_FIXED_CONTRACTS

logger = logging.getLogger(__name__)

CUOTAS_ATRASADAS_COL = "Cuotas Atrasadas"


class ReportServiceExtended:
    """Servicio para generación de reportes detallados con información de contratos fijos."""
    
    def __init__(self):
        self.db_config_prod = {
            'host': settings.REPORTS_EXT_PROD_HOST,
            'user': settings.REPORTS_EXT_PROD_USER,
            'password': settings.REPORTS_EXT_PROD_PASSWORD,
            'database': settings.REPORTS_EXT_PROD_DATABASE,
            'port': settings.REPORTS_EXT_PROD_PORT,
            'options': f"-csearch_path={settings.REPORTS_EXT_PROD_SCHEMA}"
        }
        
        self.db_config_ind = {
            'host': settings.REPORTS_EXT_IND_HOST,
            'user': settings.REPORTS_EXT_IND_USER,
            'password': settings.REPORTS_EXT_IND_PASSWORD,
            'database': settings.REPORTS_EXT_IND_DATABASE,
            'port': settings.REPORTS_EXT_IND_PORT,
            'options': f"-csearch_path={settings.REPORTS_EXT_IND_SCHEMA}"
        }
        
        self.reports_dir = Path("reports")
        self.reports_dir.mkdir(exist_ok=True)
    
    def get_assigned_contracts(self, user_id: int) -> List[int]:
        """Obtiene los contratos asignados a un usuario"""
        query = f"SELECT contract_id FROM contract_advisors WHERE user_id = {user_id};"

        try:
            conn = psycopg2.connect(
                host=self.db_config_ind['host'],
                user=self.db_config_ind['user'],
                password=self.db_config_ind['password'],
                dbname=self.db_config_ind['database'],
                port=self.db_config_ind['port'],
                options=self.db_config_ind['options']
            )
            df = pd.read_sql(query, conn)
            conn.close()
            return df['contract_id'].tolist() if not df.empty else []
        except Exception as e:
            logger.error(f"Error obteniendo contratos para user {user_id}: {e}")
            return []

    def get_assigned_contracts_for_house(self, user_ids: List[int]) -> List[int]:
        """Obtiene TODOS los contratos asignados a cualquier usuario de la casa."""
        if not user_ids:
            return []
        users_str = ",".join(str(int(uid)) for uid in user_ids)
        query = f"SELECT DISTINCT contract_id FROM contract_advisors WHERE user_id IN ({users_str});"

        try:
            conn = psycopg2.connect(
                host=self.db_config_ind['host'],
                user=self.db_config_ind['user'],
                password=self.db_config_ind['password'],
                dbname=self.db_config_ind['database'],
                port=self.db_config_ind['port'],
                options=self.db_config_ind['options']
            )
            df = pd.read_sql(query, conn)
            conn.close()
            return df['contract_id'].tolist() if not df.empty else []
        except Exception as e:
            logger.error(f"Error obteniendo contratos para casa {user_ids}: {e}")
            return []
    
    def generate_detailed_query(self, lista_contratos: str) -> str:
        """Genera la consulta SQL detallada para los informes"""
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
LEFT JOIN application a ON a.id = c.application_id
LEFT JOIN customer c2 ON c2.id = a.customer_id
LEFT JOIN DeudaActual da ON da.contract_id = c.id
LEFT JOIN Descuentos dsc ON dsc.contract_id = c.id
LEFT JOIN ValorFinalDescuento vfd ON vfd.contract_id = c.id
LEFT JOIN OpcionesPago op ON op.contract_id = c.id
LEFT JOIN CuotasAtrasadas ca ON ca.contract_id = c.id
LEFT JOIN CuotasPagadas cp ON cp.contract_id = c.id
WHERE c.id IN ({lista_contratos})
ORDER BY c.id ASC;
"""
    
    def generate_report_for_user(
        self,
        user_id: int,
        user_name: str,
        contracts: List[int],
        days_overdue_map: Optional[Dict[int, int]] = None,
    ) -> Tuple[str, pd.DataFrame]:
        """
        Genera reporte detallado para un usuario específico
        
        Returns:
            Tuple[str, pd.DataFrame]: (ruta_archivo, dataframe)
        """
        if not contracts:
            logger.warning(f"No hay contratos para user {user_id}")
            return None, None

        try:
            logger.info(f"📊 Generando reporte para {user_name} ({len(contracts)} contratos)...")
            
            # Fuente unica: MySQL en vivo. PG-prod quedo congelado en 2025-12 y
            # ademas no contiene ~35% de los contratos asignados, lo que producia
            # valores stale + un fallback con otra formula de capital. Ahora TODOS
            # los contratos asignados se construyen desde MySQL con una sola formula
            # de capital, y dias/cuotas/comision/%/descuentos/opciones se derivan de
            # los MISMOS dias operativos (la fila ya no se contradice).
            if days_overdue_map is None:
                days_overdue_map = self._load_operational_days_overdue(contracts)
            overdue_installments_map = self._load_operational_overdue_installments(contracts)

            df = self._build_report_dataframe_mysql(
                contracts, days_overdue_map, overdue_installments_map,
            )
            cols_by_lower = {str(col).lower(): col for col in df.columns}

            df = self._finalize_phone_df(df, cols_by_lower, user_id)

            # Generar nombre de archivo
            fecha_actual = datetime.now().strftime('%d-%m-%y')
            if user_id == 81:
                casa = "Serlefin"
            elif user_id == 45:
                casa = "Cobyser"
            else:
                casa = f"User{user_id}"
            file_name = f"AloCredit-Phone-{fecha_actual}_INFORME_{casa}.xlsx"
            file_path = self.reports_dir / file_name

            df_twist1, df_twist2 = self._build_twist_sheets(user_id)

            # Regla vigente: la franja 31-60 se reparte por PARIDAD de cédula
            # (impar -> Cobyser, par -> Serlefin), por lo que Serlefin SÍ reporta
            # sus contratos 31-60 (cédulas pares). Ya no se descartan esas filas.

            # Guardar Excel con UNA HOJA POR PRODUCTO
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Phone', index=False)
                df_twist1.to_excel(writer, sheet_name='Twist1', index=False)
                df_twist2.to_excel(writer, sheet_name='Twist2', index=False)
            logger.info(
                "✅ INFORME GENERADO: %s (Phone=%s, Twist1=%s, Twist2=%s)",
                file_path, len(df), len(df_twist1), len(df_twist2),
            )

            return str(file_path), df
            
        except Exception as e:
            logger.error(f"❌ Error generando reporte para user {user_id}: {e}")
            return None, None

    def _finalize_phone_df(
        self, df: pd.DataFrame, cols_by_lower: Dict[str, str], user_id: int,
    ) -> pd.DataFrame:
        """Post-procesa la hoja Phone: drops, Contrato_Fijo, comision Cobyser,
        columna Tipo y NIT. Comportamiento identico al original."""
        # Eliminar campos innecesarios
        for col in ['cantidad_cuotas_pagados', 'Marca']:
            if col in df.columns:
                df = df.drop(columns=[col])

        # Agregar campo "Contrato Fijo"
        self._apply_contrato_fijo(df, cols_by_lower, user_id)

        # Ajustar comision para Cobyser (Usuario 45)
        if user_id == 45:
            comision_col = cols_by_lower.get('comision')
            if comision_col:
                df[comision_col] = '30%'

        # Columna "Tipo": etiqueta "Cedulas Impar" para la franja Cobyser
        # (dias 31-60). Solo Cobyser (45) en los buckets 31_45/46_60
        # (el informe expresa la franja como rango '31_60'). Vacia en el resto.
        self._apply_tipo_franja(df, cols_by_lower, user_id)

        # Agregar campo NIT al inicio
        df.insert(0, 'NIT', '901546410-9')
        return df

    @staticmethod
    def _apply_contrato_fijo(
        df: pd.DataFrame, cols_by_lower: Dict[str, str], user_id: int,
    ) -> None:
        """Marca Contrato_Fijo (SI/NO) segun los contratos fijos manuales del user."""
        manual_fixed = MANUAL_FIXED_CONTRACTS.get(user_id, [])
        contrato_col = cols_by_lower.get('contrato_x')
        if contrato_col:
            df['Contrato_Fijo'] = df[contrato_col].apply(
                lambda x: 'SI' if x in manual_fixed else 'NO'
            )
        else:
            logger.warning(
                "No se encontro columna de contrato para user %s. Se marcara Contrato_Fijo='NO'.",
                user_id,
            )
            df['Contrato_Fijo'] = 'NO'

    @staticmethod
    def _apply_tipo_franja(
        df: pd.DataFrame, cols_by_lower: Dict[str, str], user_id: int,
    ) -> None:
        """
        Etiqueta la columna Tipo en la franja 31-60 según la casa:
        Cobyser (45) -> 'Cédulas Impar', Serlefin (81) -> 'Cédulas Par'.
        """
        df['Tipo'] = ''
        rango_col = cols_by_lower.get('rango')
        if rango_col and rango_col in df.columns:
            es_franja = df[rango_col].astype(str).isin(['31_60', '31_45', '46_60'])
            if user_id == 45:
                df.loc[es_franja, 'Tipo'] = 'Cédulas Impar'
            elif user_id == 81:
                df.loc[es_franja, 'Tipo'] = 'Cédulas Par'

    def _finalize_twist_sheet(self, tdf: pd.DataFrame, user_id: int) -> pd.DataFrame:
        """Finaliza una hoja Twist (Contrato_Fijo, Tipo, NIT)."""
        if tdf is None or tdf.empty:
            return pd.DataFrame(columns=['NIT'] + self.REPORT_BASE_COLUMNS + ['Contrato_Fijo', 'Tipo'])
        tdf = tdf.copy()
        tdf['Contrato_Fijo'] = 'NO'
        tdf['Tipo'] = ''
        if 'rango' in tdf.columns:
            es_franja = tdf['rango'].astype(str).isin(['31_60', '31_45', '46_60'])
            if user_id == 45:
                tdf.loc[es_franja, 'Tipo'] = 'Cédulas Impar'
            elif user_id == 81:
                tdf.loc[es_franja, 'Tipo'] = 'Cédulas Par'
        tdf.insert(0, 'NIT', '901546410-9')
        return tdf

    def _build_twist_sheets(self, user_id: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Construye las hojas Twist1 / Twist2. Aislado: un fallo en Twist no
        rompe la hoja Phone."""
        df_twist1 = pd.DataFrame()
        df_twist2 = pd.DataFrame()
        try:
            twist1_ids = self.get_assigned_twist1_for_house([user_id])
            df_twist1 = self._finalize_twist_sheet(
                self._build_twist1_report_dataframe(twist1_ids), user_id
            )
            twist2_rows = self.get_assigned_twist2_for_house([user_id])
            df_twist2 = self._finalize_twist_sheet(
                self._build_twist2_report_dataframe(twist2_rows), user_id
            )
        except Exception as twist_err:
            logger.error("Error construyendo hojas Twist para user %s: %s", user_id, twist_err)
        return df_twist1, df_twist2

    @staticmethod
    def _drop_serlefin_franja(
        df: pd.DataFrame, df_twist1: pd.DataFrame, df_twist2: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Excluye la franja 31-60 (exclusiva de Cobyser) de las 3 hojas."""
        FRANJA = ('31_60', '31_45', '46_60')

        def _drop_franja(frame):
            if frame is None or frame.empty or 'rango' not in frame.columns:
                return frame
            return frame[~frame['rango'].astype(str).isin(FRANJA)].copy()

        return _drop_franja(df), _drop_franja(df_twist1), _drop_franja(df_twist2)

    # Columnas base del informe (sin NIT al frente ni Contrato_Fijo al final),
    # en el MISMO orden/nombre que el informe historico para no romper consumidores.
    REPORT_BASE_COLUMNS = [
        "llave", "producto", "contrato_x", "cliente", "telefono", "correo",
        "cedula", "ciudad", "capital_pendiente", "gastos_vencidos", "deuda_actual",
        "dias_iniciales_mes", "%_Pago_capital", "%_Descuento_gastos",
        "valor_final_descuento", "valor_opcion_1",
        "valor_1_cuota_opcion_2", "valor_2_cuotas_opcion_2", "valor_3_cuotas_opcion_2",
        "valor_1_cuota_opcion_3", "valor_2_cuotas_opcion_3", "valor_3_cuotas_opcion_3",
        "valor_1_cuota_opcion_4", "valor_2_cuotas_opcion_4", "valor_3_cuotas_opcion_4",
        CUOTAS_ATRASADAS_COL, "comision", "rango",
        "descripcion_opcion_1", "descripcion_opcion_2",
        "descripcion_opcion_3", "descripcion_opcion_4",
    ]

    @staticmethod
    def _comision_por_dias(dias: int) -> str:
        """Comision por dias de mora. Rangos contiguos y sin solapamiento
        (corrige el CASE original donde 151-210 y 151-211 se pisaban)."""
        if dias <= 0:
            return '0%'
        if dias <= 60:
            return '4%'
        if dias <= 90:
            return '6%'
        if dias <= 150:
            return '8%'
        if dias <= 210:
            return '11%'
        if dias <= 240:
            return '13%'
        return '15%'

    @staticmethod
    def _discount_factors(dias: int):
        """Factores de descuento por dias (regla de negocio, igual para los 3 productos)."""
        if dias <= 150:
            factor_capital = 1.0
        elif dias <= 180:
            factor_capital = 0.95
        elif dias <= 300:
            factor_capital = 0.90
        else:
            factor_capital = 0.75
        if dias <= 90:
            factor_gastos = 0.70
        elif dias <= 120:
            factor_gastos = 0.60
        elif dias <= 150:
            factor_gastos = 0.50
        elif dias <= 365:
            factor_gastos = 0.40
        else:
            factor_gastos = 0.0
        return factor_capital, factor_gastos

    def _financial_report_row(
        self, *, producto: str, llave: str, contrato_x,
        capital, gastos, dias: int, cuotas: int, quota,
        cliente=None, telefono=None, correo=None, cedula=None, ciudad=None,
    ) -> dict:
        """
        Construye una fila del informe con la MISMA logica financiera para
        Phone / Twist1 / Twist2 (descuento por dias + 4 opciones de pago).
        Solo cambian las fuentes de capital/gastos/cliente segun el producto.
        """
        capital = float(capital or 0)
        if capital < 0:
            capital = 0.0
        gastos = float(gastos or 0)
        deuda = capital + gastos
        factor_capital, factor_gastos = self._discount_factors(dias)
        vfd = round(capital * factor_capital + gastos * factor_gastos)
        return {
            "llave": llave,
            "producto": producto,
            "contrato_x": contrato_x,
            "cliente": cliente,
            "telefono": telefono,
            "correo": correo,
            "cedula": cedula,
            "ciudad": ciudad,
            "capital_pendiente": capital,
            "gastos_vencidos": gastos,
            "deuda_actual": deuda,
            "dias_iniciales_mes": dias,
            "%_Pago_capital": f"{int(factor_capital * 100)}%",
            "%_Descuento_gastos": f"{int(factor_gastos * 100)}%",
            "valor_final_descuento": vfd,
            "valor_opcion_1": quota,
            "valor_1_cuota_opcion_2": deuda,
            "valor_2_cuotas_opcion_2": round(deuda / 2) if deuda else 0,
            "valor_3_cuotas_opcion_2": round(deuda / 3) if deuda > 600000 else None,
            "valor_1_cuota_opcion_3": vfd,
            "valor_2_cuotas_opcion_3": round(vfd / 2) if vfd else 0,
            "valor_3_cuotas_opcion_3": round(vfd / 3) if vfd > 600000 else None,
            "valor_1_cuota_opcion_4": capital,
            "valor_2_cuotas_opcion_4": round(capital / 2) if capital else 0,
            "valor_3_cuotas_opcion_4": round(capital / 3) if capital > 600000 else None,
            CUOTAS_ATRASADAS_COL: cuotas,
            "comision": self._comision_por_dias(dias),
            "rango": get_assignment_dpd_range(dias) or get_dpd_range(dias) or '0',
            "descripcion_opcion_1": "Pagar_1_cuota__para_normalizar",
            "descripcion_opcion_2": "Pagar_de_1_a_3_cuotas",
            "descripcion_opcion_3": "descuento_1_cta_100%_2ctas<=$600k__3ctas>$600k",
            "descripcion_opcion_4": "cap_pendiente_1_cta_100%_2ctas<=$600k__3ctas>$600k",
        }

    @staticmethod
    def _pagare_exclude_sql(column: str) -> str:
        """Fragmento SQL para excluir contratos endosados a afianzadora (pagaré)."""
        ids = settings.pagare_excluded_status_id_list
        if not ids:
            return ""
        return f" AND ({column} IS NULL OR {column} NOT IN ({','.join(str(i) for i in ids)}))"

    def _fetch_phone_base(self, contracts: List[int]) -> Dict:
        """Carga (por lotes, desde MySQL) los datos base Phone por contract_id."""
        from app.database.connections import db_manager
        from sqlalchemy import text

        base = {}
        batch_size = 1000
        with db_manager.get_mysql_session() as mysql_session:
            for i in range(0, len(contracts), batch_size):
                batch = contracts[i:i + batch_size]
                batch_str = ",".join(str(int(c)) for c in batch)
                query = text(f"""
                    SELECT
                        c.id AS contract_id,
                        CONCAT_WS(' ', cu.name, cu.name2, cu.last_name, cu.last_name2) AS cliente,
                        cu.phone AS telefono,
                        cu.email AS correo,
                        cu.dni AS cedula,
                        cu.departament_reference AS ciudad,
                        COALESCE(
                            (SELECT cap.outstanding_principal
                             FROM contract_amortization cap
                             WHERE cap.contract_id = c.id
                               AND cap.contract_amortization_payment_status_id IN (1,5)
                             ORDER BY cap.period_number DESC
                             LIMIT 1),
                            (COALESCE(al.device_price,0) - COALESCE(al.initial_pay,0) + COALESCE(acc.total_acc,0))
                        ) AS capital_pendiente,
                        COALESCE((
                            SELECT SUM(
                                COALESCE(g.interest_payment,0) + COALESCE(g.endorsement,0) +
                                COALESCE(g.vat,0) + COALESCE(g.seguro_vida,0) +
                                COALESCE(g.seguro,0) + COALESCE(g.digital_sign,0) +
                                COALESCE(g.digital_sign_iva,0))
                            FROM contract_amortization g
                            WHERE g.contract_id = c.id
                              AND g.contract_amortization_payment_status_id = 4
                              AND g.expiration_date <= CURDATE()
                              AND g.outstanding_principal > 0
                        ), 0) AS gastos_vencidos,
                        al.quota AS quota
                    FROM contract c
                    JOIN application a ON a.id = c.application_id
                    JOIN customer cu ON cu.id = a.customer_id
                    LEFT JOIN (
                        SELECT application_id, MAX(id) AS max_loan_id
                        FROM application_loan GROUP BY application_id
                    ) mx ON mx.application_id = a.id
                    LEFT JOIN application_loan al ON al.id = mx.max_loan_id
                    LEFT JOIN (
                        SELECT application_id, SUM(price) AS total_acc
                        FROM application_accessory GROUP BY application_id
                    ) acc ON acc.application_id = a.id
                    WHERE c.id IN ({batch_str}){self._pagare_exclude_sql('c.pagare_status_id')}
                """)
                for row in mysql_session.execute(query):
                    m = row._mapping
                    base[int(m['contract_id'])] = m
        return base

    def _phone_row_from_base(
        self,
        raw_cid,
        base: Dict,
        days_overdue_map: Dict[int, int],
        overdue_installments_map: Dict[int, int],
    ) -> dict:
        """Construye una fila Phone a partir del mapa base + dias/cuotas operativos."""
        cid = int(raw_cid)
        m = base.get(cid)
        dias = int(days_overdue_map.get(cid, 0) or 0)
        cuotas = int(overdue_installments_map.get(cid, 0) or 0)

        if m is not None:
            capital = float(m['capital_pendiente'] or 0)
            gastos = float(m['gastos_vencidos'] or 0)
            quota = float(m['quota']) if m['quota'] is not None else None
            cliente, telefono, correo = m['cliente'], m['telefono'], m['correo']
            cedula, ciudad = m['cedula'], m['ciudad']
        else:
            capital = gastos = 0.0
            quota = None
            cliente = telefono = correo = cedula = ciudad = None

        return self._financial_report_row(
            producto="PHONE",
            llave=f"PHONE{cid}",
            contrato_x=cid,
            capital=capital,
            gastos=gastos,
            dias=dias,
            cuotas=cuotas,
            quota=quota,
            cliente=cliente,
            telefono=telefono,
            correo=correo,
            cedula=cedula,
            ciudad=ciudad,
        )

    def _build_report_dataframe_mysql(
        self,
        contracts: List[int],
        days_overdue_map: Dict[int, int],
        overdue_installments_map: Dict[int, int],
    ) -> pd.DataFrame:
        """
        Construye el informe COMPLETO desde MySQL (fuente viva) para TODOS los
        contratos asignados, con una sola definicion de cada campo:

        - capital_pendiente = outstanding_principal de la ULTIMA cuota pagada
          (status 1,5, mayor period_number). Si el contrato no tiene cuotas
          pagadas: device_price - initial_pay + accesorios.
        - gastos_vencidos = suma de gastos de las cuotas vencidas operativas
          (status 4, expiration_date <= hoy, outstanding_principal > 0).
        - deuda_actual = capital_pendiente + gastos_vencidos.
        - dias_iniciales_mes / Cuotas Atrasadas = mapas operativos (misma logica
          de asignacion).
        - %_Pago_capital, %_Descuento_gastos, valor_final_descuento, comision,
          rango y todas las opciones de pago se derivan de los MISMOS dias
          operativos, de modo que la fila es internamente consistente.
        """
        base = self._fetch_phone_base(contracts)

        rows = [
            self._phone_row_from_base(
                raw_cid, base, days_overdue_map, overdue_installments_map,
            )
            for raw_cid in contracts
        ]

        # dias_iniciales_mes = dia inicial guardado en contract_advisors_history
        hist = self._history_initial_days([r['contrato_x'] for r in rows], 'PHONE')
        for r in rows:
            cid = r.get('contrato_x')
            if isinstance(cid, int) and cid in hist:
                r['dias_iniciales_mes'] = hist[cid]
        return pd.DataFrame(rows, columns=self.REPORT_BASE_COLUMNS)

    # ==================================================================
    # Twist 1.0 / Twist 2.0 -> mismas columnas/formulas que Phone.
    # ==================================================================
    def _query_ind(self, query: str):
        """Ejecuta un SELECT en la base de indicadores (contract_advisors*)."""
        conn = psycopg2.connect(
            host=self.db_config_ind['host'], user=self.db_config_ind['user'],
            password=self.db_config_ind['password'], dbname=self.db_config_ind['database'],
            port=self.db_config_ind['port'], options=self.db_config_ind['options'],
        )
        try:
            df = pd.read_sql(query, conn)
        finally:
            conn.close()
        return df

    def _history_initial_days(self, contract_ids, producto: str) -> dict:
        """
        Dia inicial (dias_atraso_inicial) guardado en contract_advisors_history,
        por producto. Se usa para la columna dias_iniciales_mes del informe.
        """
        out = {}
        ids = [int(c) for c in contract_ids if c is not None]
        for i in range(0, len(ids), 2000):
            ins = ",".join(str(x) for x in ids[i:i + 2000])
            try:
                df = self._query_ind(
                    f"SELECT contract_id, dias_atraso_incial "
                    f"FROM alocreditindicators.contract_advisors_history "
                    f"WHERE producto = '{producto}' AND \"Fecha Terminal\" IS NULL "
                    f"AND contract_id IN ({ins})"
                )
            except Exception as e:
                logger.warning("No se pudo leer dia inicial de history (%s): %s", producto, e)
                continue
            for _, r in df.iterrows():
                v = r['dias_atraso_incial']
                if pd.notna(v):
                    out[int(r['contract_id'])] = int(v)
        return out

    def get_assigned_twist1_for_house(self, user_ids: List[int]) -> List[int]:
        if not user_ids:
            return []
        users_str = ",".join(str(int(u)) for u in user_ids)
        try:
            df = self._query_ind(
                f"SELECT DISTINCT contract_id FROM alocreditindicators.contract_advisors_twist "
                f"WHERE user_id IN ({users_str});"
            )
            return df['contract_id'].tolist() if not df.empty else []
        except Exception as e:
            logger.error("Error obteniendo Twist1 de casa %s: %s", user_ids, e)
            return []

    def get_assigned_twist2_for_house(self, user_ids: List[int]) -> List[dict]:
        if not user_ids:
            return []
        users_str = ",".join(str(int(u)) for u in user_ids)
        try:
            df = self._query_ind(
                f"SELECT line_id, cbs_id, cedula, days_overdue "
                f"FROM alocreditindicators.contract_advisors_twist2 WHERE user_id IN ({users_str});"
            )
            return df.to_dict("records") if not df.empty else []
        except Exception as e:
            logger.error("Error obteniendo Twist2 de casa %s: %s", user_ids, e)
            return []

    def _fetch_twist1_base(self, contract_ids: List[int]) -> Dict:
        """Carga (por lotes, desde MySQL twist_) los datos base Twist1 por contract_id."""
        from app.database.connections import db_manager
        from sqlalchemy import text

        base = {}
        batch_size = 1000
        with db_manager.get_mysql_session() as mysql_session:
            for i in range(0, len(contract_ids), batch_size):
                batch = contract_ids[i:i + batch_size]
                batch_str = ",".join(str(int(c)) for c in batch)
                query = text(f"""
                    SELECT
                        c.id AS contract_id,
                        CONCAT_WS(' ', cu.name, cu.name2, cu.last_name, cu.last_name2) AS cliente,
                        cu.phone AS telefono, cu.email AS correo, cu.dni AS cedula,
                        cu.departament_reference AS ciudad,
                        COALESCE((SELECT cap.outstanding_principal FROM twist_contract_amortization cap
                                  WHERE cap.twist_contract_id = c.id
                                    AND cap.twist_contract_payment_status_id IN (1, 4)
                                  ORDER BY cap.period_number DESC LIMIT 1),
                                 -- Sin cuotas pagadas: cae al credito original (precio del prestamo Twist).
                                 (SELECT tal.price FROM twist_application_loan tal
                                  WHERE tal.twist_application_id = a.id
                                  ORDER BY tal.id DESC LIMIT 1),
                                 0) AS capital_pendiente,
                        -- gastos_vencidos = cuota COMPLETA (total_fee) de TODAS las cuotas
                        -- Atrasadas (status 3) por estado, incluida la ultima cuota aunque
                        -- su outstanding_principal sea 0 (Twist 1.0, definicion PDS wallet).
                        COALESCE((SELECT SUM(COALESCE(g.total_fee,0))
                                  FROM twist_contract_amortization g
                                  WHERE g.twist_contract_id = c.id
                                    AND g.twist_contract_payment_status_id = 3
                                    AND g.expiration_date <= CURDATE()), 0) AS gastos_vencidos,
                        COALESCE((SELECT DATEDIFF(CURDATE(), MIN(d.expiration_date))
                                  FROM twist_contract_amortization d
                                  WHERE d.twist_contract_id = c.id
                                    AND d.twist_contract_payment_status_id = 3
                                    AND d.expiration_date < CURDATE()
                                    AND d.outstanding_principal > 0), 0) AS dias,
                        COALESCE((SELECT COUNT(*) FROM twist_contract_amortization q
                                  WHERE q.twist_contract_id = c.id
                                    AND q.twist_contract_payment_status_id = 3
                                    AND q.expiration_date <= CURDATE()
                                    AND q.outstanding_principal > 0), 0) AS cuotas
                    FROM twist_contract c
                    JOIN twist_application a ON a.id = c.twist_application_id
                    JOIN customer cu ON cu.id = a.customer_id
                    WHERE c.id IN ({batch_str}){self._pagare_exclude_sql('c.twist_pagare_status_id')}
                """)
                for row in mysql_session.execute(query):
                    m = row._mapping
                    base[int(m['contract_id'])] = m
        return base

    def _build_twist1_report_dataframe(self, contract_ids: List[int]) -> pd.DataFrame:
        """Informe Twist1 (MySQL twist_) con la MISMA logica financiera que Phone."""
        if not contract_ids:
            return pd.DataFrame(columns=self.REPORT_BASE_COLUMNS)

        base = self._fetch_twist1_base(contract_ids)

        rows = []
        for cid in contract_ids:
            cid = int(cid)
            m = base.get(cid)
            if m is None:
                continue
            dias = int(m['dias'] or 0)
            # Solo se reporta mora valida (>=31). Excluye al-dia (rango 0) y 1-30
            # que pueden quedar por herencia del proceso externo o deriva append-only.
            if dias < settings.FRANJA_COBYSER_MIN_DAYS:
                continue
            rows.append(self._financial_report_row(
                producto="TWIST1", llave=f"TWIST1{cid}", contrato_x=cid,
                capital=m['capital_pendiente'], gastos=m['gastos_vencidos'],
                dias=dias, cuotas=int(m['cuotas'] or 0), quota=None,
                cliente=m['cliente'], telefono=m['telefono'], correo=m['correo'],
                cedula=m['cedula'], ciudad=m['ciudad'],
            ))
        # dias_iniciales_mes = dia inicial guardado en history (producto TWIST1)
        hist = self._history_initial_days([r['contrato_x'] for r in rows], 'TWIST1')
        for r in rows:
            cid = r.get('contrato_x')
            if isinstance(cid, int) and cid in hist:
                r['dias_iniciales_mes'] = hist[cid]
        return pd.DataFrame(rows, columns=self.REPORT_BASE_COLUMNS)

    def _build_twist2_report_dataframe(self, assigned_rows: List[dict]) -> pd.DataFrame:
        """Informe Twist2 (CBS line_balance + PDS clients) con la MISMA logica que Phone."""
        if not assigned_rows:
            return pd.DataFrame(columns=self.REPORT_BASE_COLUMNS)

        cbs_ids = [int(r['cbs_id']) for r in assigned_rows if r.get('cbs_id') is not None]
        line_ids = [str(r['line_id']) for r in assigned_rows if r.get('line_id')]

        balance_map = self._fetch_twist2_balances(cbs_ids)
        client_map = self._fetch_twist2_clients(line_ids)

        rows = []
        for r in assigned_rows:
            cbs_id = int(r['cbs_id']) if r.get('cbs_id') is not None else None
            dias = int(r.get('days_overdue') or 0)
            # Solo mora valida (>=31): excluye al-dia (rango 0) y 1-30.
            if dias < settings.FRANJA_COBYSER_MIN_DAYS:
                continue
            rows.append(self._build_twist2_row(r, cbs_id, dias, balance_map, client_map))
        return pd.DataFrame(rows, columns=self.REPORT_BASE_COLUMNS)

    @staticmethod
    def _fetch_twist2_balances(cbs_ids: List[int]) -> Dict:
        """Saldos CBS (line_balance) por id_credit_line para Twist2."""
        balance_map = {}
        if cbs_ids:
            cbs_conn = psycopg2.connect(
                host=settings.CBS_DB_HOST, port=settings.CBS_DB_PORT, user=settings.CBS_DB_USER,
                password=settings.CBS_DB_PASSWORD, dbname=settings.CBS_DB_NAME,
                connect_timeout=settings.CBS_DB_CONNECT_TIMEOUT,
            )
            try:
                cur = cbs_conn.cursor()
                cur.execute(
                    """
                    SELECT DISTINCT ON (id_credit_line)
                        id_credit_line, principal_pending, accrued_interest,
                        accrued_fee, accrued_arrear
                    FROM line_balance
                    WHERE id_credit_line = ANY(%s)
                    ORDER BY id_credit_line, as_of DESC
                    """,
                    (cbs_ids,),
                )
                for r in cur.fetchall():
                    balance_map[int(r[0])] = r
                cur.close()
            finally:
                cbs_conn.close()
        return balance_map

    @staticmethod
    def _fetch_twist2_clients(line_ids: List[str]) -> Dict:
        """Datos de cliente PDS (credit_lines + clients) por line_id para Twist2."""
        client_map = {}
        if line_ids:
            pds_conn = psycopg2.connect(
                host=settings.PDS_DB_HOST, port=settings.PDS_DB_PORT, user=settings.PDS_DB_USER,
                password=settings.PDS_DB_PASSWORD, dbname=settings.PDS_DB_NAME,
                connect_timeout=settings.PDS_DB_CONNECT_TIMEOUT,
            )
            try:
                cur = pds_conn.cursor()
                cur.execute(
                    """
                    SELECT cl.id::text, c.full_name, c.phone_number, c.email, c.city, cl.external_id
                    FROM credit_lines cl JOIN clients c ON c.id = cl.client_id
                    WHERE cl.id::text = ANY(%s)
                    """,
                    (line_ids,),
                )
                for r in cur.fetchall():
                    client_map[str(r[0])] = r
                cur.close()
            finally:
                pds_conn.close()
        return client_map

    def _build_twist2_row(
        self, r: dict, cbs_id, dias: int, balance_map: Dict, client_map: Dict,
    ) -> dict:
        """Construye una fila del informe Twist2 (CBS balance + PDS client)."""
        line_id = str(r.get('line_id'))
        bal = balance_map.get(cbs_id) if cbs_id is not None else None
        capital, gastos = self._twist2_balance_values(bal)
        cli = client_map.get(line_id)
        cliente = cli[1] if cli else None
        telefono = cli[2] if cli else None
        correo = cli[3] if cli else None
        ciudad = cli[4] if cli else None
        # El contrato de Twist2 es el external_id (numero), no el UUID de la linea.
        external = self._twist2_external_id(cli, cbs_id)
        return self._financial_report_row(
            producto="TWIST2", llave=f"TWIST2_{external}", contrato_x=external,
            capital=capital, gastos=gastos, dias=dias, cuotas=0, quota=None,
            cliente=cliente, telefono=telefono, correo=correo,
            cedula=r.get('cedula'), ciudad=ciudad,
        )

    @staticmethod
    def _twist2_balance_values(bal):
        """Capital y gastos a partir de la fila de saldo CBS (o ceros si falta)."""
        capital = float(bal[1] or 0) if bal else 0.0
        gastos = (float(bal[2] or 0) + float(bal[3] or 0) + float(bal[4] or 0)) if bal else 0.0
        return capital, gastos

    @staticmethod
    def _twist2_external_id(cli, cbs_id):
        """external_id numerico del cliente PDS; cae a cbs_id si no es numerico."""
        ext_raw = cli[5] if cli else None
        if ext_raw is not None and str(ext_raw).strip().isdigit():
            return int(str(ext_raw).strip())
        return cbs_id

    @staticmethod
    def _safe_int(value) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _missing_comision(dias: int) -> str:
        """Comision (logica historica del fallback MySQL de contratos faltantes)."""
        if 1 <= dias <= 60:
            return '4%'
        elif 61 <= dias <= 90:
            return '6%'
        elif 91 <= dias <= 150:
            return '8%'
        elif 151 <= dias <= 210:
            return '11%'
        elif dias == 211:
            return '13%'
        elif dias >= 212:
            return '15%'
        else:
            return '0%'

    @staticmethod
    def _missing_rango(dias: int) -> str:
        """Rango (logica historica del fallback MySQL de contratos faltantes)."""
        if 1 <= dias <= 30:
            return '1_30'
        elif 31 <= dias <= 60:
            return '31_60'
        elif 61 <= dias <= 90:
            return '61_90'
        elif 91 <= dias <= 150:
            return '91_150'
        elif 151 <= dias <= 210:
            return '151_210'
        elif dias == 211:
            return '211'
        elif dias >= 212:
            return 'Cartera Castigada'
        else:
            return '0'

    def _build_missing_contract_row(
        self,
        row,
        target_columns: List[str],
        cols_by_lower: Dict[str, str],
        base_cols: Dict[str, str],
    ) -> Dict:
        """
        Construye una fila del DataFrame de contratos faltantes (fallback MySQL)
        con la MISMA logica historica de factores/comision/rango/opciones.
        """
        (
            contract_id, llave, producto, cliente, telefono,
            correo, cedula, ciudad, capital, gastos, deuda,
            dias, cuotas, quota,
        ) = row

        capital = float(capital or 0)
        gastos = float(gastos or 0)
        deuda = float(deuda or 0)
        dias = int(dias) if dias is not None else 0
        cuotas = int(cuotas) if cuotas is not None else 0
        quota = float(quota) if quota is not None else None

        # Calcular factores de descuento (misma logica que PG)
        factor_capital, factor_gastos = self._discount_factors(dias)
        valor_final_descuento = round(capital * factor_capital + gastos * factor_gastos)
        comision = self._missing_comision(dias)
        rango = self._missing_rango(dias)

        r = dict.fromkeys(target_columns, None)
        r[base_cols['contrato']] = contract_id
        r[base_cols['llave']] = llave
        r[base_cols['producto']] = producto
        r[base_cols['cliente']] = cliente
        r[base_cols['telefono']] = telefono
        r[base_cols['correo']] = correo
        r[base_cols['cedula']] = cedula
        r[base_cols['ciudad']] = ciudad
        r[base_cols['capital']] = capital
        r[base_cols['gastos']] = gastos
        r[base_cols['deuda']] = deuda
        r[base_cols['dias']] = dias
        r[base_cols['cuotas']] = cuotas

        # Campos calculados y opciones de pago: misma asignacion condicionada
        # por columna disponible, expresada como (clave_lower -> valor).
        computed = self._missing_computed_values(
            capital, gastos, deuda, quota, valor_final_descuento,
            factor_capital, factor_gastos, comision, rango,
        )
        for key_lower, value in computed.items():
            col = cols_by_lower.get(key_lower)
            if col:
                r[col] = value

        return r

    @staticmethod
    def _missing_computed_values(
        capital, gastos, deuda, quota, valor_final_descuento,
        factor_capital, factor_gastos, comision, rango,
    ) -> Dict:
        """Valores calculados (clave_lower -> valor) del fallback MySQL."""
        return {
            '%_pago_capital': f"{int(factor_capital * 100)}%",
            '%_descuento_gastos': f"{int(factor_gastos * 100)}%",
            'valor_final_descuento': valor_final_descuento,
            'valor_opcion_1': quota,
            'valor_1_cuota_opcion_2': deuda,
            'valor_2_cuotas_opcion_2': round(deuda / 2) if deuda else 0,
            'valor_3_cuotas_opcion_2': round(deuda / 3) if deuda > 600000 else None,
            'valor_1_cuota_opcion_3': valor_final_descuento,
            'valor_2_cuotas_opcion_3': round(valor_final_descuento / 2) if valor_final_descuento else 0,
            'valor_3_cuotas_opcion_3': round(valor_final_descuento / 3) if valor_final_descuento > 600000 else None,
            'valor_1_cuota_opcion_4': capital,
            'valor_2_cuotas_opcion_4': round(capital / 2) if capital else 0,
            'valor_3_cuotas_opcion_4': round(capital / 3) if capital > 600000 else None,
            'comision': comision,
            'rango': rango,
            'descripcion_opcion_1': 'Pagar_1_cuota__para_normalizar',
            'descripcion_opcion_2': 'Pagar_de_1_a_3_cuotas',
            'descripcion_opcion_3': 'descuento_1_cta_100%_2ctas<=$600k__3ctas>$600k',
            'descripcion_opcion_4': 'cap_pendiente_1_cta_100%_2ctas<=$600k__3ctas>$600k',
        }

    def _fetch_missing_contracts_from_mysql(
        self,
        missing_ids: List[int],
        target_columns: List[str],
        cols_by_lower: Dict[str, str],
    ) -> Optional[pd.DataFrame]:
        """
        Consulta MySQL para obtener datos de contratos que no existen en PG produccion.
        Retorna un DataFrame con las mismas columnas que el reporte principal.
        """
        if not missing_ids:
            return None

        try:
            from app.database.connections import db_manager
            from sqlalchemy import text

            batch_size = 1000
            all_rows = []

            with db_manager.get_mysql_session() as mysql_session:
                for i in range(0, len(missing_ids), batch_size):
                    batch = missing_ids[i : i + batch_size]
                    batch_str = ",".join(str(int(cid)) for cid in batch)

                    query = text(f"""
                        SELECT
                            c.id AS contract_id,
                            CONCAT('PHONE', c.id) AS llave,
                            'PHONE' AS producto,
                            CONCAT_WS(' ', cu.name, cu.name2, cu.last_name, cu.last_name2) AS cliente,
                            cu.phone AS telefono,
                            cu.email AS correo,
                            cu.dni AS cedula,
                            cu.departament_reference AS ciudad,
                            COALESCE(SUM(ca.outstanding_principal), 0) AS capital_pendiente,
                            SUM(
                                COALESCE(ca.interest_payment,0) +
                                COALESCE(ca.endorsement,0) +
                                COALESCE(ca.vat,0) +
                                COALESCE(ca.seguro_vida,0) +
                                COALESCE(ca.seguro,0) +
                                COALESCE(ca.digital_sign,0) +
                                COALESCE(ca.digital_sign_iva,0)
                            ) AS gastos_vencidos,
                            COALESCE(SUM(ca.outstanding_principal), 0) + SUM(
                                COALESCE(ca.interest_payment,0) +
                                COALESCE(ca.endorsement,0) +
                                COALESCE(ca.vat,0) +
                                COALESCE(ca.seguro_vida,0) +
                                COALESCE(ca.seguro,0) +
                                COALESCE(ca.digital_sign,0) +
                                COALESCE(ca.digital_sign_iva,0)
                            ) AS deuda_actual,
                            DATEDIFF(CURDATE(), MIN(ca.expiration_date)) AS dias_iniciales_mes,
                            COUNT(ca.id) AS cuotas_atrasadas,
                            (
                                SELECT al2.quota
                                FROM application_loan al2
                                WHERE al2.application_id = a.id
                                ORDER BY al2.id DESC
                                LIMIT 1
                            ) AS quota
                        FROM contract c
                        JOIN application a ON a.id = c.application_id
                        JOIN customer cu ON cu.id = a.customer_id
                        LEFT JOIN contract_amortization ca
                            ON ca.contract_id = c.id
                            AND ca.contract_amortization_payment_status_id = 4
                            AND ca.expiration_date <= CURDATE()
                            AND ca.outstanding_principal > 0
                        WHERE c.id IN ({batch_str}){self._pagare_exclude_sql('c.pagare_status_id')}
                        GROUP BY c.id, a.id, cu.name, cu.name2, cu.last_name, cu.last_name2,
                                 cu.phone, cu.email, cu.dni, cu.departament_reference
                    """)

                    result = mysql_session.execute(query)
                    for row in result:
                        all_rows.append(row)

            if not all_rows:
                return None

            # Construir DataFrame con las mismas columnas del reporte
            contrato_col = cols_by_lower.get('contrato_x', 'contrato_x')
            llave_col = cols_by_lower.get('llave', 'llave')
            producto_col = cols_by_lower.get('producto', 'producto')
            cliente_col = cols_by_lower.get('cliente', 'cliente')
            telefono_col = cols_by_lower.get('telefono', 'telefono')
            correo_col = cols_by_lower.get('correo', 'correo')
            cedula_col = cols_by_lower.get('cedula', 'cedula')
            ciudad_col = cols_by_lower.get('ciudad', 'ciudad')
            capital_col = cols_by_lower.get('capital_pendiente', 'capital_pendiente')
            gastos_col = cols_by_lower.get('gastos_vencidos', 'gastos_vencidos')
            deuda_col = cols_by_lower.get('deuda_actual', 'deuda_actual')
            dias_col = cols_by_lower.get('dias_iniciales_mes', 'dias_iniciales_mes')
            cuotas_col = (
                cols_by_lower.get('cuotas atrasadas')
                or cols_by_lower.get('cuotas_atrasadas')
                or CUOTAS_ATRASADAS_COL
            )
            base_cols = {
                'contrato': contrato_col, 'llave': llave_col, 'producto': producto_col,
                'cliente': cliente_col, 'telefono': telefono_col, 'correo': correo_col,
                'cedula': cedula_col, 'ciudad': ciudad_col, 'capital': capital_col,
                'gastos': gastos_col, 'deuda': deuda_col, 'dias': dias_col,
                'cuotas': cuotas_col,
            }

            rows_data = [
                self._build_missing_contract_row(
                    row, target_columns, cols_by_lower, base_cols,
                )
                for row in all_rows
            ]

            return pd.DataFrame(rows_data)

        except Exception as error:
            logger.warning(
                "No se pudo consultar MySQL para contratos faltantes: %s", error,
            )
            return None

    def _load_operational_days_overdue(self, contracts: List[int]) -> Dict[int, int]:
        """
        Obtiene dias de atraso con la logica operativa usada por asignacion.
        """
        if not contracts:
            return {}

        try:
            from app.database.connections import db_manager
            from app.services.contract_service import ContractService

            with db_manager.get_mysql_session() as mysql_session:
                contract_service = ContractService(mysql_session)
                return contract_service.get_days_overdue_for_contracts(
                    [int(contract_id) for contract_id in contracts]
                )
        except Exception as error:
            logger.warning(
                "No se pudo cargar dias de atraso operativos para reporte: %s",
                error,
            )
            return {}

    def _load_operational_overdue_installments(self, contracts: List[int]) -> Dict[int, int]:
        """
        Obtiene cantidad de cuotas atrasadas con la misma logica operativa
        usada en el proceso de asignacion.
        """
        if not contracts:
            return {}

        try:
            from app.database.connections import db_manager
            from app.services.contract_service import ContractService

            with db_manager.get_mysql_session() as mysql_session:
                contract_service = ContractService(mysql_session)
                return contract_service.get_overdue_installments_count_for_contracts(
                    [int(contract_id) for contract_id in contracts]
                )
        except Exception as error:
            logger.warning(
                "No se pudo cargar cuotas atrasadas operativas para reporte: %s",
                error,
            )
            return {}

    def _apply_operational_days_and_ranges(
        self,
        df: pd.DataFrame,
        cols_by_lower: Dict[str, str],
        days_overdue_map: Optional[Dict[int, int]],
        overdue_installments_map: Optional[Dict[int, int]] = None,
    ) -> None:
        """
        Reemplaza en el DataFrame de reporte los campos de dias/rango por la
        misma logica de asignacion operativa.
        """
        if not days_overdue_map:
            return

        contract_col = (
            cols_by_lower.get("contrato_x")
            or cols_by_lower.get("contrato")
            or cols_by_lower.get("contract_id")
        )
        if not contract_col:
            return

        days_col = cols_by_lower.get("dias_iniciales_mes")
        overdue_installments_col = (
            cols_by_lower.get("cuotas atrasadas")
            or cols_by_lower.get("cuotas_atrasadas")
        )
        range_col = (
            cols_by_lower.get("rango")
            or cols_by_lower.get("rango dias")
            or cols_by_lower.get("rango_dias")
        )

        if not days_col and not range_col:
            return

        # Mapea contrato -> dias operativos
        def _resolve_days(contract_value) -> int:
            contract_id = self._safe_int(contract_value)
            if contract_id is None:
                return 0
            return int(days_overdue_map.get(contract_id, 0))

        contract_days = df[contract_col].apply(_resolve_days)

        if days_col:
            df[days_col] = contract_days

        if range_col:
            df[range_col] = contract_days.apply(
                lambda days: (
                    get_assignment_dpd_range(int(days))
                    or get_dpd_range(int(days))
                    or "0"
                )
            )

        if overdue_installments_map is not None:
            self._apply_overdue_installments(
                df, contract_col, overdue_installments_col, overdue_installments_map,
            )

    def _apply_overdue_installments(
        self,
        df: pd.DataFrame,
        contract_col: str,
        overdue_installments_col: Optional[str],
        overdue_installments_map: Dict[int, int],
    ) -> None:
        """Aplica las cuotas atrasadas operativas a la columna correspondiente."""
        def _resolve_overdue_installments(contract_value) -> int:
            contract_id = self._safe_int(contract_value)
            if contract_id is None:
                return 0
            return int(overdue_installments_map.get(contract_id, 0))

        if overdue_installments_col:
            df[overdue_installments_col] = df[contract_col].apply(
                _resolve_overdue_installments
            )
        else:
            df[CUOTAS_ATRASADAS_COL] = df[contract_col].apply(
                _resolve_overdue_installments
            )
    
    def calculate_distribution_metrics(self) -> Dict:
        """
        Calcula métricas de distribución 60/40 entre Serlefin y Cobyser
        
        Returns:
            Dict: Métricas de distribución
        """
        try:
            contracts_81 = self.get_assigned_contracts_for_house(settings.SERLEFIN_USERS)
            contracts_45 = self.get_assigned_contracts_for_house(settings.COBYSER_USERS)

            total = len(contracts_81) + len(contracts_45)
            
            if total == 0:
                return {
                    'total': 0,
                    'serlefin': 0,
                    'cobyser': 0,
                    'serlefin_percent': 0,
                    'cobyser_percent': 0,
                    'cumple_60_40': False,
                    'diferencia_60': 0,
                    'diferencia_40': 0,
                    'bucket_distribution': [],
                }
            
            serlefin_percent = (len(contracts_81) / total) * 100
            cobyser_percent = (len(contracts_45) / total) * 100
            
            # Tolerancia de 2%
            cumple_60_40 = (58 <= serlefin_percent <= 62) and (38 <= cobyser_percent <= 42)
            
            manual_fixed_81 = len(MANUAL_FIXED_CONTRACTS.get(81, []))
            manual_fixed_45 = len(MANUAL_FIXED_CONTRACTS.get(45, []))
            bucket_distribution = self._calculate_bucket_distribution(
                contracts_81=contracts_81,
                contracts_45=contracts_45,
            )

            # Conteo Twist (aditivo): no altera las llaves Phone existentes.
            try:
                t1_45 = len(self.get_assigned_twist1_for_house(settings.COBYSER_USERS))
                t1_81 = len(self.get_assigned_twist1_for_house(settings.SERLEFIN_USERS))
                t2_45 = len(self.get_assigned_twist2_for_house(settings.COBYSER_USERS))
                t2_81 = len(self.get_assigned_twist2_for_house(settings.SERLEFIN_USERS))
            except Exception as twist_err:
                logger.warning("No se pudo contar Twist en metricas: %s", twist_err)
                t1_45 = t1_81 = t2_45 = t2_81 = 0

            return {
                'total': total,
                'serlefin': len(contracts_81),
                'cobyser': len(contracts_45),
                'serlefin_percent': round(serlefin_percent, 2),
                'cobyser_percent': round(cobyser_percent, 2),
                'cumple_60_40': cumple_60_40,
                'diferencia_60': round(serlefin_percent - 60, 2),
                'diferencia_40': round(cobyser_percent - 40, 2),
                'manual_fixed_81': manual_fixed_81,
                'manual_fixed_45': manual_fixed_45,
                'bucket_distribution': bucket_distribution,
                # --- NUEVO: conteo por producto (Phone es el de arriba) ---
                'productos': {
                    'phone': {'cobyser': len(contracts_45), 'serlefin': len(contracts_81),
                              'total': total},
                    'twist1': {'cobyser': t1_45, 'serlefin': t1_81, 'total': t1_45 + t1_81},
                    'twist2': {'cobyser': t2_45, 'serlefin': t2_81, 'total': t2_45 + t2_81},
                },
                'total_cobyser_todos': len(contracts_45) + t1_45 + t2_45,
                'total_serlefin_todos': len(contracts_81) + t1_81 + t2_81,
                'total_todos_productos': total + t1_45 + t1_81 + t2_45 + t2_81,
            }
            
        except Exception as e:
            logger.error(f"Error calculando métricas: {e}")
            return {}
    
    def generate_metrics_html(self, metrics: Dict, audience: str = "general") -> str:
        """
        Genera HTML con metricas de distribucion.

        audience:
        - "general": muestra Serlefin + Cobyser + TOTAL
        - "serlefin": muestra solo Serlefin + TOTAL
        - "cobyser": muestra solo Cobyser + TOTAL
        """
        serlefin_total = int(metrics.get("serlefin", 0) or 0)
        cobyser_total = int(metrics.get("cobyser", 0) or 0)
        total_global = int(metrics.get("total", 0) or 0)
        serlefin_pct = float(metrics.get("serlefin_percent", 0) or 0)
        cobyser_pct = float(metrics.get("cobyser_percent", 0) or 0)

        audience_key = str(audience or "general").strip().lower()
        rows, total_count_row, audience_note = self._metrics_audience_rows(
            audience_key, serlefin_total, cobyser_total, total_global,
            serlefin_pct, cobyser_pct,
        )

        rows_html = ""
        for name, qty, pct in rows:
            rows_html += f"""
            <tr>
                <td style=\"border: 1px solid #ddd; padding: 8px;\"><strong>{name}</strong></td>
                <td style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">{qty}</td>
                <td style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">{pct}%</td>
            </tr>
            """

        html = f"""
        <h3>Metricas de Distribucion</h3>
        <table style=\"width:100%; border-collapse: collapse;\">
            <tr style=\"background-color: #f0f0f0;\">
                <th style=\"border: 1px solid #ddd; padding: 8px; text-align: left;\">Casa de Cobranza</th>
                <th style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">Contratos Asignados</th>
                <th style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">Porcentaje</th>
            </tr>
            {rows_html}
            <tr style=\"background-color: #f9f9f9; font-weight: bold;\">
                <td style=\"border: 1px solid #ddd; padding: 8px;\">TOTAL</td>
                <td style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">{total_count_row}</td>
                <td style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">100%</td>
            </tr>
        </table>
        {audience_note}
        """

        if audience_key == "general":
            html += self._metrics_general_section(metrics)

        return html

    @staticmethod
    def _metrics_audience_rows(
        audience_key: str,
        serlefin_total: int, cobyser_total: int, total_global: int,
        serlefin_pct: float, cobyser_pct: float,
    ) -> Tuple[list, int, str]:
        """Filas/totales/nota segun la audiencia del correo de metricas."""
        if audience_key == "serlefin":
            rows = [("Serlefin (User 81)", serlefin_total, serlefin_pct)]
            total_count_row = serlefin_total
            audience_note = (
                "<p><small>Vista filtrada: este correo solo muestra Serlefin.</small></p>"
            )
        elif audience_key == "cobyser":
            rows = [("Cobyser (User 45)", cobyser_total, cobyser_pct)]
            total_count_row = cobyser_total
            audience_note = (
                "<p><small>Vista filtrada: este correo solo muestra Cobyser.</small></p>"
            )
        else:
            rows = [
                ("Serlefin (User 81)", serlefin_total, serlefin_pct),
                ("Cobyser (User 45)", cobyser_total, cobyser_pct),
            ]
            total_count_row = total_global
            audience_note = ""
        return rows, total_count_row, audience_note

    def _metrics_general_section(self, metrics: Dict) -> str:
        """Seccion HTML extra (cumplimiento + buckets) para la audiencia general."""
        cumple_icon = "OK" if metrics.get("cumple_60_40") else "ALERTA"
        cumple_text = "SI CUMPLE" if metrics.get("cumple_60_40") else "NO CUMPLE"
        html = f"""
            <p style=\"margin-top: 15px;\">
                <strong>{cumple_icon} Cumplimiento 60/40:</strong> {cumple_text}<br>
                <small>Meta: Serlefin 60% / Cobyser 40% (tolerancia +/-2%)</small>
            </p>
            """

        bucket_rows = metrics.get("bucket_distribution", []) or []
        if bucket_rows:
            bucket_rows_html = ""
            for row in bucket_rows:
                bucket_rows_html += f"""
                    <tr>
                        <td style=\"border: 1px solid #ddd; padding: 8px;\"><strong>{row.get('bucket', '')}</strong></td>
                        <td style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">{int(row.get('total', 0) or 0)}</td>
                        <td style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">{int(row.get('serlefin_assigned', 0) or 0)}</td>
                        <td style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">{int(row.get('cobyser_assigned', 0) or 0)}</td>
                        <td style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">{int(row.get('serlefin_target', 0) or 0)}</td>
                        <td style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">{int(row.get('cobyser_target', 0) or 0)}</td>
                    </tr>
                    """

            html += f"""
                <h3 style=\"margin-top: 20px;\">Distribucion por Bucket (objetivo 60/40)</h3>
                <table style=\"width:100%; border-collapse: collapse;\">
                    <tr style=\"background-color: #f0f0f0;\">
                        <th style=\"border: 1px solid #ddd; padding: 8px; text-align: left;\">Bucket DPD</th>
                        <th style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">Total Bucket</th>
                        <th style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">Asignados Serlefin</th>
                        <th style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">Asignados Cobyser</th>
                        <th style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">Destino Serlefin (60%)</th>
                        <th style=\"border: 1px solid #ddd; padding: 8px; text-align: center;\">Destino Cobyser (40%)</th>
                    </tr>
                    {bucket_rows_html}
                </table>
                """

        return html

    @staticmethod
    def _compute_bucket_targets(total: int, serlefin_ratio: float = 0.6) -> Tuple[int, int]:
        total_int = max(0, int(total))
        ratio = max(0.0, min(1.0, float(serlefin_ratio)))
        exact_81 = total_int * ratio
        exact_45 = total_int * (1.0 - ratio)
        target_81 = int(math.floor(exact_81))
        target_45 = int(math.floor(exact_45))
        remainder = total_int - (target_81 + target_45)
        if remainder > 0:
            frac_81 = exact_81 - target_81
            frac_45 = exact_45 - target_45
            if frac_81 >= frac_45:
                target_81 += remainder
            else:
                target_45 += remainder
        return target_81, target_45

    def _calculate_bucket_distribution(
        self,
        contracts_81: List[int],
        contracts_45: List[int],
    ) -> List[Dict[str, int]]:
        contract_ids = sorted(
            {int(contract_id) for contract_id in (contracts_81 + contracts_45)}
        )
        if not contract_ids:
            return []

        days_map = self._load_operational_days_overdue(contract_ids)
        if not days_map:
            return []

        bucket_totals: Dict[str, Dict[str, int]] = {
            bucket: {
                "total": 0,
                "serlefin_assigned": 0,
                "cobyser_assigned": 0,
            }
            for bucket in ASSIGNMENT_DPD_ORDER
        }

        for contract_id in contracts_81:
            days = int(days_map.get(int(contract_id), 0))
            bucket = get_assignment_dpd_range(days)
            if bucket in bucket_totals:
                bucket_totals[bucket]["total"] += 1
                bucket_totals[bucket]["serlefin_assigned"] += 1

        for contract_id in contracts_45:
            days = int(days_map.get(int(contract_id), 0))
            bucket = get_assignment_dpd_range(days)
            if bucket in bucket_totals:
                bucket_totals[bucket]["total"] += 1
                bucket_totals[bucket]["cobyser_assigned"] += 1

        rows: List[Dict[str, int]] = []
        for bucket in ASSIGNMENT_DPD_ORDER:
            total_bucket = int(bucket_totals[bucket]["total"])
            if total_bucket <= 0:
                continue

            target_81, target_45 = self._compute_bucket_targets(total_bucket, 0.6)
            rows.append(
                {
                    "bucket": bucket,
                    "total": total_bucket,
                    "serlefin_target": target_81,
                    "cobyser_target": target_45,
                    "serlefin_assigned": int(bucket_totals[bucket]["serlefin_assigned"]),
                    "cobyser_assigned": int(bucket_totals[bucket]["cobyser_assigned"]),
                }
            )

        return rows

# Instancia global
report_service_extended = ReportServiceExtended()


