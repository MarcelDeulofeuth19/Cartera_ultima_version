"""
Servicio extendido para generaciÃ³n de reportes detallados de asignaciÃ³n
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


class ReportServiceExtended:
    """Servicio para generaciÃ³n de reportes detallados con informaciÃ³n de contratos fijos"""
    
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
        Genera reporte detallado para un usuario especÃ­fico
        
        Returns:
            Tuple[str, pd.DataFrame]: (ruta_archivo, dataframe)
        """
        if not contracts:
            logger.warning(f"No hay contratos para user {user_id}")
            return None, None
        
        lista_contratos = ",".join(str(x) for x in contracts)
        
        try:
            logger.info(f"ðŸ“Š Generando reporte para {user_name} ({len(contracts)} contratos)...")
            
            conn = psycopg2.connect(
                host=self.db_config_prod['host'],
                user=self.db_config_prod['user'],
                password=self.db_config_prod['password'],
                dbname=self.db_config_prod['database'],
                port=self.db_config_prod['port'],
                options=self.db_config_prod['options']
            )
            
            query = self.generate_detailed_query(lista_contratos)
            df = pd.read_sql(query, conn)
            conn.close()

            # PostgreSQL normaliza a minusculas aliases sin comillas.
            cols_by_lower = {str(col).lower(): col for col in df.columns}

            # Detectar contratos asignados que no existen en PostgreSQL produccion
            # y obtener sus datos desde MySQL (fuente original).
            contrato_col_temp = cols_by_lower.get('contrato_x')
            if contrato_col_temp and len(df) < len(contracts):
                reported_ids = set(df[contrato_col_temp].dropna().astype(int).values)
                missing_ids = [cid for cid in contracts if cid not in reported_ids]
                if missing_ids:
                    logger.warning(
                        "Reporte %s: %d contratos no existen en PG produccion. "
                        "Consultando MySQL como fallback.",
                        user_name, len(missing_ids),
                    )
                    mysql_df = self._fetch_missing_contracts_from_mysql(
                        missing_ids, df.columns.tolist(), cols_by_lower,
                    )
                    if mysql_df is not None and not mysql_df.empty:
                        df = pd.concat([df, mysql_df], ignore_index=True)
                        logger.info(
                            "Reporte %s: %d contratos recuperados desde MySQL.",
                            user_name, len(mysql_df),
                        )

            # Forzar dias/rango del reporte con la misma logica operativa de asignacion (MySQL).
            if days_overdue_map is None:
                days_overdue_map = self._load_operational_days_overdue(contracts)
            overdue_installments_map = self._load_operational_overdue_installments(contracts)

            self._apply_operational_days_and_ranges(
                df,
                cols_by_lower,
                days_overdue_map,
                overdue_installments_map,
            )

            # Eliminar campos innecesarios
            for col in ['cantidad_cuotas_pagados', 'Marca']:
                if col in df.columns:
                    df = df.drop(columns=[col])

            # Agregar campo "Contrato Fijo"
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

            # Ajustar comisiÃ³n para Cobyser (Usuario 45)
            if user_id == 45:
                comision_col = cols_by_lower.get('comision')
                if comision_col:
                    df[comision_col] = '30%'
            
            # Agregar campo NIT al inicio
            df.insert(0, 'NIT', '901546410-9')
            
            # Generar nombre de archivo
            fecha_actual = datetime.now().strftime('%d-%m-%y')
            if user_id == 81:
                file_name = f"AloCredit-Phone-{fecha_actual}_INFORME_Serlefin.xlsx"
            elif user_id == 45:
                file_name = f"AloCredit-Phone-{fecha_actual}_INFORME_Cobyser.xlsx"
            else:
                file_name = f"AloCredit-Phone-{fecha_actual}_INFORME_User{user_id}.xlsx"
            
            file_path = self.reports_dir / file_name
            
            # Guardar Excel
            df.to_excel(file_path, index=False)
            logger.info(f"âœ… INFORME GENERADO: {file_path}")
            
            return str(file_path), df
            
        except Exception as e:
            logger.error(f"âŒ Error generando reporte para user {user_id}: {e}")
            return None, None

    @staticmethod
    def _safe_int(value) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except Exception:
            return None

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
                        WHERE c.id IN ({batch_str})
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
                or 'Cuotas Atrasadas'
            )

            rows_data = []
            for row in all_rows:
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

                valor_final_descuento = round(capital * factor_capital + gastos * factor_gastos)

                # Comision
                if 1 <= dias <= 60:
                    comision = '4%'
                elif 61 <= dias <= 90:
                    comision = '6%'
                elif 91 <= dias <= 150:
                    comision = '8%'
                elif 151 <= dias <= 210:
                    comision = '11%'
                elif dias == 211:
                    comision = '13%'
                elif dias >= 212:
                    comision = '15%'
                else:
                    comision = '0%'

                # Rango
                if 1 <= dias <= 30:
                    rango = '1_30'
                elif 31 <= dias <= 60:
                    rango = '31_60'
                elif 61 <= dias <= 90:
                    rango = '61_90'
                elif 91 <= dias <= 150:
                    rango = '91_150'
                elif 151 <= dias <= 210:
                    rango = '151_210'
                elif dias == 211:
                    rango = '211'
                elif dias >= 212:
                    rango = 'Cartera Castigada'
                else:
                    rango = '0'

                r = {col: None for col in target_columns}
                r[contrato_col] = contract_id
                r[llave_col] = llave
                r[producto_col] = producto
                r[cliente_col] = cliente
                r[telefono_col] = telefono
                r[correo_col] = correo
                r[cedula_col] = cedula
                r[ciudad_col] = ciudad
                r[capital_col] = capital
                r[gastos_col] = gastos
                r[deuda_col] = deuda
                r[dias_col] = dias
                r[cuotas_col] = cuotas

                # Campos calculados
                pago_cap_col = cols_by_lower.get('%_pago_capital')
                if pago_cap_col:
                    r[pago_cap_col] = f"{int(factor_capital * 100)}%"
                desc_gastos_col = cols_by_lower.get('%_descuento_gastos')
                if desc_gastos_col:
                    r[desc_gastos_col] = f"{int(factor_gastos * 100)}%"
                vfd_col = cols_by_lower.get('valor_final_descuento')
                if vfd_col:
                    r[vfd_col] = valor_final_descuento

                # Opciones de pago
                quota_col = cols_by_lower.get('valor_opcion_1')
                if quota_col:
                    r[quota_col] = quota
                op2_1 = cols_by_lower.get('valor_1_cuota_opcion_2')
                if op2_1:
                    r[op2_1] = deuda
                op2_2 = cols_by_lower.get('valor_2_cuotas_opcion_2')
                if op2_2:
                    r[op2_2] = round(deuda / 2) if deuda else 0
                op2_3 = cols_by_lower.get('valor_3_cuotas_opcion_2')
                if op2_3:
                    r[op2_3] = round(deuda / 3) if deuda > 600000 else None
                op3_1 = cols_by_lower.get('valor_1_cuota_opcion_3')
                if op3_1:
                    r[op3_1] = valor_final_descuento
                op3_2 = cols_by_lower.get('valor_2_cuotas_opcion_3')
                if op3_2:
                    r[op3_2] = round(valor_final_descuento / 2) if valor_final_descuento else 0
                op3_3 = cols_by_lower.get('valor_3_cuotas_opcion_3')
                if op3_3:
                    r[op3_3] = round(valor_final_descuento / 3) if valor_final_descuento > 600000 else None
                op4_1 = cols_by_lower.get('valor_1_cuota_opcion_4')
                if op4_1:
                    r[op4_1] = capital
                op4_2 = cols_by_lower.get('valor_2_cuotas_opcion_4')
                if op4_2:
                    r[op4_2] = round(capital / 2) if capital else 0
                op4_3 = cols_by_lower.get('valor_3_cuotas_opcion_4')
                if op4_3:
                    r[op4_3] = round(capital / 3) if capital > 600000 else None

                comision_col = cols_by_lower.get('comision')
                if comision_col:
                    r[comision_col] = comision
                rango_col = cols_by_lower.get('rango')
                if rango_col:
                    r[rango_col] = rango

                desc1 = cols_by_lower.get('descripcion_opcion_1')
                if desc1:
                    r[desc1] = 'Pagar_1_cuota__para_normalizar'
                desc2 = cols_by_lower.get('descripcion_opcion_2')
                if desc2:
                    r[desc2] = 'Pagar_de_1_a_3_cuotas'
                desc3 = cols_by_lower.get('descripcion_opcion_3')
                if desc3:
                    r[desc3] = 'descuento_1_cta_100%_2ctas<=$600k__3ctas>$600k'
                desc4 = cols_by_lower.get('descripcion_opcion_4')
                if desc4:
                    r[desc4] = 'cap_pendiente_1_cta_100%_2ctas<=$600k__3ctas>$600k'

                rows_data.append(r)

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
                df["Cuotas Atrasadas"] = df[contract_col].apply(
                    _resolve_overdue_installments
                )
    
    def calculate_distribution_metrics(self) -> Dict:
        """
        Calcula mÃ©tricas de distribuciÃ³n 60/40 entre Serlefin y Cobyser
        
        Returns:
            Dict: MÃ©tricas de distribuciÃ³n
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
            }
            
        except Exception as e:
            logger.error(f"Error calculando mÃ©tricas: {e}")
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
        rows = []
        total_count_row = total_global

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
            audience_note = ""

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
            cumple_icon = "OK" if metrics.get("cumple_60_40") else "ALERTA"
            cumple_text = "SI CUMPLE" if metrics.get("cumple_60_40") else "NO CUMPLE"
            html += f"""
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


