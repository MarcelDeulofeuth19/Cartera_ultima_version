"""
Servicio para generar informes de casa de cobranza (SERLEFIN y COBYSER)
Mantiene la lÃ³gica original exacta del script
"""
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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
        
        # ConfiguraciÃ³n de bases de datos (exacta del script original)
        self.DB_CONFIG_PROD = {
            'host': settings.REPORTS_EXT_PROD_HOST,
            'user': settings.REPORTS_EXT_PROD_USER,
            'password': settings.REPORTS_EXT_PROD_PASSWORD,
            'database': settings.REPORTS_EXT_PROD_DATABASE,
            'port': settings.REPORTS_EXT_PROD_PORT,
            'options': f"-csearch_path={settings.REPORTS_EXT_PROD_SCHEMA}",
            'driver': 'psycopg2'
        }
        
        self.DB_CONFIG_IND = {
            'host': settings.REPORTS_EXT_IND_HOST,
            'user': settings.REPORTS_EXT_IND_USER,
            'password': settings.REPORTS_EXT_IND_PASSWORD,
            'database': settings.REPORTS_EXT_IND_DATABASE,
            'port': settings.REPORTS_EXT_IND_PORT,
            'options': f"-csearch_path={settings.REPORTS_EXT_IND_SCHEMA}",
            'driver': 'psycopg2'
        }
    
    def _get_assigned_contracts(self, user_id: int) -> List[int]:
        """Obtener contratos asignados a un usuario especÃ­fico"""
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

    def _get_assigned_contracts_for_house(self, user_ids: List[int]) -> List[int]:
        """Obtener TODOS los contratos asignados a cualquier usuario de la casa."""
        if not user_ids:
            return []
        users_str = ",".join(str(int(uid)) for uid in user_ids)
        query = f"""
        SELECT DISTINCT contract_id
        FROM contract_advisors
        WHERE user_id IN ({users_str});
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
    
    def _fetch_missing_from_mysql(
        self, missing_ids: List[int], target_columns: List[str],
    ) -> Optional[pd.DataFrame]:
        """Consulta MySQL para contratos que no existen en PG produccion."""
        if not missing_ids:
            return None

        try:
            from sqlalchemy import text

            batch_size = 1000
            all_rows = []

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
                            COALESCE(ca.interest_payment,0) + COALESCE(ca.endorsement,0) +
                            COALESCE(ca.vat,0) + COALESCE(ca.seguro_vida,0) +
                            COALESCE(ca.seguro,0) + COALESCE(ca.digital_sign,0) +
                            COALESCE(ca.digital_sign_iva,0)
                        ) AS gastos_vencidos,
                        COALESCE(SUM(ca.outstanding_principal), 0) + SUM(
                            COALESCE(ca.interest_payment,0) + COALESCE(ca.endorsement,0) +
                            COALESCE(ca.vat,0) + COALESCE(ca.seguro_vida,0) +
                            COALESCE(ca.seguro,0) + COALESCE(ca.digital_sign,0) +
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

                result = self.mysql_session.execute(query)
                for row in result:
                    all_rows.append(row)

            if not all_rows:
                return None

            cols_lower = {str(c).lower(): c for c in target_columns}
            contrato_col = cols_lower.get('contrato_x', 'contrato_x')
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

                if dias <= 150:
                    fc = 1.0
                elif dias <= 180:
                    fc = 0.95
                elif dias <= 300:
                    fc = 0.90
                else:
                    fc = 0.75
                if dias <= 90:
                    fg = 0.70
                elif dias <= 120:
                    fg = 0.60
                elif dias <= 150:
                    fg = 0.50
                elif dias <= 365:
                    fg = 0.40
                else:
                    fg = 0.0

                vfd = round(capital * fc + gastos * fg)

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
                r[cols_lower.get('llave', 'llave')] = llave
                r[cols_lower.get('producto', 'producto')] = producto
                r[cols_lower.get('cliente', 'cliente')] = cliente
                r[cols_lower.get('telefono', 'telefono')] = telefono
                r[cols_lower.get('correo', 'correo')] = correo
                r[cols_lower.get('cedula', 'cedula')] = cedula
                r[cols_lower.get('ciudad', 'ciudad')] = ciudad
                r[cols_lower.get('capital_pendiente', 'capital_pendiente')] = capital
                r[cols_lower.get('gastos_vencidos', 'gastos_vencidos')] = gastos
                r[cols_lower.get('deuda_actual', 'deuda_actual')] = deuda
                r[cols_lower.get('dias_iniciales_mes', 'dias_iniciales_mes')] = dias
                cuotas_key = cols_lower.get('cuotas atrasadas') or cols_lower.get('cuotas_atrasadas') or 'Cuotas Atrasadas'
                r[cuotas_key] = cuotas
                pc = cols_lower.get('%_pago_capital')
                if pc:
                    r[pc] = f"{int(fc * 100)}%"
                dg = cols_lower.get('%_descuento_gastos')
                if dg:
                    r[dg] = f"{int(fg * 100)}%"
                v = cols_lower.get('valor_final_descuento')
                if v:
                    r[v] = vfd
                vop1 = cols_lower.get('valor_opcion_1')
                if vop1:
                    r[vop1] = quota
                o2_1 = cols_lower.get('valor_1_cuota_opcion_2')
                if o2_1:
                    r[o2_1] = deuda
                o2_2 = cols_lower.get('valor_2_cuotas_opcion_2')
                if o2_2:
                    r[o2_2] = round(deuda / 2) if deuda else 0
                o2_3 = cols_lower.get('valor_3_cuotas_opcion_2')
                if o2_3:
                    r[o2_3] = round(deuda / 3) if deuda > 600000 else None
                o3_1 = cols_lower.get('valor_1_cuota_opcion_3')
                if o3_1:
                    r[o3_1] = vfd
                o3_2 = cols_lower.get('valor_2_cuotas_opcion_3')
                if o3_2:
                    r[o3_2] = round(vfd / 2) if vfd else 0
                o3_3 = cols_lower.get('valor_3_cuotas_opcion_3')
                if o3_3:
                    r[o3_3] = round(vfd / 3) if vfd > 600000 else None
                o4_1 = cols_lower.get('valor_1_cuota_opcion_4')
                if o4_1:
                    r[o4_1] = capital
                o4_2 = cols_lower.get('valor_2_cuotas_opcion_4')
                if o4_2:
                    r[o4_2] = round(capital / 2) if capital else 0
                o4_3 = cols_lower.get('valor_3_cuotas_opcion_4')
                if o4_3:
                    r[o4_3] = round(capital / 3) if capital > 600000 else None
                cm = cols_lower.get('comision')
                if cm:
                    r[cm] = comision
                rg = cols_lower.get('rango')
                if rg:
                    r[rg] = rango
                d1 = cols_lower.get('descripcion_opcion_1')
                if d1:
                    r[d1] = 'Pagar_1_cuota__para_normalizar'
                d2 = cols_lower.get('descripcion_opcion_2')
                if d2:
                    r[d2] = 'Pagar_de_1_a_3_cuotas'
                d3 = cols_lower.get('descripcion_opcion_3')
                if d3:
                    r[d3] = 'descuento_1_cta_100%_2ctas<=$600k__3ctas>$600k'
                d4 = cols_lower.get('descripcion_opcion_4')
                if d4:
                    r[d4] = 'cap_pendiente_1_cta_100%_2ctas<=$600k__3ctas>$600k'
                rows_data.append(r)

            return pd.DataFrame(rows_data)

        except Exception as error:
            logger.warning("No se pudo consultar MySQL para contratos faltantes: %s", error)
            return None

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
        
        # Obtener contratos asignados (TODOS los usuarios de cada casa)
        logger.info("1) Obteniendo contratos asignados...")
        contracts_81 = self._get_assigned_contracts_for_house(settings.SERLEFIN_USERS)
        contracts_45 = self._get_assigned_contracts_for_house(settings.COBYSER_USERS)

        result['serlefin_contracts'] = len(contracts_81)
        result['cobyser_contracts'] = len(contracts_45)

        logger.info(f"  - SERLEFIN (todos los usuarios): {len(contracts_81)} contratos")
        logger.info(f"  - COBYSER (todos los usuarios): {len(contracts_45)} contratos")

        if not contracts_81 and not contracts_45:
            logger.warning("No hay contratos asignados a Serlefin ni Cobyser")
            return result
        
        # Fecha actual para nombres de archivo
        fecha_actual = datetime.now().strftime('%d-%m-%y')
        reports_dir = settings.REPORTS_DIR
        os.makedirs(reports_dir, exist_ok=True)
        
        # GENERAR INFORME USER 81 (SERLEFIN)
        if contracts_81:
            logger.info(f"\nðŸ“Š Generando reporte para USER 81 - SERLEFIN ({len(contracts_81)} contratos)...")
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

                # Recuperar contratos faltantes desde MySQL
                if 'contrato_x' in df_81.columns and len(df_81) < len(contracts_81):
                    reported_ids = set(df_81['contrato_x'].dropna().astype(int).values)
                    missing_ids = [cid for cid in contracts_81 if cid not in reported_ids]
                    if missing_ids:
                        logger.warning(
                            "SERLEFIN: %d contratos no en PG produccion. Consultando MySQL.",
                            len(missing_ids),
                        )
                        mysql_df = self._fetch_missing_from_mysql(missing_ids, df_81.columns.tolist())
                        if mysql_df is not None and not mysql_df.empty:
                            df_81 = pd.concat([df_81, mysql_df], ignore_index=True)
                            logger.info("SERLEFIN: %d contratos recuperados desde MySQL.", len(mysql_df))

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
                logger.info(f"âœ… INFORME USER 81 (SERLEFIN) GENERADO: {file_name_81}")
                
            finally:
                conn_prod.close()
        
        # GENERAR INFORME USER 45 (COBYSER)
        if contracts_45:
            logger.info(f"\nðŸ“Š Generando reporte para USER 45 - COBYSER ({len(contracts_45)} contratos)...")
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

                # Recuperar contratos faltantes desde MySQL
                if 'contrato_x' in df_45.columns and len(df_45) < len(contracts_45):
                    reported_ids = set(df_45['contrato_x'].dropna().astype(int).values)
                    missing_ids = [cid for cid in contracts_45 if cid not in reported_ids]
                    if missing_ids:
                        logger.warning(
                            "COBYSER: %d contratos no en PG produccion. Consultando MySQL.",
                            len(missing_ids),
                        )
                        mysql_df = self._fetch_missing_from_mysql(missing_ids, df_45.columns.tolist())
                        if mysql_df is not None and not mysql_df.empty:
                            df_45 = pd.concat([df_45, mysql_df], ignore_index=True)
                            logger.info("COBYSER: %d contratos recuperados desde MySQL.", len(mysql_df))

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
                logger.info(f"âœ… INFORME USER 45 (COBYSER) GENERADO: {file_name_45}")
                
            finally:
                conn_prod.close()
        
        logger.info("\nðŸ”¥ PROCESO COMPLETADO")
        return result

