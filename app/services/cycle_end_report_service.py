"""Informe de finalizacion de ciclo - casa de cobranza.

Genera el CSV de 29 columnas (formato AloCredit-Phone) para los contratos que
HOY estan asignados a las casas de cobranza Serlefin y Cobyser
(tabla alocreditindicators.contract_advisors) y lo envia por correo.

Notas de fuentes de datos:
- Contratos asignados: PostgreSQL indicators (contract_advisors).
- Cuotas atrasadas/pendientes/sin pagar y dias de mora: PostgreSQL prod
  (con respaldo en MySQL para contratos ausentes en PG).
- Pagos del mes (Ingreso_Mes_Actual, Valor_cuotas_pagadas, etc.): MySQL, que es
  la fuente viva (PostgreSQL prod no tiene los pagos recientes).
- Gestion (motivo, accion, resultado, efecto, mejor gestion, promesa): PG indicators.

Usado por:
- AutoAssignmentScheduler._run_monthly_report_sync (ultimo dia de cada mes).
- run_cycle_end_report.py (ejecucion manual / prueba).
"""
import calendar
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
import psycopg2
from sqlalchemy import text

from app.core.config import settings
from app.core.dpd import get_assignment_dpd_range, get_dpd_range
from app.database.connections import db_manager
from app.services.email_service import email_service

logger = logging.getLogger(__name__)

MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre",
    12: "Diciembre",
}

# Casas de cobranza: clave interna -> (etiqueta visible, lista de user_id)
HOUSES: Dict[str, Tuple[str, List[int]]] = {
    "serlefin": ("Serlefin", list(settings.SERLEFIN_USERS)),
    "cobyser": ("Cobyser", list(settings.COBYSER_USERS)),
}

RANGO_INICIAL = "Rango Inicial"

OUTPUT_COLUMNS = [
    "Llave", "Producto", "Contrato", "Cedula", "estado_contrato",
    "cantidad_cuotas_atrasadas", "vr_pagos_atrasadas",
    "Cuotas_pendientes", "vr_cuotas_pendientes",
    "cantidad_cuotas_sin_pagar", "vr_cuotas_sin_pagar",
    "cantidad_cuotas_pagados", "Valor_cuotas_pagadas",
    "Dias_iniciales_Mes", RANGO_INICIAL,
    "Dias_Actual", "Rango_Actual", "Tipo",
    "Ingreso_Mes_Actual", "Fecha_ingreso_actual",
    "MOTIVO", "Pais", "intentos",
    "Fecha_gestion", "Accion", "Resultado", "Efecto", "Mejor_gestion",
    "Fecha_promesa", "Telefono",
]


# --------------------------------------------------------------------------- #
# Fechas del ciclo
# --------------------------------------------------------------------------- #
def month_bounds(report_date: date) -> Tuple[date, date]:
    """Primer y ultimo dia del mes de `report_date`."""
    first = report_date.replace(day=1)
    last_day = calendar.monthrange(report_date.year, report_date.month)[1]
    last = report_date.replace(day=last_day)
    return first, last


# --------------------------------------------------------------------------- #
# Conexiones PostgreSQL (prod / indicators)
# --------------------------------------------------------------------------- #
def _connect_pg(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"], user=cfg["user"], password=cfg["password"],
        dbname=cfg["database"], port=cfg["port"], options=cfg["options"],
    )


def _prod_cfg() -> dict:
    return {
        "host": settings.REPORTS_EXT_PROD_HOST, "user": settings.REPORTS_EXT_PROD_USER,
        "password": settings.REPORTS_EXT_PROD_PASSWORD, "database": settings.REPORTS_EXT_PROD_DATABASE,
        "port": settings.REPORTS_EXT_PROD_PORT,
        "options": f"-csearch_path={settings.REPORTS_EXT_PROD_SCHEMA}",
    }


def _ind_cfg() -> dict:
    return {
        "host": settings.REPORTS_EXT_IND_HOST, "user": settings.REPORTS_EXT_IND_USER,
        "password": settings.REPORTS_EXT_IND_PASSWORD, "database": settings.REPORTS_EXT_IND_DATABASE,
        "port": settings.REPORTS_EXT_IND_PORT,
        "options": f"-csearch_path={settings.REPORTS_EXT_IND_SCHEMA}",
    }


# --------------------------------------------------------------------------- #
# Contratos asignados HOY a la casa
# --------------------------------------------------------------------------- #
def get_assigned_contracts(user_ids: Sequence[int]) -> List[int]:
    """Contratos actualmente asignados (contract_advisors) a los user_id dados."""
    ids = ",".join(str(int(u)) for u in user_ids)
    conn = _connect_pg(_ind_cfg())
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT DISTINCT contract_id FROM contract_advisors WHERE user_id IN ({ids})"
        )
        return sorted(int(r[0]) for r in cur.fetchall())
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Consultas de cuotas / dias (PG prod) y respaldo MySQL
# --------------------------------------------------------------------------- #
def _query_pg_prod(contracts: List[int], prim_dia: date, fecha_tope: date) -> pd.DataFrame:
    lista = ",".join(str(int(c)) for c in contracts)
    fecha_tope_s = fecha_tope.isoformat()
    fecha_tope_ts = f"{fecha_tope_s} 23:59:59"
    prim_dia_s = prim_dia.isoformat()
    cond_no = (
        "(contract_amortization_payment_status_id NOT IN (1,5) "
        "OR payment_date IS NULL "
        f"OR payment_date > TIMESTAMP '{fecha_tope_ts}')"
    )
    cond_no_ca = (
        "(ca.contract_amortization_payment_status_id NOT IN (1,5) "
        "OR ca.payment_date IS NULL "
        f"OR ca.payment_date > TIMESTAMP '{fecha_tope_ts}')"
    )
    GASTOS = ("COALESCE(outstanding_principal,0) + COALESCE(interest_payment,0) + "
              "COALESCE(endorsement,0) + COALESCE(vat,0) + COALESCE(seguro_vida,0) + "
              "COALESCE(seguro,0) + COALESCE(digital_sign,0) + COALESCE(digital_sign_iva,0)")
    sql = f"""
WITH
CuotasAtrasadas AS (
    SELECT contract_id, COUNT(*) AS cant, SUM({GASTOS}) AS valor
    FROM contract_amortization
    WHERE contract_id IN ({lista})
      AND {cond_no}
      AND expiration_date <= DATE '{fecha_tope_s}'
    GROUP BY contract_id
),
CuotasPendientes AS (
    SELECT contract_id, COUNT(*) AS cant, SUM({GASTOS}) AS valor
    FROM contract_amortization
    WHERE contract_id IN ({lista})
      AND {cond_no}
      AND expiration_date > DATE '{fecha_tope_s}'
    GROUP BY contract_id
),
CuotasSinPagar AS (
    SELECT contract_id, COUNT(*) AS cant, SUM({GASTOS}) AS valor
    FROM contract_amortization
    WHERE contract_id IN ({lista})
      AND {cond_no}
    GROUP BY contract_id
),
DiasIniciales AS (
    SELECT c.id AS contract_id,
        COALESCE(GREATEST((DATE '{prim_dia_s}' - MIN(ca.expiration_date)::date), 0), 0)::int AS dias
    FROM contract c
    LEFT JOIN contract_amortization ca
      ON ca.contract_id = c.id
     AND {cond_no_ca}
     AND ca.expiration_date <= DATE '{fecha_tope_s}'
    WHERE c.id IN ({lista})
    GROUP BY c.id
),
DiasActual AS (
    SELECT c.id AS contract_id,
        COALESCE(GREATEST((DATE '{fecha_tope_s}' - MIN(ca.expiration_date)::date), 0), 0)::int AS dias
    FROM contract c
    LEFT JOIN contract_amortization ca
      ON ca.contract_id = c.id
     AND {cond_no_ca}
     AND ca.expiration_date <= DATE '{fecha_tope_s}'
    WHERE c.id IN ({lista})
    GROUP BY c.id
)
SELECT
    c.id AS contrato,
    c.contracts_status_id AS estado_contrato,
    cu.dni AS cedula,
    cu.phone AS telefono,
    COALESCE(ca.cant, 0) AS cant_atrasadas,
    COALESCE(ca.valor, 0) AS vr_atrasadas,
    COALESCE(cp.cant, 0) AS cant_pendientes,
    COALESCE(cp.valor, 0) AS vr_pendientes,
    COALESCE(csp.cant, 0) AS cant_sin_pagar,
    COALESCE(csp.valor, 0) AS vr_sin_pagar,
    COALESCE(di.dias, 0) AS dias_iniciales,
    COALESCE(da.dias, 0) AS dias_actual
FROM contract c
LEFT JOIN application a ON a.id = c.application_id
LEFT JOIN customer cu ON cu.id = a.customer_id
LEFT JOIN CuotasAtrasadas ca ON ca.contract_id = c.id
LEFT JOIN CuotasPendientes cp ON cp.contract_id = c.id
LEFT JOIN CuotasSinPagar csp ON csp.contract_id = c.id
LEFT JOIN DiasIniciales di ON di.contract_id = c.id
LEFT JOIN DiasActual da ON da.contract_id = c.id
WHERE c.id IN ({lista})
"""
    conn = _connect_pg(_prod_cfg())
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def _query_mysql_pagos(contracts: List[int], prim_dia: date, fecha_tope: date) -> pd.DataFrame:
    """Pagos del mes para TODOS los contratos (MySQL es la fuente de verdad)."""
    fecha_tope_ts = f"{fecha_tope.isoformat()} 23:59:59"
    prim_dia_s = prim_dia.isoformat()
    cond_pagada = (
        "(ca.contract_amortization_payment_status_id IN (1,5) "
        "AND ca.payment_date IS NOT NULL "
        f"AND ca.payment_date <= '{fecha_tope_ts}')"
    )
    rows = []
    with db_manager.get_mysql_session() as sess:
        for i in range(0, len(contracts), 1000):
            chunk = contracts[i:i + 1000]
            lista = ",".join(str(int(c)) for c in chunk)
            sql = text(f"""
                SELECT c.id AS contrato,
                    SUM(CASE WHEN {cond_pagada} THEN 1 ELSE 0 END) AS cant_pagadas,
                    SUM(CASE WHEN {cond_pagada} THEN COALESCE(ca.amount_payed, ca.fee, 0) ELSE 0 END) AS valor_pagos,
                    SUM(CASE WHEN ca.contract_amortization_payment_status_id IN (1,5)
                              AND ca.payment_date IS NOT NULL
                              AND ca.payment_date >= '{prim_dia_s} 00:00:00'
                              AND ca.payment_date <= '{fecha_tope_ts}'
                          THEN COALESCE(ca.amount_payed, ca.fee, 0) ELSE 0 END) AS monto_mes,
                    (SELECT MAX(ca4.payment_date) FROM contract_amortization ca4
                       WHERE ca4.contract_id = c.id
                         AND ca4.contract_amortization_payment_status_id IN (1,5)
                         AND ca4.payment_date IS NOT NULL
                         AND ca4.payment_date >= '{prim_dia_s} 00:00:00'
                         AND ca4.payment_date <= '{fecha_tope_ts}') AS ultimo_pago_mes
                FROM contract c
                LEFT JOIN contract_amortization ca ON ca.contract_id = c.id
                WHERE c.id IN ({lista})
                GROUP BY c.id
            """)
            for r in sess.execute(sql):
                rows.append(dict(r._mapping))
    cols = ["contrato", "cant_pagadas", "valor_pagos", "monto_mes", "ultimo_pago_mes"]
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=cols)


def _query_mysql_fallback_basico(missing_ids: List[int], prim_dia: date, fecha_tope: date) -> pd.DataFrame:
    """Cuotas/dias basicos desde MySQL para contratos ausentes en PG prod."""
    if not missing_ids:
        return pd.DataFrame()
    fecha_tope_s = fecha_tope.isoformat()
    fecha_tope_ts = f"{fecha_tope_s} 23:59:59"
    prim_dia_s = prim_dia.isoformat()
    cond_no = (
        "(ca.contract_amortization_payment_status_id NOT IN (1,5) "
        "OR ca.payment_date IS NULL "
        f"OR ca.payment_date > '{fecha_tope_ts}')"
    )
    GASTOS = ("COALESCE(ca.outstanding_principal,0) + COALESCE(ca.interest_payment,0) + "
              "COALESCE(ca.endorsement,0) + COALESCE(ca.vat,0) + COALESCE(ca.seguro_vida,0) + "
              "COALESCE(ca.seguro,0) + COALESCE(ca.digital_sign,0) + COALESCE(ca.digital_sign_iva,0)")
    rows = []
    with db_manager.get_mysql_session() as sess:
        for i in range(0, len(missing_ids), 1000):
            chunk = missing_ids[i:i + 1000]
            lista = ",".join(str(int(c)) for c in chunk)
            sql = text(f"""
                SELECT c.id AS contrato,
                    c.contracts_status_id AS estado_contrato,
                    cu.dni AS cedula, cu.phone AS telefono,
                    SUM(CASE WHEN {cond_no} AND ca.expiration_date <= '{fecha_tope_s}' THEN 1 ELSE 0 END) AS cant_atrasadas,
                    SUM(CASE WHEN {cond_no} AND ca.expiration_date <= '{fecha_tope_s}' THEN {GASTOS} ELSE 0 END) AS vr_atrasadas,
                    SUM(CASE WHEN {cond_no} AND ca.expiration_date >  '{fecha_tope_s}' THEN 1 ELSE 0 END) AS cant_pendientes,
                    SUM(CASE WHEN {cond_no} AND ca.expiration_date >  '{fecha_tope_s}' THEN {GASTOS} ELSE 0 END) AS vr_pendientes,
                    SUM(CASE WHEN {cond_no} THEN 1 ELSE 0 END) AS cant_sin_pagar,
                    SUM(CASE WHEN {cond_no} THEN {GASTOS} ELSE 0 END) AS vr_sin_pagar,
                    GREATEST(DATEDIFF('{fecha_tope_s}', (
                        SELECT MIN(ca2.expiration_date) FROM contract_amortization ca2
                        WHERE ca2.contract_id = c.id
                          AND (ca2.contract_amortization_payment_status_id NOT IN (1,5)
                               OR ca2.payment_date IS NULL
                               OR ca2.payment_date > '{fecha_tope_ts}')
                          AND ca2.expiration_date <= '{fecha_tope_s}'
                    )), 0) AS dias_actual,
                    GREATEST(DATEDIFF('{prim_dia_s}', (
                        SELECT MIN(ca3.expiration_date) FROM contract_amortization ca3
                        WHERE ca3.contract_id = c.id
                          AND (ca3.contract_amortization_payment_status_id NOT IN (1,5)
                               OR ca3.payment_date IS NULL
                               OR ca3.payment_date > '{fecha_tope_ts}')
                          AND ca3.expiration_date <= '{fecha_tope_s}'
                    )), 0) AS dias_iniciales
                FROM contract c
                JOIN application a ON a.id = c.application_id
                JOIN customer cu ON cu.id = a.customer_id
                LEFT JOIN contract_amortization ca ON ca.contract_id = c.id
                WHERE c.id IN ({lista})
                GROUP BY c.id, c.contracts_status_id, cu.dni, cu.phone
            """)
            for r in sess.execute(sql):
                rows.append(dict(r._mapping))
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _query_pg_indicators(contracts: List[int], fecha_tope: date) -> pd.DataFrame:
    """Mejor gestion + intentos desde alocreditindicators.managements."""
    lista = ",".join(str(int(c)) for c in contracts)
    fecha_tope_ts = f"{fecha_tope.isoformat()} 23:59:59"
    sql = f"""
WITH RankedMgmt AS (
    SELECT contract_id, management_date, action, result, effect, promise_date, summary,
        ROW_NUMBER() OVER (
            PARTITION BY contract_id
            ORDER BY
                CASE LOWER(COALESCE(effect, ''))
                    WHEN 'pago_total' THEN 1
                    WHEN 'acuerdo_de_pago' THEN 2
                    WHEN 'promesa' THEN 3
                    WHEN 'promesa_de_pago' THEN 3
                    WHEN 'promesa_pago' THEN 3
                    WHEN 'pago_parcial' THEN 4
                    ELSE 99
                END,
                management_date DESC NULLS LAST
        ) AS rn
    FROM alocreditindicators.managements
    WHERE contract_id IN ({lista})
      AND management_date <= TIMESTAMP '{fecha_tope_ts}'
),
Intentos AS (
    SELECT contract_id, COUNT(*) AS intentos
    FROM alocreditindicators.managements
    WHERE contract_id IN ({lista})
      AND management_date <= TIMESTAMP '{fecha_tope_ts}'
    GROUP BY contract_id
)
SELECT
    COALESCE(rm.contract_id, it.contract_id) AS contrato,
    rm.management_date AS fecha_gestion,
    rm.action AS accion,
    rm.result AS resultado,
    rm.effect AS efecto,
    rm.effect AS mejor_gestion,
    rm.promise_date AS fecha_promesa,
    rm.summary AS motivo,
    it.intentos AS intentos
FROM (SELECT * FROM RankedMgmt WHERE rn = 1) rm
FULL OUTER JOIN Intentos it ON it.contract_id = rm.contract_id
"""
    conn = _connect_pg(_ind_cfg())
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Generacion del CSV por casa
# --------------------------------------------------------------------------- #
def _build_twist_rows_for_cycle(user_ids: List[int], house_key: str) -> "pd.DataFrame":
    """
    Filas Twist1/Twist2 para el CSV mensual, en formato OUTPUT_COLUMNS y con
    columna 'Producto'. Reutiliza los constructores de ReportServiceExtended
    (cedula/dias/rango/cliente); las columnas de gestion/pago quedan vacias.
    """
    rows: List[dict] = []
    try:
        from app.services.report_service_extended import ReportServiceExtended

        ext = ReportServiceExtended()
        is_cobyser = (house_key == "cobyser")
        df1 = ext._build_twist1_report_dataframe(ext.get_assigned_twist1_for_house(user_ids))
        df2 = ext._build_twist2_report_dataframe(ext.get_assigned_twist2_for_house(user_ids))
        for df in (df1, df2):
            if df is None or df.empty:
                continue
            for _, r in df.iterrows():
                rango = str(r.get("rango") or "")
                tipo = "Cédulas Impar" if (is_cobyser and rango in ("31_60", "31_45", "46_60")) else ""
                rows.append({
                    "Llave": r.get("llave"),
                    "Producto": r.get("producto"),
                    "Contrato": r.get("contrato_x"),
                    "Cedula": r.get("cedula"),
                    "Dias_iniciales_Mes": r.get("dias_iniciales_mes"),
                    "Dias_Actual": r.get("dias_iniciales_mes"),
                    RANGO_INICIAL: rango,
                    "Rango_Actual": rango,
                    "Tipo": tipo,
                    "Ingreso_Mes_Actual": 0,
                    "Pais": "CO",
                    "Telefono": r.get("telefono"),
                })
    except Exception as e:
        logger.error("cycle_end: error construyendo filas Twist: %s", e)

    if not rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    tdf = pd.DataFrame(rows)
    for col in OUTPUT_COLUMNS:
        if col not in tdf.columns:
            tdf[col] = ""
    return tdf[OUTPUT_COLUMNS]


def _load_merged_dataframe(
    contracts: List[int], prim_dia: date, fecha_tope: date, label: str
) -> pd.DataFrame:
    """Consulta PG prod (con respaldo MySQL), pagos e indicadores y los une por contrato."""
    df_prod = _query_pg_prod(contracts, prim_dia, fecha_tope)
    found = set(df_prod["contrato"].astype(int).tolist()) if not df_prod.empty else set()
    missing = [c for c in contracts if c not in found]
    logger.info("%s: PG-prod=%d, faltantes=%d", label, len(found), len(missing))

    if missing:
        df_my_basico = _query_mysql_fallback_basico(missing, prim_dia, fecha_tope)
        if not df_my_basico.empty:
            df_prod = pd.concat([df_prod, df_my_basico], ignore_index=True)

    df_pagos = _query_mysql_pagos(contracts, prim_dia, fecha_tope)
    df_ind = _query_pg_indicators(contracts, fecha_tope)

    return df_prod.merge(df_pagos, on="contrato", how="left").merge(df_ind, on="contrato", how="left")


def _populate_output_columns(merged: pd.DataFrame, house_key: str) -> None:
    """Construye in-place todas las columnas de OUTPUT_COLUMNS sobre `merged`."""
    merged["Llave"] = "PHONE" + merged["contrato"].astype(str)
    merged["Producto"] = "PHONE"
    merged["Contrato"] = merged["contrato"]
    merged["Cedula"] = merged.get("cedula")
    merged["estado_contrato"] = merged.get("estado_contrato")
    merged["cantidad_cuotas_atrasadas"] = merged.get("cant_atrasadas", 0).fillna(0).astype(int)
    merged["vr_pagos_atrasadas"] = merged.get("vr_atrasadas", 0).fillna(0)
    merged["Cuotas_pendientes"] = merged.get("cant_pendientes", 0).fillna(0).astype(int)
    merged["vr_cuotas_pendientes"] = merged.get("vr_pendientes", 0).fillna(0)
    merged["cantidad_cuotas_sin_pagar"] = merged.get("cant_sin_pagar", 0).fillna(0).astype(int)
    merged["vr_cuotas_sin_pagar"] = merged.get("vr_sin_pagar", 0).fillna(0)
    merged["cantidad_cuotas_pagados"] = merged.get("cant_pagadas", 0).fillna(0).astype(int)
    merged["Valor_cuotas_pagadas"] = merged.get("valor_pagos", 0).fillna(0)
    merged["Dias_iniciales_Mes"] = merged.get("dias_iniciales", 0).fillna(0).astype(int)
    merged["Dias_Actual"] = merged.get("dias_actual", 0).fillna(0).astype(int)

    def to_rango(d):
        d = int(d or 0)
        return get_assignment_dpd_range(d) or get_dpd_range(d) or "0"

    merged[RANGO_INICIAL] = merged["Dias_iniciales_Mes"].apply(to_rango)
    merged["Rango_Actual"] = merged["Dias_Actual"].apply(to_rango)

    # Columna "Tipo": franja Cobyser (dias 31-60) = "Cedulas Impar". Solo Cobyser;
    # se evalua por el rango inicial (buckets 31_45/46_60). Vacia en el resto.
    merged["Tipo"] = ""
    if house_key == "cobyser":
        es_franja = merged[RANGO_INICIAL].astype(str).isin({"31_45", "46_60", "31_60"})
        merged.loc[es_franja, "Tipo"] = "Cédulas Impar"

    merged["Ingreso_Mes_Actual"] = merged.get("monto_mes", 0).fillna(0)
    ultimo_pago = pd.to_datetime(merged.get("ultimo_pago_mes"), errors="coerce")
    merged["Fecha_ingreso_actual"] = ultimo_pago.dt.strftime("%Y-%m-%d")

    fecha_g = pd.to_datetime(merged.get("fecha_gestion"), errors="coerce")
    merged["Fecha_gestion"] = fecha_g.dt.strftime("%Y-%m-%d %H:%M:%S")
    merged["Accion"] = merged.get("accion")
    merged["Resultado"] = merged.get("resultado")
    merged["Efecto"] = merged.get("efecto")
    merged["Mejor_gestion"] = merged.get("mejor_gestion")
    fecha_p = pd.to_datetime(merged.get("fecha_promesa"), errors="coerce")
    merged["Fecha_promesa"] = fecha_p.dt.strftime("%Y-%m-%d")
    merged["MOTIVO"] = merged.get("motivo")
    merged["intentos"] = merged.get("intentos", 0).fillna(0).astype(int)
    merged["Pais"] = "CO"
    merged["Telefono"] = merged.get("telefono")


def generate_house_csv(
    house_key: str,
    prim_dia: date,
    fecha_tope: date,
    output_dir: Path,
) -> Dict:
    """Genera el CSV de 29 columnas para una casa. Retorna dict con ruta y stats."""
    label, user_ids = HOUSES[house_key]
    contracts = get_assigned_contracts(user_ids)
    logger.info("%s: %d contratos asignados hoy", label, len(contracts))

    if not contracts:
        raise RuntimeError(f"La casa {label} no tiene contratos asignados hoy")

    merged = _load_merged_dataframe(contracts, prim_dia, fecha_tope, label)

    _populate_output_columns(merged, house_key)

    final = merged[OUTPUT_COLUMNS].copy()

    # Anexar filas Twist1/Twist2 (mismo CSV, distinguidas por columna 'Producto').
    twist_rows = _build_twist_rows_for_cycle(user_ids, house_key)
    if not twist_rows.empty:
        final = pd.concat([final, twist_rows], ignore_index=True)
        logger.info("%s: +%d filas Twist en el CSV mensual", label, len(twist_rows))

    fecha_str = fecha_tope.strftime("%d-%m-%y")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"AloCredit-Phone-{fecha_str}_INFORME_{label}.csv"
    final.to_csv(output_path, index=False, encoding="utf-8-sig")

    ingreso_col = final["Ingreso_Mes_Actual"].astype(float)
    stats = {
        "house": label,
        "path": str(output_path),
        "filas": int(len(final)),
        "con_ingreso": int((ingreso_col > 0).sum()),
        "suma_ingreso": float(ingreso_col.sum()),
    }
    logger.info(
        "%s: CSV=%s filas=%d con_ingreso=%d suma=%.0f",
        label, output_path, stats["filas"], stats["con_ingreso"], stats["suma_ingreso"],
    )
    return stats


# --------------------------------------------------------------------------- #
# Diseno del correo
# --------------------------------------------------------------------------- #
def _fmt_money(value: float) -> str:
    return "${:,.0f}".format(value or 0)


def build_email_html(report_date: date, prim_dia: date, fecha_tope: date, house_stats: List[Dict]) -> str:
    mes = MESES_ES[report_date.month]
    anio = report_date.year
    total_filas = sum(s["filas"] for s in house_stats)
    total_con_ingreso = sum(s["con_ingreso"] for s in house_stats)
    total_ingreso = sum(s["suma_ingreso"] for s in house_stats)

    filas_html = ""
    for s in house_stats:
        filas_html += f"""
            <tr>
                <td style="padding:10px 14px;border-bottom:1px solid #eee;font-weight:600;color:#2c3e50;">{s['house']}</td>
                <td style="padding:10px 14px;border-bottom:1px solid #eee;text-align:right;">{s['filas']:,}</td>
                <td style="padding:10px 14px;border-bottom:1px solid #eee;text-align:right;">{s['con_ingreso']:,}</td>
                <td style="padding:10px 14px;border-bottom:1px solid #eee;text-align:right;">{_fmt_money(s['suma_ingreso'])}</td>
            </tr>"""

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: Arial, Helvetica, sans-serif; background:#f4f6f8; margin:0; padding:0; color:#2c3e50; }}
            .wrap {{ max-width:640px; margin:0 auto; background:#ffffff; }}
            .header {{ background:#1f3a5f; color:#ffffff; padding:28px 24px; text-align:center; }}
            .header h1 {{ margin:0; font-size:21px; }}
            .header .sub {{ margin-top:6px; font-size:14px; color:#cdd8e6; }}
            .content {{ padding:24px; font-size:14px; line-height:1.6; }}
            table.k {{ width:100%; border-collapse:collapse; margin:16px 0; font-size:13px; }}
            table.k th {{ background:#eef2f7; text-align:left; padding:10px 14px; color:#5a6b7b; font-size:12px; text-transform:uppercase; letter-spacing:.4px; }}
            table.k th.r, table.k td.r {{ text-align:right; }}
            .total {{ background:#1f3a5f; color:#fff; }}
            .total td {{ padding:12px 14px; font-weight:700; }}
            .meta {{ background:#f8fafc; border-left:4px solid #1f3a5f; padding:12px 16px; margin:16px 0; font-size:13px; color:#5a6b7b; }}
            .footer {{ text-align:center; padding:18px 24px; color:#9aa7b3; font-size:11px; border-top:1px solid #eee; }}
        </style>
    </head>
    <body>
        <div class="wrap">
            <div class="header">
                <h1>Informe finalizacion de ciclo &mdash; Casa de Cobranza</h1>
                <div class="sub">{mes} {anio}</div>
            </div>
            <div class="content">
                <p>Cordial saludo,</p>
                <p>Se adjunta el informe de <strong>finalizacion de ciclo</strong> de las casas de cobranza
                correspondiente al mes de <strong>{mes} {anio}</strong>, con los contratos que se encuentran
                actualmente asignados a cada casa.</p>

                <table class="k">
                    <tr>
                        <th>Casa</th>
                        <th class="r">Contratos</th>
                        <th class="r">Con ingreso del mes</th>
                        <th class="r">Ingreso del mes</th>
                    </tr>
                    {filas_html}
                    <tr class="total">
                        <td>Total</td>
                        <td class="r">{total_filas:,}</td>
                        <td class="r">{total_con_ingreso:,}</td>
                        <td class="r">{_fmt_money(total_ingreso)}</td>
                    </tr>
                </table>

                <div class="meta">
                    <strong>Periodo del informe:</strong> {prim_dia.strftime('%d/%m/%Y')} al {fecha_tope.strftime('%d/%m/%Y')}<br>
                    <strong>Contratos:</strong> los actualmente asignados a cada casa (Serlefin y Cobyser).<br>
                    <strong>Ingreso del mes:</strong> pagos aplicados dentro del periodo (fuente operativa).
                </div>

                <p>Los archivos adjuntos contienen el detalle por contrato de cada casa
                (formato CSV, una fila por contrato).</p>
            </div>
            <div class="footer">
                Correo automatico del Sistema de Asignacion de Cartera AloCredit. Por favor no responder.
            </div>
        </div>
    </body>
    </html>
    """


# --------------------------------------------------------------------------- #
# Orquestador: generar + enviar
# --------------------------------------------------------------------------- #
def generate_and_send_cycle_end_report(
    report_date: Optional[date] = None,
    recipient_to: Optional[List[str]] = None,
    recipient_cc: Optional[List[str]] = None,
    output_dir: Optional[Path] = None,
    send: bool = True,
) -> Dict:
    """Genera los CSV de ambas casas (mes de report_date) y los envia en un solo correo.

    Args:
        report_date: fecha que define el mes/ciclo (default: hoy).
        recipient_to: destinatarios principales (default: settings.MONTHLY_REPORT_TO).
        recipient_cc: copia (default: settings.MONTHLY_REPORT_CC).
        output_dir: directorio de salida (default: settings.REPORTS_DIR).
        send: si False, solo genera los CSV y no envia correo.
    """
    if report_date is None:
        report_date = date.today()
    prim_dia, fecha_tope = month_bounds(report_date)

    if recipient_to is None:
        recipient_to = settings.monthly_report_to_list
    if recipient_cc is None:
        recipient_cc = settings.monthly_report_cc_list
    if output_dir is None:
        output_dir = Path(settings.REPORTS_DIR)

    mes = MESES_ES[report_date.month]
    logger.info(
        "Generando informe finalizacion de ciclo %s %d (periodo %s a %s)",
        mes, report_date.year, prim_dia, fecha_tope,
    )

    house_stats: List[Dict] = []
    for house_key in ("serlefin", "cobyser"):
        house_stats.append(generate_house_csv(house_key, prim_dia, fecha_tope, output_dir))

    attachments = [s["path"] for s in house_stats]
    subject = f"Informe finalizacion de ciclo casa de cobranza {mes}"
    body = build_email_html(report_date, prim_dia, fecha_tope, house_stats)

    result = {
        "subject": subject,
        "report_date": report_date.isoformat(),
        "periodo": [prim_dia.isoformat(), fecha_tope.isoformat()],
        "houses": house_stats,
        "attachments": attachments,
        "recipient_to": recipient_to,
        "recipient_cc": recipient_cc,
        "sent": False,
    }

    if not send:
        logger.info("send=False: CSV generados sin enviar correo")
        return result

    sent = email_service.send_assignment_report(
        recipient=recipient_to,
        subject=subject,
        body=body,
        attachments=attachments,
        cc=recipient_cc,
    )
    result["sent"] = bool(sent)
    if sent:
        logger.info(
            "Informe %s enviado a %s (cc=%s)",
            mes, ", ".join(recipient_to), ", ".join(recipient_cc or []),
        )
    else:
        logger.error("Fallo el envio del informe de finalizacion de ciclo")
    return result
