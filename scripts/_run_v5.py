"""CSV snapshot al 30/04/2026 - pagos vienen de MySQL (PG-prod esta stale).

Cambios vs v3:
- Pagos (Ingreso_Mes_Actual, Valor_cuotas_pagadas, cantidad_cuotas_pagados,
  Fecha_ingreso_actual) se sobrescriben con datos de MySQL para TODOS los
  contratos (PG-prod no tiene pagos posteriores a 2025-12).
"""
# --- bootstrap: ejecutable desde cualquier ruta (anade la raiz del repo al path) ---
import sys as _sys, pathlib as _pathlib
for _cand in (_pathlib.Path(__file__).resolve(), *_pathlib.Path(__file__).resolve().parents):
    if (_cand / "app").is_dir() and (_cand / "main.py").exists():
        _sys.path.insert(0, str(_cand)); break
# --- fin bootstrap ---
import sys
import time
import logging
from pathlib import Path
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

if len(sys.argv) < 2 or sys.argv[1] not in ("serlefin", "cobyser"):
    print("Uso: python _run_v4.py <serlefin|cobyser>")
    sys.exit(2)

NAME = sys.argv[1]
LABEL = NAME.capitalize()
logger = logging.getLogger(NAME)

sys.path.insert(0, "/app")

import pandas as pd
import psycopg2

from app.core.config import settings
from app.core.dpd import get_assignment_dpd_range, get_dpd_range
from app.database.connections import db_manager
from sqlalchemy import text

CONTRACTS_FILE = Path(f"/tmp/{NAME}_contracts.txt")
OUTPUT_FILE = Path(f"/app/AloCredit-Phone-31-05-26_INFORME_{LABEL}.csv")

FECHA_TOPE = "2026-05-31"
FECHA_TOPE_TS = f"{FECHA_TOPE} 23:59:59"
PRIM_DIA = "2026-05-01"

COND_NO_PAGADA_AL_TOPE = (
    f"(contract_amortization_payment_status_id NOT IN (1,5) "
    f"OR payment_date IS NULL "
    f"OR payment_date > TIMESTAMP '{FECHA_TOPE_TS}')"
)
COND_PAGADA_AL_TOPE = (
    f"(contract_amortization_payment_status_id IN (1,5) "
    f"AND payment_date IS NOT NULL "
    f"AND payment_date <= TIMESTAMP '{FECHA_TOPE_TS}')"
)
MY_COND_NO_PAGADA = (
    f"(ca.contract_amortization_payment_status_id NOT IN (1,5) "
    f"OR ca.payment_date IS NULL "
    f"OR ca.payment_date > '{FECHA_TOPE_TS}')"
)
MY_COND_PAGADA = (
    f"(ca.contract_amortization_payment_status_id IN (1,5) "
    f"AND ca.payment_date IS NOT NULL "
    f"AND ca.payment_date <= '{FECHA_TOPE_TS}')"
)

OUTPUT_COLUMNS = [
    "Llave", "Producto", "Contrato", "Cedula", "estado_contrato",
    "cantidad_cuotas_atrasadas", "vr_pagos_atrasadas",
    "Cuotas_pendientes", "vr_cuotas_pendientes",
    "cantidad_cuotas_sin_pagar", "vr_cuotas_sin_pagar",
    "cantidad_cuotas_pagados", "Valor_cuotas_pagadas",
    "Dias_iniciales_Mes", "Rango Inicial",
    "Dias_Actual", "Rango_Actual",
    "Ingreso_Mes_Actual", "Fecha_ingreso_actual",
    "MOTIVO", "Pais", "intentos",
    "Fecha_gestion", "Accion", "Resultado", "Efecto", "Mejor_gestion",
    "Fecha_promesa", "Telefono",
]


def connect_pg(cfg):
    return psycopg2.connect(
        host=cfg["host"], user=cfg["user"], password=cfg["password"],
        dbname=cfg["database"], port=cfg["port"], options=cfg["options"],
    )


def query_pg_prod(contracts):
    cfg = {
        "host": settings.REPORTS_EXT_PROD_HOST, "user": settings.REPORTS_EXT_PROD_USER,
        "password": settings.REPORTS_EXT_PROD_PASSWORD, "database": settings.REPORTS_EXT_PROD_DATABASE,
        "port": settings.REPORTS_EXT_PROD_PORT,
        "options": f"-csearch_path={settings.REPORTS_EXT_PROD_SCHEMA}",
    }
    lista = ",".join(str(int(c)) for c in contracts)
    GASTOS = ("COALESCE(outstanding_principal,0) + COALESCE(interest_payment,0) + "
              "COALESCE(endorsement,0) + COALESCE(vat,0) + COALESCE(seguro_vida,0) + "
              "COALESCE(seguro,0) + COALESCE(digital_sign,0) + COALESCE(digital_sign_iva,0)")
    sql = f"""
WITH
CuotasAtrasadas AS (
    SELECT contract_id, COUNT(*) AS cant, SUM({GASTOS}) AS valor
    FROM contract_amortization
    WHERE contract_id IN ({lista})
      AND {COND_NO_PAGADA_AL_TOPE}
      AND expiration_date <= DATE '{FECHA_TOPE}'
    GROUP BY contract_id
),
CuotasPendientes AS (
    SELECT contract_id, COUNT(*) AS cant, SUM({GASTOS}) AS valor
    FROM contract_amortization
    WHERE contract_id IN ({lista})
      AND {COND_NO_PAGADA_AL_TOPE}
      AND expiration_date > DATE '{FECHA_TOPE}'
    GROUP BY contract_id
),
CuotasSinPagar AS (
    SELECT contract_id, COUNT(*) AS cant, SUM({GASTOS}) AS valor
    FROM contract_amortization
    WHERE contract_id IN ({lista})
      AND {COND_NO_PAGADA_AL_TOPE}
    GROUP BY contract_id
),
DiasIniciales AS (
    SELECT c.id AS contract_id,
        COALESCE(GREATEST((DATE '{PRIM_DIA}' - MIN(ca.expiration_date)::date), 0), 0)::int AS dias
    FROM contract c
    LEFT JOIN contract_amortization ca
      ON ca.contract_id = c.id
     AND (ca.contract_amortization_payment_status_id NOT IN (1,5)
          OR ca.payment_date IS NULL
          OR ca.payment_date > TIMESTAMP '{FECHA_TOPE_TS}')
     AND ca.expiration_date <= DATE '{FECHA_TOPE}'
    WHERE c.id IN ({lista})
    GROUP BY c.id
),
DiasActual AS (
    SELECT c.id AS contract_id,
        COALESCE(GREATEST((DATE '{FECHA_TOPE}' - MIN(ca.expiration_date)::date), 0), 0)::int AS dias
    FROM contract c
    LEFT JOIN contract_amortization ca
      ON ca.contract_id = c.id
     AND (ca.contract_amortization_payment_status_id NOT IN (1,5)
          OR ca.payment_date IS NULL
          OR ca.payment_date > TIMESTAMP '{FECHA_TOPE_TS}')
     AND ca.expiration_date <= DATE '{FECHA_TOPE}'
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
    conn = connect_pg(cfg)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df


def query_mysql_pagos(contracts):
    """Pagos para TODOS los contratos (MySQL es la fuente de verdad)."""
    rows = []
    with db_manager.get_mysql_session() as sess:
        for i in range(0, len(contracts), 1000):
            chunk = contracts[i:i+1000]
            lista = ",".join(str(int(c)) for c in chunk)
            sql = text(f"""
                SELECT c.id AS contrato,
                    SUM(CASE WHEN {MY_COND_PAGADA} THEN 1 ELSE 0 END) AS cant_pagadas,
                    SUM(CASE WHEN {MY_COND_PAGADA} THEN COALESCE(ca.amount_payed, ca.fee, 0) ELSE 0 END) AS valor_pagos,
                    SUM(CASE WHEN ca.contract_amortization_payment_status_id IN (1,5)
                              AND ca.payment_date IS NOT NULL
                              AND ca.payment_date >= '{PRIM_DIA} 00:00:00'
                              AND ca.payment_date <= '{FECHA_TOPE_TS}'
                          THEN COALESCE(ca.amount_payed, ca.fee, 0) ELSE 0 END) AS monto_mes,
                    (SELECT MAX(ca4.payment_date) FROM contract_amortization ca4
                       WHERE ca4.contract_id = c.id
                         AND ca4.contract_amortization_payment_status_id IN (1,5)
                         AND ca4.payment_date IS NOT NULL
                         AND ca4.payment_date >= '{PRIM_DIA} 00:00:00'
                         AND ca4.payment_date <= '{FECHA_TOPE_TS}') AS ultimo_pago_mes
                FROM contract c
                LEFT JOIN contract_amortization ca ON ca.contract_id = c.id
                WHERE c.id IN ({lista})
                GROUP BY c.id
            """)
            for r in sess.execute(sql):
                rows.append(dict(r._mapping))
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["contrato","cant_pagadas","valor_pagos","monto_mes","ultimo_pago_mes"])


def query_mysql_fallback_basico(missing_ids):
    """Para contratos que no estan en PG-prod: datos basicos de cuotas/dias."""
    if not missing_ids:
        return pd.DataFrame()
    GASTOS = ("COALESCE(ca.outstanding_principal,0) + COALESCE(ca.interest_payment,0) + "
              "COALESCE(ca.endorsement,0) + COALESCE(ca.vat,0) + COALESCE(ca.seguro_vida,0) + "
              "COALESCE(ca.seguro,0) + COALESCE(ca.digital_sign,0) + COALESCE(ca.digital_sign_iva,0)")
    rows = []
    with db_manager.get_mysql_session() as sess:
        for i in range(0, len(missing_ids), 1000):
            chunk = missing_ids[i:i+1000]
            lista = ",".join(str(int(c)) for c in chunk)
            sql = text(f"""
                SELECT c.id AS contrato,
                    c.contracts_status_id AS estado_contrato,
                    cu.dni AS cedula, cu.phone AS telefono,
                    SUM(CASE WHEN {MY_COND_NO_PAGADA} AND ca.expiration_date <= '{FECHA_TOPE}' THEN 1 ELSE 0 END) AS cant_atrasadas,
                    SUM(CASE WHEN {MY_COND_NO_PAGADA} AND ca.expiration_date <= '{FECHA_TOPE}' THEN {GASTOS} ELSE 0 END) AS vr_atrasadas,
                    SUM(CASE WHEN {MY_COND_NO_PAGADA} AND ca.expiration_date >  '{FECHA_TOPE}' THEN 1 ELSE 0 END) AS cant_pendientes,
                    SUM(CASE WHEN {MY_COND_NO_PAGADA} AND ca.expiration_date >  '{FECHA_TOPE}' THEN {GASTOS} ELSE 0 END) AS vr_pendientes,
                    SUM(CASE WHEN {MY_COND_NO_PAGADA} THEN 1 ELSE 0 END) AS cant_sin_pagar,
                    SUM(CASE WHEN {MY_COND_NO_PAGADA} THEN {GASTOS} ELSE 0 END) AS vr_sin_pagar,
                    GREATEST(DATEDIFF('{FECHA_TOPE}', (
                        SELECT MIN(ca2.expiration_date) FROM contract_amortization ca2
                        WHERE ca2.contract_id = c.id
                          AND (ca2.contract_amortization_payment_status_id NOT IN (1,5)
                               OR ca2.payment_date IS NULL
                               OR ca2.payment_date > '{FECHA_TOPE_TS}')
                          AND ca2.expiration_date <= '{FECHA_TOPE}'
                    )), 0) AS dias_actual,
                    GREATEST(DATEDIFF('{PRIM_DIA}', (
                        SELECT MIN(ca3.expiration_date) FROM contract_amortization ca3
                        WHERE ca3.contract_id = c.id
                          AND (ca3.contract_amortization_payment_status_id NOT IN (1,5)
                               OR ca3.payment_date IS NULL
                               OR ca3.payment_date > '{FECHA_TOPE_TS}')
                          AND ca3.expiration_date <= '{FECHA_TOPE}'
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


def query_pg_indicators(contracts):
    cfg = {
        "host": settings.REPORTS_EXT_IND_HOST, "user": settings.REPORTS_EXT_IND_USER,
        "password": settings.REPORTS_EXT_IND_PASSWORD, "database": settings.REPORTS_EXT_IND_DATABASE,
        "port": settings.REPORTS_EXT_IND_PORT,
        "options": f"-csearch_path={settings.REPORTS_EXT_IND_SCHEMA}",
    }
    lista = ",".join(str(int(c)) for c in contracts)
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
      AND management_date <= TIMESTAMP '{FECHA_TOPE_TS}'
),
Intentos AS (
    SELECT contract_id, COUNT(*) AS intentos
    FROM alocreditindicators.managements
    WHERE contract_id IN ({lista})
      AND management_date <= TIMESTAMP '{FECHA_TOPE_TS}'
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
    conn = connect_pg(cfg)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df


def main():
    with CONTRACTS_FILE.open() as f:
        original_list = [int(line.strip()) for line in f if line.strip()]
    unique = sorted(set(original_list))
    logger.info("%s lineas=%d unicos=%d", LABEL, len(original_list), len(unique))

    t0 = time.time()
    df_prod = query_pg_prod(unique)
    logger.info("PG-prod base: %d filas (%.1fs)", len(df_prod), time.time() - t0)

    found = set(df_prod["contrato"].astype(int).tolist()) if not df_prod.empty else set()
    missing = [c for c in unique if c not in found]
    logger.info("Faltantes en PG-prod: %d", len(missing))

    if missing:
        t1 = time.time()
        df_my_basico = query_mysql_fallback_basico(missing)
        logger.info("MySQL fallback basico: %d filas (%.1fs)", len(df_my_basico), time.time() - t1)
        if not df_my_basico.empty:
            df_prod = pd.concat([df_prod, df_my_basico], ignore_index=True)

    # PAGOS desde MySQL para TODOS (PG-prod stale)
    t2 = time.time()
    df_pagos = query_mysql_pagos(unique)
    logger.info("MySQL pagos (todos): %d filas (%.1fs)", len(df_pagos), time.time() - t2)

    # Gestion
    t3 = time.time()
    df_ind = query_pg_indicators(unique)
    logger.info("PG-indicators: %d filas (%.1fs)", len(df_ind), time.time() - t3)

    merged = df_prod.merge(df_pagos, on="contrato", how="left").merge(df_ind, on="contrato", how="left")

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

    merged["Rango Inicial"] = merged["Dias_iniciales_Mes"].apply(to_rango)
    merged["Rango_Actual"] = merged["Dias_Actual"].apply(to_rango)

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

    final_unique = merged[OUTPUT_COLUMNS].copy()
    driver = pd.DataFrame({"Contrato": original_list})
    final = driver.merge(final_unique, on="Contrato", how="left")
    final = final[OUTPUT_COLUMNS]
    final.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    con_monto = (final["Ingreso_Mes_Actual"].astype(float) > 0).sum()
    suma_monto = float(final["Ingreso_Mes_Actual"].astype(float).sum())
    unique_con_monto = final[final["Ingreso_Mes_Actual"].astype(float) > 0]["Contrato"].nunique()
    logger.info("CSV: %s filas=%d", OUTPUT_FILE, len(final))
    logger.info("Ingreso_Mes_Actual: filas con monto>0 = %d (contratos unicos = %d) | suma = $%.0f",
                con_monto, unique_con_monto, suma_monto)
    print(f"OK file={OUTPUT_FILE} rows={len(final)} con_monto={con_monto} unicos={unique_con_monto} suma=${suma_monto:.0f}")


if __name__ == "__main__":
    main()
