"""
Pruebas unitarias de helpers PUROS de los servicios de informe (sin BD):
report_service_extended y collection_agency_report_service.
"""
import types

import pytest

from app.services.report_service_extended import ReportServiceExtended
from app.services.collection_agency_report_service import CollectionAgencyReportService

pytestmark = pytest.mark.unit


# --- report_service_extended -----------------------------------------------
@pytest.mark.parametrize(
    "dias, expected",
    [(0, "0%"), (30, "4%"), (60, "4%"), (75, "6%"), (120, "8%"),
     (200, "11%"), (240, "13%"), (500, "15%")],
)
def test_comision_por_dias(dias, expected):
    assert ReportServiceExtended._comision_por_dias(dias) == expected


@pytest.mark.parametrize(
    "dias, fc, fg",
    [(100, 1.0, 0.60), (150, 1.0, 0.50), (170, 0.95, 0.40),
     (250, 0.90, 0.40), (400, 0.75, 0.0)],
)
def test_discount_factors(dias, fc, fg):
    assert ReportServiceExtended._discount_factors(dias) == (fc, fg)


def test_pagare_exclude_sql():
    sql = ReportServiceExtended._pagare_exclude_sql("c.pagare_status_id")
    assert "c.pagare_status_id" in sql and "NOT IN" in sql


def test_financial_report_row():
    fake = types.SimpleNamespace(
        _discount_factors=ReportServiceExtended._discount_factors,
        _comision_por_dias=ReportServiceExtended._comision_por_dias,
    )
    row = ReportServiceExtended._financial_report_row(
        fake, producto="PHONE", llave="PHONE1", contrato_x=1,
        capital=1_000_000, gastos=200_000, dias=100, cuotas=3, quota=50_000,
        cliente="Juan", cedula="123",
    )
    assert row["producto"] == "PHONE"
    assert row["capital_pendiente"] == 1_000_000.0
    assert row["deuda_actual"] == 1_200_000.0
    assert row["%_Pago_capital"] == "100%"       # factor capital 1.0 a 100 dias
    assert row["%_Descuento_gastos"] == "60%"    # factor gastos 0.60
    # vfd = 1_000_000*1.0 + 200_000*0.60 = 1_120_000
    assert row["valor_final_descuento"] == 1_120_000
    assert row["comision"] == "8%"


def test_financial_row_capital_negativo_se_normaliza():
    fake = types.SimpleNamespace(
        _discount_factors=ReportServiceExtended._discount_factors,
        _comision_por_dias=ReportServiceExtended._comision_por_dias,
    )
    row = ReportServiceExtended._financial_report_row(
        fake, producto="TWIST2", llave="T2", contrato_x="48",
        capital=-50, gastos=0, dias=40, cuotas=1, quota=0,
    )
    assert row["capital_pendiente"] == 0.0
    assert row["deuda_actual"] == 0.0


# --- collection_agency_report_service --------------------------------------
@pytest.mark.parametrize(
    "dias, fc, fg",
    [(100, 1.0, 0.60), (150, 1.0, 0.50), (170, 0.95, 0.40), (400, 0.75, 0.0)],
)
def test_ca_compute_factors(dias, fc, fg):
    assert CollectionAgencyReportService._compute_factors(dias) == (fc, fg)


def test_ca_compute_comision_es_porcentaje():
    for dias in (30, 75, 120, 200, 300):
        val = CollectionAgencyReportService._compute_comision(dias)
        assert isinstance(val, str) and val.endswith("%")


@pytest.mark.parametrize(
    "dias, exp",
    [(10, "1_30"), (45, "31_60"), (70, "61_90"), (120, "91_150"),
     (200, "151_210"), (211, "211"), (300, "Cartera Castigada"), (0, "0")],
)
def test_ca_compute_rango(dias, exp):
    assert CollectionAgencyReportService._compute_rango(dias) == exp


def test_ca_assign_payment_options():
    cols = {
        "valor_opcion_1": "op1",
        "valor_1_cuota_opcion_2": "o2_1",
        "valor_2_cuotas_opcion_2": "o2_2",
        "valor_3_cuotas_opcion_2": "o2_3",
        "valor_1_cuota_opcion_3": "o3_1",
        "valor_2_cuotas_opcion_3": "o3_2",
        "valor_3_cuotas_opcion_3": "o3_3",
        "valor_1_cuota_opcion_4": "o4_1",
        "valor_2_cuotas_opcion_4": "o4_2",
        "valor_3_cuotas_opcion_4": "o4_3",
    }
    r = {}
    CollectionAgencyReportService._assign_payment_options(
        r, cols, deuda=1_200_000, vfd=1_120_000, capital=1_000_000, quota=50_000
    )
    assert r["op1"] == 50_000
    assert r["o2_1"] == 1_200_000
    assert r["o2_2"] == 600_000
    assert r["o2_3"] == 400_000          # deuda > 600k -> /3
    assert r["o3_1"] == 1_120_000
    assert r["o4_1"] == 1_000_000


def test_ca_assign_payment_options_deuda_baja():
    cols = {"valor_3_cuotas_opcion_2": "o2_3", "valor_2_cuotas_opcion_2": "o2_2"}
    r = {}
    CollectionAgencyReportService._assign_payment_options(
        r, cols, deuda=100_000, vfd=0, capital=0, quota=0
    )
    assert r["o2_3"] is None            # deuda <= 600k -> None
    assert r["o2_2"] == 50_000


def test_ca_assign_descripciones():
    cols = {
        "descripcion_opcion_1": "d1",
        "descripcion_opcion_2": "d2",
        "descripcion_opcion_3": "d3",
        "descripcion_opcion_4": "d4",
    }
    r = {}
    CollectionAgencyReportService._assign_descripciones(r, cols)
    assert r["d1"].startswith("Pagar_1_cuota")
    assert r["d2"] == "Pagar_de_1_a_3_cuotas"
    assert "600k" in r["d3"] and "600k" in r["d4"]


def test_ca_mysql_missing_query_arma_sql():
    stmt = CollectionAgencyReportService._mysql_missing_query("1,2,3")
    assert "SELECT" in str(stmt)


# --- report_service_extended: más helpers puros ----------------------------
def test_twist2_balance_values():
    # bal = (cbs_id, capital, g1, g2, g3)
    assert ReportServiceExtended._twist2_balance_values((99, 1000, 100, 50, 25)) == (1000.0, 175.0)
    assert ReportServiceExtended._twist2_balance_values(None) == (0.0, 0.0)


def test_twist2_external_id():
    # cli[5] = external_id
    assert ReportServiceExtended._twist2_external_id((0, 0, 0, 0, 0, "48"), 999) == 48
    assert ReportServiceExtended._twist2_external_id((0, 0, 0, 0, 0, "abc"), 999) == 999
    assert ReportServiceExtended._twist2_external_id(None, 999) == 999


@pytest.mark.parametrize("v, exp", [("5", 5), (7, 7), (None, None), ("x", None), ("", None)])
def test_safe_int(v, exp):
    assert ReportServiceExtended._safe_int(v) == exp


@pytest.mark.parametrize(
    "dias, exp",
    [(0, "0%"), (30, "4%"), (75, "6%"), (120, "8%"), (200, "11%"), (211, "13%"), (300, "15%")],
)
def test_missing_comision(dias, exp):
    assert ReportServiceExtended._missing_comision(dias) == exp


@pytest.mark.parametrize(
    "dias, exp",
    [(10, "1_30"), (45, "31_60"), (75, "61_90"), (120, "91_150"),
     (200, "151_210"), (211, "211"), (300, "Cartera Castigada"), (0, "0")],
)
def test_missing_rango(dias, exp):
    assert ReportServiceExtended._missing_rango(dias) == exp


def test_compute_bucket_targets_suma_total():
    for total in (0, 1, 10, 13, 100):
        c, s = ReportServiceExtended._compute_bucket_targets(total, 0.6)
        assert c + s == total
        assert c >= 0 and s >= 0
