"""Cobertura de _build_mysql_row de CollectionAgencyReportService (puro, sin BD)."""
import pytest

from app.services.collection_agency_report_service import CollectionAgencyReportService

pytestmark = pytest.mark.unit

TARGET_COLUMNS = [
    "contrato_x", "llave", "producto", "cliente", "telefono", "correo", "cedula",
    "ciudad", "capital_pendiente", "gastos_vencidos", "deuda_actual",
    "dias_iniciales_mes", "Cuotas Atrasadas", "%_pago_capital", "%_descuento_gastos",
    "valor_final_descuento", "comision", "rango",
    "valor_opcion_1", "valor_1_cuota_opcion_2", "valor_2_cuotas_opcion_2",
    "descripcion_opcion_1",
]


def _svc():
    return CollectionAgencyReportService(postgres_session=None, mysql_session=None)


def test_build_mysql_row():
    cols_lower = {str(c).lower(): c for c in TARGET_COLUMNS}
    contrato_col = cols_lower.get("contrato_x", "contrato_x")
    row = (
        7, "PHONE7", "PHONE", "Juan Perez", "3001234567", "j@x.co", "123456",
        "Bogota", 1_000_000, 200_000, 1_200_000, 100, 3, 50_000,
    )
    r = _svc()._build_mysql_row(row, TARGET_COLUMNS, cols_lower, contrato_col)
    assert r[contrato_col] == 7
    assert r["capital_pendiente"] == 1_000_000.0
    assert r["deuda_actual"] == 1_200_000.0
    assert r["%_pago_capital"] == "100%"      # dias 100 -> factor capital 1.0
    assert r["%_descuento_gastos"] == "60%"   # factor gastos 0.60
    assert r["valor_final_descuento"] == 1_120_000
    assert r["rango"] == "91_150"
    assert r["comision"]  # asignada
    assert r["valor_1_cuota_opcion_2"] == 1_200_000
    assert r["descripcion_opcion_1"] == "Pagar_1_cuota__para_normalizar"


def test_build_mysql_row_valores_nulos():
    cols_lower = {str(c).lower(): c for c in TARGET_COLUMNS}
    contrato_col = cols_lower.get("contrato_x", "contrato_x")
    row = (
        9, "PHONE9", "PHONE", None, None, None, None, None,
        None, None, None, None, None, None,  # capital/gastos/dias/etc None
    )
    r = _svc()._build_mysql_row(row, TARGET_COLUMNS, cols_lower, contrato_col)
    assert r[contrato_col] == 9
    assert r["capital_pendiente"] == 0.0
    assert r["dias_iniciales_mes"] == 0
