"""Cobertura adicional: clean_assignments, _build_missing_contract_row y
helpers de reescritura de .env de RuntimeConfigService (todo puro)."""
import types

import pytest

from app.services.assignment_service import AssignmentService
from app.services.report_service_extended import report_service_extended as rse
from app.runtime_config.service import RuntimeConfigService

pytestmark = pytest.mark.unit


def test_clean_assignments_append_only_no_borra():
    stats = AssignmentService.clean_assignments(types.SimpleNamespace(), 240)
    assert stats["deleted_total"] == 0
    assert stats["days_threshold_applied"] == 240


def test_build_missing_contract_row():
    base_cols = {
        "contrato": "Contrato", "llave": "Llave", "producto": "Producto",
        "cliente": "Cliente", "telefono": "Tel", "correo": "Correo",
        "cedula": "Cedula", "ciudad": "Ciudad", "capital": "Capital",
        "gastos": "Gastos", "deuda": "Deuda", "dias": "Dias", "cuotas": "Cuotas",
    }
    cols_by_lower = {
        "comision": "Comision", "rango": "Rango",
        "%_pago_capital": "PagoCap", "%_descuento_gastos": "DescGastos",
        "valor_final_descuento": "VFD", "valor_opcion_1": "Op1",
    }
    target_columns = list(base_cols.values()) + list(cols_by_lower.values())
    row = (1, "L1", "PHONE", "cli", "tel", "corr", "ced", "ciu",
           1_000_000, 200_000, 1_200_000, 100, 3, 50_000)
    r = rse._build_missing_contract_row(row, target_columns, cols_by_lower, base_cols)
    assert r["Contrato"] == 1
    assert r["Dias"] == 100
    assert r["Comision"] == "8%"        # 91-150
    assert r["Rango"] == "91_150"
    assert r["PagoCap"] == "100%"


def test_rewrite_existing_lines_y_append():
    import re
    key_pat = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")
    lines = ["A=1", "# comentario", "B=2"]
    values = {"B": "9", "C": "3"}
    new_lines = []
    pending = set(values)
    changed = RuntimeConfigService._rewrite_existing_lines(
        lines=lines, values=values, key_pattern=key_pat,
        new_lines=new_lines, pending_keys=pending,
    )
    assert changed is True                 # B cambió de 2 a 9
    assert "B=9" in new_lines and "A=1" in new_lines
    assert pending == {"C"}                # C queda pendiente (no existía)

    RuntimeConfigService._append_pending_keys(
        new_lines=new_lines, pending_keys={"C"}, values=values
    )
    assert "C=3" in new_lines
