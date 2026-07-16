"""Cobertura de métodos DataFrame de report_service_extended (sin BD)."""
import types

import pandas as pd
import pytest

from app.services.report_service_extended import ReportServiceExtended
from app.services.report_service_extended import report_service_extended as rse

pytestmark = pytest.mark.unit


def test_apply_tipo_franja_cobyser():
    df = pd.DataFrame({"rango": ["31_45", "61_90", "46_60"]})
    ReportServiceExtended._apply_tipo_franja(df, {"rango": "rango"}, user_id=45)
    assert list(df["Tipo"]) == ["Cédulas Impar", "", "Cédulas Impar"]


def test_apply_tipo_franja_serlefin_par():
    df = pd.DataFrame({"rango": ["31_45", "61_90", "46_60"]})
    ReportServiceExtended._apply_tipo_franja(df, {"rango": "rango"}, user_id=81)
    assert list(df["Tipo"]) == ["Cédulas Par", "", "Cédulas Par"]


def test_apply_contrato_fijo_sin_columna():
    df = pd.DataFrame({"x": [1, 2]})
    rse._apply_contrato_fijo(df, {}, user_id=45)  # sin 'contrato_x' -> todos 'NO'
    assert list(df["Contrato_Fijo"]) == ["NO", "NO"]


def test_apply_contrato_fijo_con_columna():
    df = pd.DataFrame({"Contrato": [999999991, 999999992]})
    rse._apply_contrato_fijo(df, {"contrato_x": "Contrato"}, user_id=45)
    # contratos improbables en la lista fija -> 'NO'
    assert set(df["Contrato_Fijo"]) <= {"SI", "NO"}


def test_finalize_twist_sheet_vacio():
    out = rse._finalize_twist_sheet(pd.DataFrame(), user_id=45)
    assert "NIT" in out.columns and out.empty


def test_finalize_twist_sheet_con_datos():
    tdf = pd.DataFrame({"rango": ["31_45", "61_90"], "contrato_x": [1, 2]})
    out = rse._finalize_twist_sheet(tdf, user_id=45)
    assert out.iloc[0]["NIT"] == "901546410-9"
    assert out.iloc[0]["Tipo"] == "Cédulas Impar"
    assert out.iloc[1]["Tipo"] == ""
    assert out.iloc[0]["Contrato_Fijo"] == "NO"


def test_finalize_twist_sheet_serlefin_par():
    tdf = pd.DataFrame({"rango": ["31_45", "61_90"], "contrato_x": [1, 2]})
    out = rse._finalize_twist_sheet(tdf, user_id=81)
    assert out.iloc[0]["Tipo"] == "Cédulas Par"   # franja par -> Serlefin
    assert out.iloc[1]["Tipo"] == ""


def test_drop_serlefin_franja():
    df = pd.DataFrame({"rango": ["31_45", "61_90", "46_60", "91_120"]})
    t1 = pd.DataFrame({"rango": ["31_60", "61_90"]})
    t2 = pd.DataFrame()  # vacío
    out, ot1, ot2 = ReportServiceExtended._drop_serlefin_franja(df, t1, t2)
    assert list(out["rango"]) == ["61_90", "91_120"]   # franja eliminada
    assert list(ot1["rango"]) == ["61_90"]
