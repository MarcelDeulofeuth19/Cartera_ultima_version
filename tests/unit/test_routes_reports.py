"""
Cobertura de funciones del router reports.py sin BD: descarga por casa
(excel/json) y _build_json_response. (El módulo reports importa db_manager solo
dentro de funciones, así que importarlo no conecta.)
"""
import asyncio

import pandas as pd
import pytest
from fastapi import BackgroundTasks, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from app.api.routes import reports as reports_mod

pytestmark = pytest.mark.unit


def _make_xlsx(path):
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame([{"contrato_x": 1, "producto": "PHONE"}]).to_excel(w, sheet_name="Phone", index=False)
        pd.DataFrame([{"contrato_x": 2, "producto": "TWIST1"}]).to_excel(w, sheet_name="Twist1", index=False)
        pd.DataFrame([{"contrato_x": 3, "producto": "TWIST2"}]).to_excel(w, sheet_name="Twist2", index=False)


def _prep(tmp_path, monkeypatch):
    monkeypatch.setattr("app.api.routes.reports.settings.REPORTS_DIR", str(tmp_path))
    f = tmp_path / "2026-07-15_INFORME_Cobyser.xlsx"
    _make_xlsx(f)
    return f


def test_download_report_house_invalido():
    with pytest.raises(HTTPException) as ei:
        asyncio.run(reports_mod.download_report("otra", BackgroundTasks(), format="excel"))
    assert ei.value.status_code == 400


def test_download_report_excel(tmp_path, monkeypatch):
    _prep(tmp_path, monkeypatch)
    resp = asyncio.run(reports_mod.download_report("cobyser", BackgroundTasks(), format="excel"))
    assert isinstance(resp, FileResponse)


def test_download_report_json_phone(tmp_path, monkeypatch):
    _prep(tmp_path, monkeypatch)
    resp = asyncio.run(
        reports_mod.download_report("cobyser", BackgroundTasks(), format="json", product="phone")
    )
    assert isinstance(resp, JSONResponse)


def test_build_json_response_all(tmp_path):
    f = tmp_path / "r.xlsx"
    _make_xlsx(f)
    resp = reports_mod._build_json_response(f, "all")
    assert isinstance(resp, JSONResponse)


def test_build_json_response_twist1(tmp_path):
    f = tmp_path / "r.xlsx"
    _make_xlsx(f)
    resp = reports_mod._build_json_response(f, "twist1")
    assert isinstance(resp, JSONResponse)


def test_build_json_response_producto_invalido(tmp_path):
    f = tmp_path / "r.xlsx"
    _make_xlsx(f)
    resp = reports_mod._build_json_response(f, "no-existe")
    assert resp.status_code == 400
