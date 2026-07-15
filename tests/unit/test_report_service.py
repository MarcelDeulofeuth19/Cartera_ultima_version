"""Pruebas unitarias de ReportService (generación de TXT en directorio temporal)."""
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit


def _service(tmp_path, monkeypatch):
    monkeypatch.setattr("app.services.report_service.settings.REPORTS_DIR", str(tmp_path))
    from app.services.report_service import ReportService
    return ReportService()


def test_generate_assignment_txt_files(tmp_path, monkeypatch):
    svc = _service(tmp_path, monkeypatch)
    paths = svc.generate_assignment_txt_files({45: [1, 2], 81: [3]}, {1: 70, 2: 80})
    assert "user_45" in paths and "user_81" in paths
    p45 = Path(paths["user_45"])
    assert p45.exists()
    content = p45.read_text(encoding="utf-8")
    assert "COBYSER" in content
    assert "Total de contratos: 2" in content
    # el contrato 1 aparece con sus días de atraso
    assert "70" in content
    p81 = Path(paths["user_81"])
    assert "SERLEFIN" in p81.read_text(encoding="utf-8")


def test_generate_txt_sin_dias(tmp_path, monkeypatch):
    svc = _service(tmp_path, monkeypatch)
    paths = svc.generate_assignment_txt_files({45: [9], 81: []})
    assert Path(paths["user_45"]).exists()
    # sin days_map -> N/A
    assert "N/A" in Path(paths["user_45"]).read_text(encoding="utf-8")


def test_ensure_reports_directory_crea(tmp_path, monkeypatch):
    target = tmp_path / "nuevo_dir"
    monkeypatch.setattr("app.services.report_service.settings.REPORTS_DIR", str(target))
    from app.services.report_service import ReportService
    ReportService()  # __init__ crea el directorio
    assert target.exists()
