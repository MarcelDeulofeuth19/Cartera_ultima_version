"""Pruebas del archivo JSON de asignaciones en la raíz (servicio externo)."""
import json

import pytest

pytestmark = pytest.mark.unit


def test_persist_assignments_json_crea_archivo(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # el archivo se escribe relativo al CWD (raíz)
    from app.api.routes import reports

    reports.persist_assignments_json("2026-07-15", {"success": True, "data": {}}, force=True)
    p = tmp_path / "asignaciones_cache_v2_2026-07-15.json"
    assert p.exists()
    assert json.loads(p.read_text(encoding="utf-8"))["success"] is True


def test_persist_assignments_json_force_false_no_sobrescribe(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from app.api.routes import reports

    reports.persist_assignments_json("2026-07-15", {"v": 1}, force=True)
    reports.persist_assignments_json("2026-07-15", {"v": 2}, force=False)  # ya existe -> no toca
    p = tmp_path / "asignaciones_cache_v2_2026-07-15.json"
    assert json.loads(p.read_text(encoding="utf-8"))["v"] == 1


def test_assignments_json_path():
    from app.api.routes import reports

    assert reports.assignments_json_path("2026-07-15").name == "asignaciones_cache_v2_2026-07-15.json"
