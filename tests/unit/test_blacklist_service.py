"""Pruebas unitarias de BlacklistService (lista negra por archivo TXT)."""
import pytest

from app.services.blacklist_service import BlacklistService

pytestmark = pytest.mark.unit


def test_load_sin_archivo(tmp_path):
    svc = BlacklistService(str(tmp_path / "no-existe.txt"))
    assert svc.load_contract_ids() == set()
    st = svc.status()
    assert st["exists"] is False and st["contracts_loaded"] == 0


def test_parse_ignora_ceros_y_no_digitos():
    svc = BlacklistService("x.txt")
    assert svc._parse_contract_ids("1\n2\n2\nabc\n0\n") == {1, 2}
    assert svc._parse_contract_ids("") == set()


def test_save_y_recarga(tmp_path):
    f = tmp_path / "bl.txt"
    svc = BlacklistService(str(f))
    res = svc.save_from_text("10\n20\n20\n0\n")
    assert res["contracts_loaded"] == 2
    assert f.exists()
    assert svc.load_contract_ids() == {10, 20}
    # segunda carga usa cache por mtime (mismo resultado)
    assert svc.load_contract_ids() == {10, 20}
    # force_reload vuelve a leer
    assert svc.load_contract_ids(force_reload=True) == {10, 20}
    st = svc.status()
    assert st["exists"] is True and st["contracts_loaded"] == 2


def test_save_vacio_deja_archivo_sin_ids(tmp_path):
    f = tmp_path / "bl.txt"
    svc = BlacklistService(str(f))
    res = svc.save_from_text("sin numeros aqui")
    assert res["contracts_loaded"] == 0
    assert svc.load_contract_ids() == set()
