"""
Pruebas unitarias del file lock que garantiza una unica instancia del proceso.
"""
import pytest

from app.core import file_lock
from app.core.config import settings

pytestmark = pytest.mark.unit


def test_check_lock_status_shape_when_absent(monkeypatch):
    monkeypatch.setattr(settings, "LOCK_FILE", "no_existe_este_lock.lock")
    status = file_lock.check_lock_status()
    assert set(status.keys()) == {"lock_file", "exists", "is_locked"}
    assert status["exists"] is False
    assert status["is_locked"] is False


def test_acquire_and_release_lifecycle(monkeypatch):
    monkeypatch.setattr(settings, "LOCK_FILE", "test_unit_proceso.lock")

    assert file_lock.check_lock_status()["is_locked"] is False

    with file_lock.acquire_process_lock():
        # Mientras el lock esta tomado, el archivo existe
        assert file_lock.check_lock_status()["is_locked"] is True

    # Tras salir del contexto, el lock se libera y limpia
    assert file_lock.check_lock_status()["is_locked"] is False


def test_second_acquire_is_blocked(monkeypatch):
    monkeypatch.setattr(settings, "LOCK_FILE", "test_unit_proceso2.lock")

    with file_lock.acquire_process_lock():
        with pytest.raises(file_lock.ProcessLockError):
            with file_lock.acquire_process_lock():
                pass
