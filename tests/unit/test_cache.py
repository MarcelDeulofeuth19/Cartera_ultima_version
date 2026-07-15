"""Pruebas unitarias del caché Redis tolerante a fallos (app/core/cache.py)."""
import pytest

from app.core.cache import RedisCache

pytestmark = pytest.mark.unit


class _FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None):
        self.store[key] = value

    def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)


class _BoomRedis:
    def get(self, key):
        raise RuntimeError("boom")

    def set(self, *a, **k):
        raise RuntimeError("boom")

    def delete(self, *a):
        raise RuntimeError("boom")


def test_cache_deshabilitado_es_noop(monkeypatch):
    monkeypatch.setattr("app.core.cache.settings.REDIS_ENABLED", False)
    c = RedisCache()
    assert c.available is False
    assert c.get_json("k") is None
    assert c.set_json("k", {"a": 1}) is False
    c.delete("k")  # no-op, no lanza


def test_cache_roundtrip(monkeypatch):
    monkeypatch.setattr("app.core.cache.settings.REDIS_ENABLED", True)
    c = RedisCache()
    c._client = _FakeRedis()  # inyecta cliente operativo
    assert c.available is True
    assert c.set_json("k", {"a": 1, "b": [1, 2]}, ttl_seconds=10) is True
    assert c.get_json("k") == {"a": 1, "b": [1, 2]}
    assert c.get_json("no-existe") is None
    c.delete("k")
    assert c.get_json("k") is None


def test_cache_errores_no_lanzan(monkeypatch):
    monkeypatch.setattr("app.core.cache.settings.REDIS_ENABLED", True)
    c = RedisCache()
    c._client = _BoomRedis()
    assert c.get_json("k") is None      # error -> None
    assert c.set_json("k", 1) is False  # error -> False
    c.delete("k")                       # error -> no lanza


def test_cache_delete_sin_claves(monkeypatch):
    monkeypatch.setattr("app.core.cache.settings.REDIS_ENABLED", True)
    c = RedisCache()
    c._client = _FakeRedis()
    c.delete()  # sin claves -> no-op
