"""
Pruebas unitarias de la verificacion de firma HMAC de la API.

Se construye un `Request` falso minimo (sin levantar servidor) y se ejercita
`verify_hmac_signature` para los tres caminos: firma valida, ausente e invalida.
"""
import asyncio
import hashlib
import hmac

import pytest
from fastapi import HTTPException

from app.api.dependencies.security import verify_hmac_signature
from app.core.config import settings

pytestmark = pytest.mark.unit


class _FakeURL:
    def __init__(self, path: str):
        self.path = path


class _FakeRequest:
    """Stub minimo compatible con lo que usa verify_hmac_signature."""

    def __init__(self, method: str, path: str, body: bytes, headers: dict):
        self.method = method
        self.url = _FakeURL(path)
        self._body = body
        self.headers = headers

    async def body(self) -> bytes:
        return self._body


def _valid_signature(method: str, path: str, body: bytes) -> str:
    payload = method.encode() + path.encode() + body
    return hmac.new(
        settings.API_HMAC_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()


def test_valid_signature_passes():
    method, path, body = "POST", "/api/v1/run-assignment", b""
    sig = _valid_signature(method, path, body)
    req = _FakeRequest(method, path, body, {"X-Signature": sig})
    # No debe lanzar excepcion; retorna None implicito
    assert asyncio.run(verify_hmac_signature(req)) is None


def test_missing_header_raises_401():
    req = _FakeRequest("GET", "/api/v1/health", b"", headers={})
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(verify_hmac_signature(req))
    assert exc_info.value.status_code == 401


def test_invalid_signature_raises_403():
    req = _FakeRequest(
        "POST", "/api/v1/run-assignment", b"", {"X-Signature": "deadbeef"}
    )
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(verify_hmac_signature(req))
    assert exc_info.value.status_code == 403


def test_signature_bound_to_body():
    method, path = "POST", "/api/v1/run-assignment"
    sig = _valid_signature(method, path, b"original")
    # Mismo path/method pero body distinto => firma invalida
    req = _FakeRequest(method, path, b"tampered", {"X-Signature": sig})
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(verify_hmac_signature(req))
    assert exc_info.value.status_code == 403
