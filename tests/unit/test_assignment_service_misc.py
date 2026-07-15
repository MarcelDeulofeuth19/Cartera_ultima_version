"""
Cobertura de helpers de carga/config de AssignmentService (sin BD):
listas negras y carga de configuración dinámica con fallback.
"""
import types

import pytest

from app.services.assignment_service import AssignmentService

pytestmark = pytest.mark.unit


def test_load_customer_document_blacklist():
    docs = AssignmentService._load_customer_document_blacklist(types.SimpleNamespace())
    assert isinstance(docs, set)
    # solo dígitos, sin vacíos
    assert all(d.isdigit() for d in docs)


def test_load_contract_blacklist_deshabilitada(monkeypatch):
    monkeypatch.setattr("app.services.assignment_service.settings.BLACKLIST_ENABLED", False)
    assert AssignmentService._load_contract_blacklist(types.SimpleNamespace()) == set()


def test_resolve_blocked_por_documentos_vacio():
    fake = types.SimpleNamespace(contract_service=None)
    assert AssignmentService._resolve_blocked_contract_ids_by_documents(fake, set()) == set()


def test_resolve_blocked_sin_mysql():
    fake = types.SimpleNamespace(contract_service=None)
    assert AssignmentService._resolve_blocked_contract_ids_by_documents(fake, {"123"}) == set()


def test_load_runtime_config_fallback_usa_defaults():
    from app.core.config import settings

    class _Boom:
        def get_assignment_config(self):
            raise RuntimeError("sin BD interna")

    fake = types.SimpleNamespace(runtime_config_service=_Boom())
    cfg = AssignmentService._load_runtime_assignment_config(fake)
    assert cfg.serlefin_percent == float(settings.DEFAULT_SERLEFIN_PERCENT)
    assert cfg.cobyser_percent == float(settings.DEFAULT_COBYSER_PERCENT)
    assert cfg.min_days == int(settings.DEFAULT_ASSIGNMENT_MIN_DAYS)
