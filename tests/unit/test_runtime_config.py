"""
Pruebas unitarias de la configuracion dinamica de asignacion.

- `AssignmentRuntimeConfig.serlefin_ratio`: conversion porcentaje -> ratio.
- `RuntimeConfigService._validate_payload` / `_collect_change`: validacion y
  auditoria de cambios. Son estaticos y puros (no se instancia el servicio,
  por lo que no se abre la base interna de configuracion).
"""
from datetime import datetime

import pytest

from app.core.config import settings
from app.runtime_config.service import (
    AssignmentRuntimeConfig,
    RuntimeConfigService,
)

pytestmark = pytest.mark.unit


def _config(serlefin_percent, cobyser_percent=None):
    return AssignmentRuntimeConfig(
        serlefin_percent=serlefin_percent,
        cobyser_percent=cobyser_percent
        if cobyser_percent is not None
        else 100 - serlefin_percent,
        min_days=61,
        max_days=240,
        updated_by="tester",
        updated_at=datetime(2026, 1, 1),
    )


@pytest.mark.parametrize(
    "percent, expected",
    [
        (60.0, 0.6),
        (40.0, 0.4),
        (100.0, 1.0),
        (0.0, 0.0),
        (-5.0, 0.0),  # negativo se trata como 0
    ],
)
def test_serlefin_ratio(percent, expected):
    assert _config(percent).serlefin_ratio == pytest.approx(expected)


def test_validate_payload_ok():
    # No debe lanzar con una configuracion valida.
    RuntimeConfigService._validate_payload(
        serlefin_percent=60,
        cobyser_percent=40,
        min_days=int(settings.DAYS_THRESHOLD),
        max_days=int(settings.DAYS_THRESHOLD) + 100,
    )


def test_validate_payload_suma_distinta_de_100():
    with pytest.raises(ValueError):
        RuntimeConfigService._validate_payload(
            serlefin_percent=70,
            cobyser_percent=40,
            min_days=int(settings.DAYS_THRESHOLD),
            max_days=int(settings.DAYS_THRESHOLD) + 10,
        )


def test_validate_payload_porcentaje_negativo():
    with pytest.raises(ValueError):
        RuntimeConfigService._validate_payload(
            serlefin_percent=-10,
            cobyser_percent=110,
            min_days=int(settings.DAYS_THRESHOLD),
            max_days=int(settings.DAYS_THRESHOLD) + 10,
        )


def test_validate_payload_min_days_por_debajo_del_umbral():
    with pytest.raises(ValueError):
        RuntimeConfigService._validate_payload(
            serlefin_percent=60,
            cobyser_percent=40,
            min_days=int(settings.DAYS_THRESHOLD) - 1,
            max_days=int(settings.DAYS_THRESHOLD) + 10,
        )


def test_validate_payload_max_menor_que_min():
    with pytest.raises(ValueError):
        RuntimeConfigService._validate_payload(
            serlefin_percent=60,
            cobyser_percent=40,
            min_days=int(settings.DAYS_THRESHOLD) + 20,
            max_days=int(settings.DAYS_THRESHOLD) + 10,
        )


def test_collect_change_detecta_diferencia():
    changes = RuntimeConfigService._collect_change("serlefin_percent", 60.0, 55.0)
    assert changes == [
        {"field": "serlefin_percent", "old": "60.0", "new": "55.0"}
    ]


def test_collect_change_sin_diferencia():
    assert RuntimeConfigService._collect_change("min_days", 61, 61) == []
