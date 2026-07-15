"""Pruebas de métricas/HTML y valores calculados de report_service_extended (puro)."""
import pytest

from app.services.report_service_extended import report_service_extended as rse

pytestmark = pytest.mark.unit


def _metrics():
    return {
        "serlefin": 60, "cobyser": 40, "total": 100,
        "serlefin_percent": 60.0, "cobyser_percent": 40.0,
        "cumple_60_40": True,
        "bucket_distribution": [
            {"bucket": "61_90", "total": 10, "serlefin_assigned": 6, "cobyser_assigned": 4,
             "serlefin_target": 6, "cobyser_target": 4},
        ],
    }


@pytest.mark.parametrize("audience", ["general", "serlefin", "cobyser"])
def test_generate_metrics_html(audience):
    html = rse.generate_metrics_html(_metrics(), audience)
    assert isinstance(html, str) and len(html) > 50
    if audience in ("general", "serlefin"):
        assert "Serlefin" in html
    if audience in ("general", "cobyser"):
        assert "Cobyser" in html


def test_generate_metrics_html_general_incluye_buckets():
    html = rse.generate_metrics_html(_metrics(), "general")
    assert "Bucket" in html or "bucket" in html
    assert "Cumplimiento" in html


def test_metrics_general_section_no_cumple():
    m = _metrics()
    m["cumple_60_40"] = False
    m["bucket_distribution"] = []
    html = rse._metrics_general_section(m)
    assert "NO CUMPLE" in html


def test_missing_computed_values():
    r = rse._missing_computed_values(
        1_000_000, 200_000, 1_200_000, 50_000, 1_120_000, 1.0, 0.6, "8%", "61_90"
    )
    assert r["comision"] == "8%"
    assert r["rango"] == "61_90"
    assert r["%_pago_capital"] == "100%"
    assert r["%_descuento_gastos"] == "60%"
    assert r["valor_1_cuota_opcion_2"] == 1_200_000
    assert r["valor_3_cuotas_opcion_2"] == round(1_200_000 / 3)  # deuda > 600k
