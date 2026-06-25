"""Ejecucion manual del informe de finalizacion de ciclo (casa de cobranza).

Genera los CSV de ambas casas (Serlefin + Cobyser) con los contratos
actualmente asignados y los envia en un solo correo.

Ejemplos:
    # Prueba: enviar SOLO a mdeulofeuth@alocredit.co (sin CC), mes actual
    python run_cycle_end_report.py --test

    # Real: To=jcarrasco (default), CC=mdeulofeuth (default), mes actual
    python run_cycle_end_report.py

    # Mes especifico
    python run_cycle_end_report.py --month 2026-05

    # Solo generar CSV, sin enviar
    python run_cycle_end_report.py --no-send
"""
# --- bootstrap: ejecutable desde cualquier ruta (anade la raiz del repo al path) ---
import sys as _sys, pathlib as _pathlib
for _cand in (_pathlib.Path(__file__).resolve(), *_pathlib.Path(__file__).resolve().parents):
    if (_cand / "app").is_dir() and (_cand / "main.py").exists():
        _sys.path.insert(0, str(_cand)); break
# --- fin bootstrap ---
import argparse
import calendar
import logging
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("cycle_end_report")


def _resolve_report_date(month_arg: str) -> date:
    today = date.today()
    if not month_arg or month_arg == "current":
        return today
    if month_arg == "previous":
        first_this = today.replace(day=1)
        prev_last = first_this.replace(day=1)
        # ultimo dia del mes anterior
        year = today.year - (1 if today.month == 1 else 0)
        month = 12 if today.month == 1 else today.month - 1
        last_day = calendar.monthrange(year, month)[1]
        return date(year, month, last_day)
    # formato YYYY-MM -> ultimo dia de ese mes
    year_s, month_s = month_arg.split("-")
    year, month = int(year_s), int(month_s)
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)


def main() -> int:
    parser = argparse.ArgumentParser(description="Informe finalizacion de ciclo casa de cobranza")
    parser.add_argument("--month", default="current",
                        help="'current' (default), 'previous' o 'YYYY-MM'")
    parser.add_argument("--to", default=None, help="Destinatarios principales (CSV)")
    parser.add_argument("--cc", default=None, help="Destinatarios en copia (CSV)")
    parser.add_argument("--test", action="store_true",
                        help="Prueba: envia SOLO a mdeulofeuth@alocredit.co, sin CC")
    parser.add_argument("--no-send", action="store_true", help="Solo generar CSV, no enviar")
    args = parser.parse_args()

    from app.services.cycle_end_report_service import generate_and_send_cycle_end_report

    report_date = _resolve_report_date(args.month)

    recipient_to = None
    recipient_cc = None
    if args.test:
        recipient_to = ["mdeulofeuth@alocredit.co"]
        recipient_cc = []
    if args.to:
        recipient_to = [x.strip() for x in args.to.split(",") if x.strip()]
    if args.cc is not None:
        recipient_cc = [x.strip() for x in args.cc.split(",") if x.strip()]

    result = generate_and_send_cycle_end_report(
        report_date=report_date,
        recipient_to=recipient_to,
        recipient_cc=recipient_cc,
        send=not args.no_send,
    )

    logger.info("=" * 70)
    logger.info("Asunto: %s", result["subject"])
    logger.info("Periodo: %s a %s", *result["periodo"])
    for h in result["houses"]:
        logger.info(
            "  %s: filas=%d con_ingreso=%d ingreso=%s -> %s",
            h["house"], h["filas"], h["con_ingreso"],
            "${:,.0f}".format(h["suma_ingreso"]), h["path"],
        )
    if not args.no_send:
        logger.info("To: %s", ", ".join(result["recipient_to"] or []))
        logger.info("CC: %s", ", ".join(result["recipient_cc"] or []))
        logger.info("Enviado: %s", result["sent"])
    logger.info("=" * 70)
    return 0 if (args.no_send or result["sent"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
