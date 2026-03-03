"""
Panel visual protegido por hash para configurar parametros de asignacion.
"""
import html
import logging
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import psycopg2
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

from app.core.config import settings
from app.core.dpd import get_dpd_range
from app.data.manual_fixed_contracts import MANUAL_FIXED_CONTRACTS
from app.runtime_config.service import RuntimeConfigService
from app.services.email_service import email_service
from app.services.report_service_extended import report_service_extended

logger = logging.getLogger(__name__)
runtime_config_service = RuntimeConfigService()
runtime_config_service.initialize_defaults_if_needed()

PANEL_HASH = (settings.ADMIN_PANEL_HASH or "").strip().strip("/")
if not PANEL_HASH:
    PANEL_HASH = "admin-secure"
AUDITOR_EMAIL = "mdeulofeuth@alocredit.co"
ASSIGNMENT_HISTORY_START_DATE = date(2025, 2, 28)
USER_LABELS = {
    45: "45 Cobyser",
    81: "81 Serlefin",
}
HOUSE_USER_IDS = {
    "cobyser": set(settings.COBYSER_USERS),
    "serlefin": set(settings.SERLEFIN_USERS),
}
DPD_BUCKET_ORDER = (
    "0",
    "1_3",
    "4_15",
    "16_30",
    "31_45",
    "46_60",
    "61_90",
    "91_120",
    "121_150",
    "151_180",
    "181_209",
    "210_MAS",
    "SIN_DPD",
)
DPD_BUCKET_LABELS = {
    "0": "0",
    "1_3": "1_3",
    "4_15": "4_15",
    "16_30": "16_30",
    "31_45": "31_45",
    "46_60": "46_60",
    "61_90": "61_90",
    "91_120": "91_120",
    "121_150": "121_150",
    "151_180": "151_180",
    "181_209": "181_209",
    "210_MAS": "Mayor 210 Dias",
    "SIN_DPD": "Sin DPD",
}
RODAMIENTO_BUCKETS = (
    "61_90",
    "91_120",
    "121_150",
    "151_180",
    "181_209",
    "210_MAS",
)
DPD_SEVERITY_INDEX = {
    bucket: idx
    for idx, bucket in enumerate(DPD_BUCKET_ORDER)
    if bucket != "SIN_DPD"
}
DPD_BUCKET_ALIASES = {
    "MAYOR_210_DIAS": "210_MAS",
    "MAYOR_210": "210_MAS",
    "210_MAS_DIAS": "210_MAS",
    "210+": "210_MAS",
    "181_210": "181_209",
    "210": "210_MAS",
    "SIN_DPD": "SIN_DPD",
    "SIN_INFO": "SIN_DPD",
    "SIN_INFORMACION": "SIN_DPD",
}

app = FastAPI(
    title="Panel de Configuracion de Asignacion",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


def _assert_hash(panel_hash: str) -> None:
    if panel_hash != PANEL_HASH:
        raise HTTPException(status_code=404, detail="Not Found")


def _format_datetime(value: datetime | None) -> str:
    if not value:
        return "-"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _format_user_label(user_id: int | None) -> str:
    if user_id is None:
        return "-"
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        return str(user_id)
    return USER_LABELS.get(user_id_int, str(user_id_int))


def _parse_start_date(raw_start_date: str) -> date:
    value = (raw_start_date or "").strip()
    if not value:
        return ASSIGNMENT_HISTORY_START_DATE

    for dt_format in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, dt_format).date()
        except ValueError:
            continue

    raise ValueError("start_date debe tener formato YYYY-MM-DD o DD/MM/YYYY")


def _format_date_ddmmyyyy(value: date) -> str:
    return value.strftime("%d/%m/%Y")


def _resolve_house_key(user_id: int | None) -> Optional[str]:
    if user_id is None:
        return None

    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        return None

    for house_key, user_ids in HOUSE_USER_IDS.items():
        if user_id_int in user_ids:
            return house_key
    return None


def _normalize_dpd_token(raw_value: str) -> str:
    normalized = unicodedata.normalize("NFKD", raw_value)
    normalized = "".join(
        char for char in normalized if not unicodedata.combining(char)
    )
    normalized = normalized.upper().strip()
    for separator in (" ", "-", "/", ".", "\\", ",", ";", ":"):
        normalized = normalized.replace(separator, "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def _normalize_dpd_bucket(raw_value: Optional[str], days_value: Optional[int]) -> str:
    if raw_value is not None:
        token = _normalize_dpd_token(str(raw_value))
        token = DPD_BUCKET_ALIASES.get(token, token)
        if token in DPD_BUCKET_ORDER:
            return token

    if days_value is not None:
        try:
            days_int = int(days_value)
        except (TypeError, ValueError):
            days_int = None
        if days_int is not None:
            range_value = get_dpd_range(days_int)
            if range_value in DPD_BUCKET_ORDER:
                return range_value

    return "SIN_DPD"


def _reports_dir() -> Path:
    path = Path(settings.REPORTS_DIR)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _latest_house_report_path(house_tag: str) -> Optional[Path]:
    reports_dir = _reports_dir()
    candidates = sorted(
        reports_dir.glob(f"*_INFORME_{house_tag}.xlsx"),
        key=lambda file_path: file_path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None
    return candidates[0]


def _generate_house_report(user_id: int, user_name: str, house_tag: str) -> Path:
    last_report = _latest_house_report_path(house_tag)
    contracts = report_service_extended.get_assigned_contracts(user_id)
    if not contracts:
        if last_report and last_report.exists():
            return last_report
        raise ValueError(f"No hay contratos asignados para {user_name}")

    try:
        report_path, _ = report_service_extended.generate_report_for_user(
            user_id=user_id,
            user_name=user_name,
            contracts=contracts,
        )
        if report_path:
            file_path = Path(report_path)
            if file_path.exists():
                return file_path

        if last_report and last_report.exists():
            logger.warning(
                "No se pudo regenerar informe de %s. Se devuelve el ultimo archivo disponible: %s",
                user_name,
                last_report,
            )
            return last_report

        raise RuntimeError(f"No se pudo generar el informe de {user_name}")

    except Exception as error:
        if last_report and last_report.exists():
            logger.warning(
                "Error generando informe de %s (%s). Se devuelve el ultimo archivo disponible: %s",
                user_name,
                error,
                last_report,
            )
            return last_report
        raise


def _send_audit_change_notification(
    *,
    actor_email: str,
    reason: str,
    client_ip: str,
    update_result: dict,
    serlefin_percent: float,
    cobyser_percent: float,
    min_days: int,
    max_days: int,
) -> bool:
    changed_rows = ""
    for change in update_result.get("changes", []):
        changed_rows += (
            "<tr>"
            f"<td>{html.escape(str(change.get('field', '-')))}</td>"
            f"<td>{html.escape(str(change.get('old', '-')))}</td>"
            f"<td>{html.escape(str(change.get('new', '-')))}</td>"
            "</tr>"
        )
    if not changed_rows:
        changed_rows = "<tr><td colspan='3'>Sin detalle de cambios</td></tr>"

    subject = "Auditoria Panel Cartera: cambios detectados"
    body = f"""
    <html>
      <body style="font-family:Verdana,Arial,sans-serif;">
        <h2>Notificacion de auditoria - Panel de Asignacion</h2>
        <p>Se registraron cambios en la configuracion de asignacion.</p>
        <p><strong>Actor:</strong> {html.escape(actor_email)}</p>
        <p><strong>IP:</strong> {html.escape(client_ip or '-')}</p>
        <p><strong>Motivo:</strong> {html.escape(reason or '-')}</p>
        <p><strong>Fecha:</strong> {html.escape(_format_datetime(datetime.now()))}</p>
        <p><strong>URL panel:</strong> /{html.escape(PANEL_HASH)}</p>
        <h3>Valores actuales</h3>
        <ul>
          <li>Serlefin: {serlefin_percent:.2f}%</li>
          <li>Cobyser: {cobyser_percent:.2f}%</li>
          <li>Rango: {min_days} a {max_days}</li>
        </ul>
        <h3>Detalle de cambios</h3>
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;">
          <thead>
            <tr>
              <th>Campo</th>
              <th>Anterior</th>
              <th>Nuevo</th>
            </tr>
          </thead>
          <tbody>
            {changed_rows}
          </tbody>
        </table>
      </body>
    </html>
    """
    return email_service.send_assignment_report(
        recipient=AUDITOR_EMAIL,
        subject=subject,
        body=body,
        attachments=None,
    )


def _load_assignment_history_report(
    start_date: Optional[date] = ASSIGNMENT_HISTORY_START_DATE,
    only_fixed: bool = False,
    fixed_contract_ids: Optional[set[int]] = None,
) -> dict:
    """Carga reporte de asignados/eliminados desde contract_advisors_history."""
    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DATABASE,
        port=settings.POSTGRES_PORT,
    )
    try:
        query = """
        SELECT
            h.id,
            h.user_id,
            h.contract_id,
            h."Fecha Inicial" AS fecha_inicial,
            h."Fecha Terminal" AS fecha_terminal,
            h.tipo,
            h.dpd_inicial,
            h.dpd_final,
            h.dias_atraso_incial,
            h.dias_atraso_terminal,
            CASE
                WHEN h."Fecha Terminal" IS NULL THEN 'ASIGNADO'
                ELSE 'ELIMINADO'
            END AS estado
        FROM alocreditindicators.contract_advisors_history h
        WHERE 1=1
        """
        params = []
        if start_date is not None:
            query += '\n AND h."Fecha Inicial"::date >= %s'
            params.append(start_date)
        if fixed_contract_ids is not None:
            if not fixed_contract_ids:
                rows = []
                start_date_label = start_date.isoformat() if start_date else "TODAS"
                return {
                    "start_date": start_date.isoformat() if start_date else None,
                    "start_date_label": start_date_label,
                    "total_rows": 0,
                    "asignados": 0,
                    "eliminados": 0,
                    "only_fixed": only_fixed,
                    "rows": rows,
                }
            fixed_ids_sql = ",".join(str(int(contract_id)) for contract_id in sorted(fixed_contract_ids))
            query += f"\n AND h.contract_id IN ({fixed_ids_sql})"
        elif only_fixed:
            query += "\n AND UPPER(COALESCE(h.tipo, '')) LIKE 'FIJO%%'"
        query += "\n ORDER BY h.\"Fecha Inicial\" DESC, h.id DESC"
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
    finally:
        conn.close()

    asignados = sum(1 for row in rows if row[10] == "ASIGNADO")
    eliminados = sum(1 for row in rows if row[10] == "ELIMINADO")

    start_date_label = start_date.isoformat() if start_date else "TODAS"

    return {
        "start_date": start_date.isoformat() if start_date else None,
        "start_date_label": start_date_label,
        "total_rows": len(rows),
        "asignados": asignados,
        "eliminados": eliminados,
        "only_fixed": only_fixed,
        "rows": rows,
    }


def _empty_mora_rotation_stats(label: str) -> dict:
    matrix = {
        initial_bucket: {
            final_bucket: 0
            for final_bucket in DPD_BUCKET_ORDER
        }
        for initial_bucket in DPD_BUCKET_ORDER
    }
    return {
        "label": label,
        "records": 0,
        "asignados": 0,
        "eliminados": 0,
        "contracts": set(),
        "matrix": matrix,
    }


def _build_mora_rotation_summary(matrix: dict) -> dict:
    summary_rows = []
    totals = {
        "asignacion": 0,
        "cantidad_rec": 0,
        "rodamiento": 0,
    }

    for initial_bucket in RODAMIENTO_BUCKETS:
        row_values = matrix.get(initial_bucket, {})
        asignacion = int(sum(row_values.values()))
        cantidad_rec = 0
        initial_severity = DPD_SEVERITY_INDEX.get(initial_bucket)

        for final_bucket, count in row_values.items():
            if not count:
                continue
            final_severity = DPD_SEVERITY_INDEX.get(final_bucket)
            if (
                initial_severity is not None
                and final_severity is not None
                and final_severity < initial_severity
            ):
                cantidad_rec += int(count)

        rodamiento = asignacion - cantidad_rec
        recovery_pct = (cantidad_rec / asignacion * 100.0) if asignacion else 0.0
        rodamiento_pct = (rodamiento / asignacion * 100.0) if asignacion else 0.0

        summary_rows.append(
            {
                "bucket": initial_bucket,
                "asignacion": asignacion,
                "cantidad_rec": cantidad_rec,
                "recovery_pct": recovery_pct,
                "meta_pct": 30.0,
                "rodamiento": rodamiento,
                "rodamiento_pct": rodamiento_pct,
            }
        )

        totals["asignacion"] += asignacion
        totals["cantidad_rec"] += cantidad_rec
        totals["rodamiento"] += rodamiento

    total_asignacion = totals["asignacion"]
    totals["recovery_pct"] = (
        totals["cantidad_rec"] / total_asignacion * 100.0
        if total_asignacion
        else 0.0
    )
    totals["rodamiento_pct"] = (
        totals["rodamiento"] / total_asignacion * 100.0
        if total_asignacion
        else 0.0
    )

    return {
        "rows": summary_rows,
        "totals": totals,
        "meta_pct": 30.0,
    }


def _load_mora_rotation_report(
    start_date: date = ASSIGNMENT_HISTORY_START_DATE,
) -> dict:
    all_users = sorted(
        {
            *settings.COBYSER_USERS,
            *settings.SERLEFIN_USERS,
        }
    )
    users_sql = ",".join(str(int(user_id)) for user_id in all_users)

    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DATABASE,
        port=settings.POSTGRES_PORT,
    )
    try:
        query = f"""
        SELECT
            h.user_id,
            h.contract_id,
            h."Fecha Inicial" AS fecha_inicial,
            h."Fecha Terminal" AS fecha_terminal,
            h.dpd_inicial,
            h.dpd_final,
            h.dias_atraso_incial,
            h.dias_atraso_terminal
        FROM alocreditindicators.contract_advisors_history h
        WHERE h."Fecha Inicial"::date >= %s
          AND h.user_id IN ({users_sql})
        ORDER BY h."Fecha Inicial" DESC, h.id DESC
        """
        with conn.cursor() as cur:
            cur.execute(query, (start_date,))
            rows = cur.fetchall()
    finally:
        conn.close()

    house_reports = {
        "cobyser": _empty_mora_rotation_stats("Cobyser"),
        "serlefin": _empty_mora_rotation_stats("Serlefin"),
    }

    for row in rows:
        (
            user_id,
            contract_id,
            _fecha_inicial,
            fecha_terminal,
            dpd_inicial,
            dpd_final,
            dias_inicial,
            dias_terminal,
        ) = row

        house_key = _resolve_house_key(user_id)
        if house_key not in house_reports:
            continue

        initial_bucket = _normalize_dpd_bucket(dpd_inicial, dias_inicial)
        final_bucket = _normalize_dpd_bucket(dpd_final, dias_terminal)
        if final_bucket == "SIN_DPD":
            final_bucket = initial_bucket
        estado = "ASIGNADO" if fecha_terminal is None else "ELIMINADO"

        target = house_reports[house_key]
        target["records"] += 1
        if estado == "ASIGNADO":
            target["asignados"] += 1
        else:
            target["eliminados"] += 1

        if contract_id is not None:
            try:
                target["contracts"].add(int(contract_id))
            except (TypeError, ValueError):
                pass

        target["matrix"][initial_bucket][final_bucket] += 1

    ordered_reports = [
        house_reports["cobyser"],
        house_reports["serlefin"],
    ]
    for report in ordered_reports:
        report["contracts_count"] = len(report["contracts"])
        report["summary"] = _build_mora_rotation_summary(report["matrix"])

    return {
        "start_date": start_date.isoformat(),
        "start_date_label": _format_date_ddmmyyyy(start_date),
        "total_rows": len(rows),
        "reports": ordered_reports,
    }


def _render_mora_matrix_table(matrix: dict) -> str:
    column_totals = defaultdict(int)
    body_rows = ""
    grand_total = 0

    for initial_bucket in DPD_BUCKET_ORDER:
        row_total = 0
        value_cells = ""
        for final_bucket in DPD_BUCKET_ORDER:
            value = int(matrix.get(initial_bucket, {}).get(final_bucket, 0))
            row_total += value
            column_totals[final_bucket] += value
            value_cells += (
                f"<td class='num'>{value if value else ''}</td>"
            )

        grand_total += row_total
        body_rows += (
            "<tr>"
            f"<th>{html.escape(DPD_BUCKET_LABELS[initial_bucket])}</th>"
            f"{value_cells}"
            f"<th class='num'>{row_total}</th>"
            "</tr>"
        )

    total_cells = ""
    for final_bucket in DPD_BUCKET_ORDER:
        total_cells += f"<th class='num'>{int(column_totals[final_bucket])}</th>"

    headers = "".join(
        f"<th>{html.escape(DPD_BUCKET_LABELS[bucket])}</th>"
        for bucket in DPD_BUCKET_ORDER
    )

    return f"""
    <div class="matrix-wrap">
      <table class="matrix-table">
        <thead>
          <tr>
            <th>Bucket Inicial</th>
            {headers}
            <th>Total general</th>
          </tr>
        </thead>
        <tbody>
          {body_rows}
        </tbody>
        <tfoot>
          <tr>
            <th>Total general</th>
            {total_cells}
            <th class='num'>{grand_total}</th>
          </tr>
        </tfoot>
      </table>
    </div>
    """


def _render_mora_summary_table(summary: dict) -> str:
    rows_html = ""
    for row in summary["rows"]:
        rows_html += (
            "<tr>"
            f"<td>{html.escape(DPD_BUCKET_LABELS[row['bucket']])}</td>"
            f"<td class='num'>{row['asignacion']}</td>"
            f"<td class='num'>{row['cantidad_rec']}</td>"
            f"<td class='num'>{row['recovery_pct']:.2f}%</td>"
            f"<td class='num'>{row['meta_pct']:.0f}%</td>"
            f"<td class='num'>{row['rodamiento']}</td>"
            f"<td class='num'>{row['rodamiento_pct']:.2f}%</td>"
            "</tr>"
        )

    totals = summary["totals"]
    rows_html += (
        "<tr class='total-row'>"
        "<td>Total</td>"
        f"<td class='num'>{totals['asignacion']}</td>"
        f"<td class='num'>{totals['cantidad_rec']}</td>"
        f"<td class='num'>{totals['recovery_pct']:.2f}%</td>"
        f"<td class='num'>{summary['meta_pct']:.0f}%</td>"
        f"<td class='num'>{totals['rodamiento']}</td>"
        f"<td class='num'>{totals['rodamiento_pct']:.2f}%</td>"
        "</tr>"
    )

    return f"""
    <div class="summary-wrap">
      <table class="summary-table">
        <thead>
          <tr>
            <th>DPD</th>
            <th>Asignacion</th>
            <th>Cantidad Rec.</th>
            <th>% Recuperacion</th>
            <th>Meta (Acida)</th>
            <th>Rodamiento</th>
            <th>% Rodamiento</th>
          </tr>
        </thead>
        <tbody>
          {rows_html}
        </tbody>
      </table>
    </div>
    """


def _render_mora_rotation_report_html(panel_hash: str, report: dict) -> str:
    sections_html = ""
    for house_report in report["reports"]:
        sections_html += f"""
        <section class="card">
          <h2>{html.escape(house_report["label"])}</h2>
          <div class="meta">
            <span>Registros: <strong>{house_report["records"]}</strong></span>
            <span>Contratos unicos: <strong>{house_report["contracts_count"]}</strong></span>
            <span>Asignados: <strong>{house_report["asignados"]}</strong></span>
            <span>Eliminados: <strong>{house_report["eliminados"]}</strong></span>
          </div>
          {_render_mora_matrix_table(house_report["matrix"])}
          <h3>Rodamiento de cartera por alturas de mora</h3>
          {_render_mora_summary_table(house_report["summary"])}
        </section>
        """

    back_path = f"/{panel_hash}"
    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Informe Rotacion de Mora</title>
  <style>
    :root {{
      --ink: #131a1f;
      --paper: #f5f2ea;
      --panel: #fffef9;
      --line: #d8d2c5;
      --head: #e5f0f7;
      --accent: #0f766e;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 18px;
      font-family: "Sora", "IBM Plex Sans", "Trebuchet MS", sans-serif;
      color: var(--ink);
      background: var(--paper);
    }}
    .wrap {{ max-width: 1650px; margin: 0 auto; display: grid; gap: 16px; }}
    .head {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
    }}
    .kpi {{
      display: inline-block;
      margin-right: 12px;
      margin-bottom: 8px;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #f8f6ef;
      font-size: 0.88rem;
    }}
    .btn {{
      display: inline-block;
      text-decoration: none;
      border-radius: 10px;
      padding: 10px 12px;
      background: var(--accent);
      color: #fff;
      font-weight: 700;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
    }}
    .card h2 {{
      margin: 0 0 10px;
      text-transform: uppercase;
      letter-spacing: 0.03em;
      font-size: 0.95rem;
    }}
    .card h3 {{
      margin: 12px 0 8px;
      font-size: 0.88rem;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 10px;
      font-size: 0.85rem;
      color: #3a4752;
    }}
    .matrix-wrap, .summary-wrap {{
      overflow-x: auto;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
    }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1220px; }}
    .summary-table {{ min-width: 760px; }}
    th, td {{
      border-bottom: 1px solid #ece7db;
      border-right: 1px solid #ece7db;
      padding: 7px 8px;
      font-size: 0.8rem;
      text-align: center;
    }}
    th:first-child, td:first-child {{ text-align: left; }}
    thead th {{
      background: var(--head);
      font-weight: 700;
    }}
    tfoot th {{
      background: #f4f2ea;
      font-weight: 700;
    }}
    .num {{ font-variant-numeric: tabular-nums; }}
    .total-row td {{
      background: #f4f2ea;
      font-weight: 700;
    }}
    @media (max-width: 760px) {{
      body {{ padding: 10px; }}
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="head">
      <h1 style="margin:0 0 8px;font-size:1.1rem;">Informe Rotacion de Mora por DPD</h1>
      <div class="kpi">Fecha inicio: <strong>{html.escape(report["start_date_label"])}</strong></div>
      <div class="kpi">Filas historial: <strong>{report["total_rows"]}</strong></div>
      <br />
      <a class="btn" href="{html.escape(back_path)}">Volver al panel</a>
    </section>

    {sections_html}
  </main>
</body>
</html>
"""


def _render_mora_panel_cards(report: dict) -> str:
    cards_html = ""
    for house_report in report["reports"]:
        cards_html += f"""
        <article class="auto-card">
          <h3>{html.escape(house_report["label"])}</h3>
          <div class="auto-kpis">
            <span>Registros: <strong>{house_report["records"]}</strong></span>
            <span>Contratos unicos: <strong>{house_report["contracts_count"]}</strong></span>
            <span>Asignados: <strong>{house_report["asignados"]}</strong></span>
            <span>Eliminados: <strong>{house_report["eliminados"]}</strong></span>
          </div>
          {_render_mora_matrix_table(house_report["matrix"])}
          <h4>Rodamiento de cartera por alturas de mora</h4>
          {_render_mora_summary_table(house_report["summary"])}
        </article>
        """
    return cards_html


def _load_fixed_contract_ids() -> set[int]:
    """Obtiene contratos fijos vigentes (manuales + managements) para 45/81."""
    manual_fixed_ids: set[int] = {
        int(contract_id)
        for contract_ids in MANUAL_FIXED_CONTRACTS.values()
        for contract_id in contract_ids
    }

    all_users = sorted({
        *settings.COBYSER_USERS,
        *settings.SERLEFIN_USERS,
    })
    users_sql = ",".join(str(int(user_id)) for user_id in all_users)

    today = datetime.now().date()
    validity_start = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=settings.PAGO_TOTAL_VALIDITY_DAYS)

    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DATABASE,
        port=settings.POSTGRES_PORT,
    )
    try:
        query = f"""
        SELECT DISTINCT m.contract_id
        FROM alocreditindicators.managements m
        WHERE m.user_id IN ({users_sql})
          AND (
            (
              m.effect = %s
              AND m.promise_date IS NOT NULL
              AND m.promise_date >= %s
            )
            OR
            (
              m.effect = %s
              AND m.management_date IS NOT NULL
              AND m.management_date >= %s
            )
          )
        """
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    settings.EFFECT_ACUERDO_PAGO,
                    today,
                    settings.EFFECT_PAGO_TOTAL,
                    validity_start,
                ),
            )
            management_rows = cur.fetchall()
    finally:
        conn.close()

    management_fixed_ids = {
        int(row[0])
        for row in management_rows
        if row and row[0] is not None
    }
    return manual_fixed_ids | management_fixed_ids


def _render_assignment_history_report_html(
    panel_hash: str,
    report: dict,
    title: str = "Informe Asignados y Eliminados",
) -> str:
    """Renderiza HTML del informe de asignados/eliminados (solo contract history)."""
    table_rows = ""
    for row in report["rows"]:
        (
            row_id,
            user_id,
            contract_id,
            fecha_inicial,
            fecha_terminal,
            tipo,
            dpd_inicial,
            dpd_final,
            dias_inicial,
            dias_terminal,
            estado,
        ) = row

        fecha_inicial_txt = _format_datetime(fecha_inicial)
        fecha_terminal_txt = _format_datetime(fecha_terminal)
        user_label = _format_user_label(user_id)

        table_rows += (
            "<tr>"
            f"<td>{row_id}</td>"
            f"<td>{html.escape(user_label)}</td>"
            f"<td>{contract_id}</td>"
            f"<td>{html.escape(fecha_inicial_txt)}</td>"
            f"<td>{html.escape(fecha_terminal_txt)}</td>"
            f"<td>{html.escape(str(tipo or '-'))}</td>"
            f"<td>{html.escape(str(dpd_inicial or '-'))}</td>"
            f"<td>{html.escape(str(dpd_final or '-'))}</td>"
            f"<td>{html.escape(str(dias_inicial if dias_inicial is not None else '-'))}</td>"
            f"<td>{html.escape(str(dias_terminal if dias_terminal is not None else '-'))}</td>"
            f"<td>{html.escape(estado)}</td>"
            "</tr>"
        )

    if not table_rows:
        if report.get("start_date"):
            table_rows = (
                "<tr><td colspan='11'>Sin registros para Fecha Inicial desde "
                + html.escape(str(report["start_date"]))
                + ".</td></tr>"
            )
        else:
            table_rows = "<tr><td colspan='11'>Sin registros.</td></tr>"

    back_path = f"/{panel_hash}"

    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    body {{
      margin: 0;
      padding: 20px;
      font-family: "Sora", "IBM Plex Sans", "Trebuchet MS", sans-serif;
      background: #f5f2ea;
      color: #1f2a33;
    }}
    .wrap {{ max-width: 1280px; margin: 0 auto; }}
    .head {{
      background: #fffef9;
      border: 1px solid #d8d2c5;
      border-radius: 12px;
      padding: 14px;
      margin-bottom: 12px;
    }}
    .kpi {{
      display: inline-block;
      margin-right: 14px;
      padding: 8px 10px;
      border: 1px solid #d8d2c5;
      border-radius: 10px;
      background: #faf8f1;
      font-size: 0.9rem;
    }}
    .btn {{
      display: inline-block;
      text-decoration: none;
      margin-top: 10px;
      border-radius: 10px;
      padding: 9px 12px;
      background: #0f766e;
      color: #fff;
      font-weight: 700;
    }}
    .table-wrap {{
      background: #fffef9;
      border: 1px solid #d8d2c5;
      border-radius: 12px;
      overflow: auto;
    }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1200px; }}
    th, td {{ border-bottom: 1px solid #ece7db; text-align: left; padding: 8px 10px; font-size: 0.85rem; }}
    th {{ background: #fcfbf7; text-transform: uppercase; font-size: 0.75rem; }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="head">
      <h2 style="margin:0 0 8px">{html.escape(title)}</h2>
      <div class="kpi">Rango Fecha Inicial: <strong>{html.escape(str(report["start_date_label"]))}</strong></div>
      <div class="kpi">Total filas: <strong>{report["total_rows"]}</strong></div>
      <div class="kpi">Asignados: <strong>{report["asignados"]}</strong></div>
      <div class="kpi">Eliminados: <strong>{report["eliminados"]}</strong></div>
      <br />
      <a class="btn" href="{html.escape(back_path)}">Volver al panel</a>
    </section>

    <section class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Casa</th>
            <th>Contrato</th>
            <th>Fecha Inicial</th>
            <th>Fecha Terminal</th>
            <th>Tipo</th>
            <th>DPD Inicial</th>
            <th>DPD Final</th>
            <th>Días Inicial</th>
            <th>Días Terminal</th>
            <th>Estado</th>
          </tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>
    </section>
  </main>
</body>
</html>
"""


def _render_panel_html(
    *,
    panel_hash: str,
    ok_message: str,
    error_message: str,
) -> str:
    config = runtime_config_service.get_assignment_config()
    audit_rows = runtime_config_service.list_audit(limit=80)

    message_block = ""
    if ok_message:
        message_block = (
            "<div class='alert ok'>"
            + html.escape(ok_message)
            + "</div>"
        )
    elif error_message:
        message_block = (
            "<div class='alert error'>"
            + html.escape(error_message)
            + "</div>"
        )

    audit_html_rows = ""
    for row in audit_rows:
        audit_html_rows += (
            "<tr>"
            f"<td>{html.escape(_format_datetime(row.get('changed_at')))}</td>"
            f"<td>{html.escape(str(row.get('actor_email') or '-'))}</td>"
            f"<td>{html.escape(str(row.get('changed_field') or '-'))}</td>"
            f"<td>{html.escape(str(row.get('old_value') or '-'))}</td>"
            f"<td>{html.escape(str(row.get('new_value') or '-'))}</td>"
            f"<td>{html.escape(str(row.get('reason') or '-'))}</td>"
            f"<td>{html.escape(str(row.get('client_ip') or '-'))}</td>"
            "</tr>"
        )

    if not audit_html_rows:
        audit_html_rows = "<tr><td colspan='7'>Sin cambios registrados aun.</td></tr>"

    action_path = f"/{panel_hash}/save"
    download_serlefin_path = f"/{panel_hash}/reports/serlefin"
    download_cobyser_path = f"/{panel_hash}/reports/cobyser"
    default_start_date = ASSIGNMENT_HISTORY_START_DATE.isoformat()
    assignment_history_path = (
        f"/{panel_hash}/history/asignados-eliminados?start_date={default_start_date}"
    )
    mora_rotation_path = (
        f"/{panel_hash}/history/rotacion-mora?start_date={default_start_date}"
    )
    fixed_history_path = f"/{panel_hash}/history/fijos"
    mora_auto_block = ""
    try:
        auto_report = _load_mora_rotation_report(start_date=ASSIGNMENT_HISTORY_START_DATE)
        auto_cards = _render_mora_panel_cards(auto_report)
        mora_auto_block = f"""
        <section class="table-card">
          <div class="table-head">Rotacion de Mora Automatica (Cobyser y Serlefin)</div>
          <div class="auto-head">
            <span>Fecha inicio: <strong>{html.escape(auto_report["start_date_label"])}</strong></span>
            <span>Filas historial: <strong>{auto_report["total_rows"]}</strong></span>
            <span>Actualizacion: <strong>{html.escape(_format_datetime(datetime.now()))}</strong></span>
          </div>
          <div class="auto-grid">
            {auto_cards}
          </div>
        </section>
        """
    except Exception as error:
        logger.error("Error cargando vista automatica de rotacion de mora: %s", error)
        mora_auto_block = (
            "<section class='table-card'>"
            "<div class='table-head'>Rotacion de Mora Automatica</div>"
            "<div style='padding:12px 14px;color:#b42318'>"
            "No se pudo cargar la rotacion automatica en el panel."
            "</div>"
            "</section>"
        )

    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Panel Seguro de Asignacion</title>
  <style>
    :root {{
      --ink: #131a1f;
      --muted: #5a6673;
      --paper: #f7f2e8;
      --panel: #fffef9;
      --accent: #046c6f;
      --accent-soft: #d8f0ed;
      --danger: #b42318;
      --danger-soft: #fce7e6;
      --ok: #0f5132;
      --ok-soft: #e8f6ee;
      --line: #d8d2c5;
      --shadow: 0 16px 40px rgba(12, 20, 26, 0.12);
    }}

    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Sora", "IBM Plex Sans", "Trebuchet MS", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 8% 8%, #cff2ec 0, #cff2ec00 48%),
        radial-gradient(circle at 96% 2%, #fde2cc 0, #fde2cc00 42%),
        linear-gradient(135deg, #ece5d6 0%, #f8f4ea 100%);
      min-height: 100vh;
      padding: 24px;
    }}

    .wrap {{
      max-width: 1250px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }}

    .hero {{
      background:
        radial-gradient(circle at 80% 10%, #ffffff40 0, #ffffff00 45%),
        linear-gradient(160deg, #0d5f5f, #0b4d5d);
      color: #fdfdfb;
      border-radius: 18px;
      padding: 24px;
      box-shadow: var(--shadow);
      animation: enter 360ms ease-out;
    }}

    .hero h1 {{
      margin: 0 0 10px;
      font-size: clamp(1.3rem, 2.2vw, 1.85rem);
      letter-spacing: 0.02em;
    }}

    .hero p {{ margin: 0; opacity: 0.94; }}
    .hero-meta {{
      margin-top: 12px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      border: 1px solid #ffffff55;
      padding: 5px 10px;
      border-radius: 999px;
      font-size: 0.78rem;
      background: #ffffff18;
    }}

    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(330px, 1fr));
      gap: 18px;
    }}

    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 18px;
      box-shadow: var(--shadow);
      animation: enter 420ms ease-out;
    }}

    .card h2 {{
      margin: 0 0 12px;
      font-size: 0.95rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: #1f2a33;
    }}

    .row {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
    }}

    label {{
      display: block;
      font-size: 0.85rem;
      color: var(--muted);
      margin-bottom: 4px;
    }}

    input, textarea {{
      width: 100%;
      border: 1px solid #c7c2b7;
      background: #fffffc;
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 0.95rem;
      color: var(--ink);
      outline: none;
      transition: border-color 180ms ease, box-shadow 180ms ease;
    }}

    input:focus, textarea:focus {{
      border-color: var(--accent);
      box-shadow: 0 0 0 3px var(--accent-soft);
    }}

    .btn {{
      border: 0;
      border-radius: 11px;
      padding: 11px 16px;
      font-size: 0.95rem;
      font-weight: 700;
      background: linear-gradient(160deg, #14532d, #0f766e);
      color: #fff;
      cursor: pointer;
      margin-top: 10px;
    }}

    .btn:hover {{ filter: brightness(1.05); }}

    .btn-link {{
      display: inline-block;
      text-decoration: none;
      text-align: center;
      margin-right: 8px;
    }}

    .btn-alt {{
      background: linear-gradient(165deg, #254b79, #1570a6);
      margin-top: 0;
    }}
    .auditor {{
      background: #edf6f5;
      border: 1px solid #c7e5e0;
      border-radius: 10px;
      padding: 10px 12px;
      color: #124240;
      font-size: 0.86rem;
      margin-top: 10px;
    }}

    .meta-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(120px, 1fr));
      gap: 12px;
    }}

    .meta-item {{
      background: #faf8f1;
      border: 1px solid #e4dece;
      border-radius: 10px;
      padding: 10px;
    }}

    .meta-item .k {{ font-size: 0.75rem; color: var(--muted); text-transform: uppercase; }}
    .meta-item .v {{ font-size: 1.1rem; font-weight: 700; margin-top: 4px; }}

    .alert {{
      border-radius: 12px;
      padding: 12px 14px;
      border: 1px solid;
      font-size: 0.92rem;
      animation: enter 280ms ease-out;
    }}

    .alert.ok {{
      background: var(--ok-soft);
      color: var(--ok);
      border-color: #9fd3b7;
    }}

    .alert.error {{
      background: var(--danger-soft);
      color: var(--danger);
      border-color: #f4b2ad;
    }}

    .table-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      overflow: hidden;
      box-shadow: var(--shadow);
      animation: enter 500ms ease-out;
    }}

    .table-head {{
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      background: #f8f6ef;
      font-weight: 700;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      font-size: 0.82rem;
    }}

    .table-wrap {{ overflow-x: auto; }}
    .auto-head {{
      padding: 10px 14px;
      border-bottom: 1px solid var(--line);
      background: #fbf9f3;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      font-size: 0.84rem;
      color: #40505e;
    }}
    .auto-head span {{
      padding: 6px 8px;
      border: 1px solid #ddd5c7;
      border-radius: 8px;
      background: #fffef8;
    }}
    .auto-grid {{
      padding: 12px;
      display: grid;
      gap: 12px;
    }}
    .auto-card {{
      background: #fffefb;
      border: 1px solid #ddd5c7;
      border-radius: 12px;
      padding: 10px;
    }}
    .auto-card h3 {{
      margin: 0 0 8px;
      text-transform: uppercase;
      font-size: 0.88rem;
      letter-spacing: 0.03em;
    }}
    .auto-card h4 {{
      margin: 10px 0 6px;
      text-transform: uppercase;
      font-size: 0.8rem;
      letter-spacing: 0.03em;
    }}
    .auto-kpis {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 8px;
      font-size: 0.8rem;
      color: #485968;
    }}
    .auto-kpis span {{
      padding: 5px 8px;
      border: 1px solid #d9d2c5;
      border-radius: 8px;
      background: #faf7ef;
    }}
    .auto-card .matrix-wrap,
    .auto-card .summary-wrap {{
      overflow-x: auto;
      border: 1px solid #d9d2c5;
      border-radius: 8px;
      background: #fff;
    }}
    .auto-card .summary-wrap {{ margin-top: 4px; }}
    .auto-card .matrix-table {{ min-width: 1260px; }}
    .auto-card .summary-table {{ min-width: 760px; }}
    .auto-card .matrix-table th,
    .auto-card .matrix-table td,
    .auto-card .summary-table th,
    .auto-card .summary-table td {{
      border-bottom: 1px solid #ece7db;
      border-right: 1px solid #ece7db;
      text-align: center;
      padding: 7px 8px;
      font-size: 0.79rem;
      vertical-align: middle;
    }}
    .auto-card .matrix-table th:first-child,
    .auto-card .matrix-table td:first-child,
    .auto-card .summary-table th:first-child,
    .auto-card .summary-table td:first-child {{
      text-align: left;
    }}
    .auto-card .matrix-table thead th,
    .auto-card .summary-table thead th {{
      background: #b7dced;
      color: #1b2b3a;
      text-transform: none;
      font-size: 0.78rem;
    }}
    .auto-card .matrix-table tfoot th {{
      background: #d5e8f2;
      font-weight: 700;
      text-transform: none;
    }}
    .auto-card .summary-table .total-row td {{
      background: #efece3;
      font-weight: 700;
    }}
    .auto-card .num {{ font-variant-numeric: tabular-nums; }}

    table {{ width: 100%; border-collapse: collapse; min-width: 900px; }}

    th, td {{
      border-bottom: 1px solid #ece7db;
      text-align: left;
      padding: 10px 12px;
      font-size: 0.86rem;
      vertical-align: top;
    }}

    th {{ background: #fcfbf7; color: #38434d; text-transform: uppercase; font-size: 0.75rem; }}

    @keyframes enter {{
      from {{ opacity: 0; transform: translateY(6px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}

    @media (max-width: 760px) {{
      body {{ padding: 14px; }}
      .row {{ grid-template-columns: 1fr; }}
      .meta-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <h1>Panel Seguro de Parametros de Asignacion</h1>
      <p>URL protegida por hash. Todo cambio queda auditado y notificado al auditor.</p>
      <div class="hero-meta">
        <span class="chip">Hash activo: /{html.escape(panel_hash)}</span>
        <span class="chip">Auditor: {html.escape(AUDITOR_EMAIL)}</span>
      </div>
    </section>

    {message_block}

    <section class="grid">
      <article class="card">
        <h2>Configuracion activa</h2>
        <div class="meta-grid">
          <div class="meta-item"><div class="k">Serlefin %</div><div class="v">{config.serlefin_percent:.2f}</div></div>
          <div class="meta-item"><div class="k">Cobyser %</div><div class="v">{config.cobyser_percent:.2f}</div></div>
          <div class="meta-item"><div class="k">Rango minimo</div><div class="v">{int(config.min_days)}</div></div>
          <div class="meta-item"><div class="k">Rango maximo</div><div class="v">{int(config.max_days)}</div></div>
          <div class="meta-item"><div class="k">Actualizado por</div><div class="v" style="font-size:.9rem">{html.escape(config.updated_by)}</div></div>
          <div class="meta-item"><div class="k">Fecha update</div><div class="v" style="font-size:.9rem">{html.escape(_format_datetime(config.updated_at))}</div></div>
        </div>
      </article>

      <article class="card">
        <h2>Actualizar parametros</h2>
        <form method="post" action="{html.escape(action_path)}">
          <div class="row">
            <div>
              <label for="serlefin_percent">Porcentaje Serlefin (%)</label>
              <input id="serlefin_percent" name="serlefin_percent" type="number" step="0.01" min="0" max="100" value="{config.serlefin_percent:.2f}" required />
            </div>
            <div>
              <label for="cobyser_percent">Porcentaje Cobyser (%)</label>
              <input id="cobyser_percent" name="cobyser_percent" type="number" step="0.01" min="0" max="100" value="{config.cobyser_percent:.2f}" required />
            </div>
          </div>

          <div class="row" style="margin-top:10px">
            <div>
              <label for="min_days">Rango minimo de atraso</label>
              <input id="min_days" name="min_days" type="number" min="0" value="{int(config.min_days)}" required />
            </div>
            <div>
              <label for="max_days">Rango maximo de atraso</label>
              <input id="max_days" name="max_days" type="number" min="0" value="{int(config.max_days)}" required />
            </div>
          </div>

          <div style="margin-top:10px">
            <label>Actor de auditoria (fijo)</label>
            <input type="email" value="{html.escape(AUDITOR_EMAIL)}" readonly />
          </div>

          <div style="margin-top:10px">
            <label for="reason">Motivo del cambio (opcional)</label>
            <textarea id="reason" name="reason" rows="3" placeholder="Ej: ajustar meta operativa de la semana"></textarea>
          </div>

          <button class="btn" type="submit">Guardar cambios auditados</button>
        </form>
      </article>

      <article class="card">
        <h2>Descarga de Informes</h2>
        <p style="margin-top:0;color:#5a6673">
          Descarga directa de Excel para cada casa de cobranza.
        </p>
        <div class="auditor">
          Cada cambio de configuracion se notifica automaticamente a <strong>{html.escape(AUDITOR_EMAIL)}</strong>.
        </div>
        <br />
        <a class="btn btn-link btn-alt" href="{html.escape(download_serlefin_path)}">Descargar Serlefin</a>
        <a class="btn btn-link btn-alt" href="{html.escape(download_cobyser_path)}">Descargar Cobyser</a>
        <a class="btn btn-link" href="{html.escape(assignment_history_path)}">Ver Asignados/Eliminados (desde 2025-02-28)</a>
        <a class="btn btn-link" href="{html.escape(mora_rotation_path)}">Ver Rotacion de Mora (Cobyser y Serlefin)</a>
        <a class="btn btn-link" href="{html.escape(fixed_history_path)}">Ver Fijos (todos)</a>
      </article>
    </section>

    {mora_auto_block}

    <section class="table-card">
      <div class="table-head">Historico de cambios</div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Fecha</th>
              <th>Actor</th>
              <th>Campo</th>
              <th>Valor anterior</th>
              <th>Valor nuevo</th>
              <th>Motivo</th>
              <th>IP</th>
            </tr>
          </thead>
          <tbody>
            {audit_html_rows}
          </tbody>
        </table>
      </div>
    </section>
  </main>
</body>
</html>
"""


@app.get("/_health", include_in_schema=False)
async def panel_health() -> JSONResponse:
    return JSONResponse({"status": "ok", "panel": "running"})


@app.get("/", include_in_schema=False)
async def hidden_root() -> HTMLResponse:
    raise HTTPException(status_code=404, detail="Not Found")


@app.get("/{panel_hash}", response_class=HTMLResponse, include_in_schema=False)
async def panel_home(
    panel_hash: str,
    ok: str = "",
    error: str = "",
) -> HTMLResponse:
    _assert_hash(panel_hash)
    page = _render_panel_html(
        panel_hash=panel_hash,
        ok_message=ok,
        error_message=error,
    )
    return HTMLResponse(page)


@app.get("/{panel_hash}/reports/{house_key}", include_in_schema=False)
async def download_house_report(panel_hash: str, house_key: str):
    _assert_hash(panel_hash)

    houses = {
        "serlefin": (81, "Serlefin", "Serlefin"),
        "cobyser": (45, "Cobyser", "Cobyser"),
    }
    house = houses.get(house_key.strip().lower())
    if not house:
        raise HTTPException(status_code=404, detail="Not Found")

    user_id, user_name, house_tag = house

    try:
        file_path = _generate_house_report(
            user_id=user_id,
            user_name=user_name,
            house_tag=house_tag,
        )
        return FileResponse(
            path=str(file_path),
            filename=file_path.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as error:
        logger.error("Error generando/descargando informe %s: %s", house_key, error)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.get("/{panel_hash}/history/asignados-eliminados", response_class=HTMLResponse, include_in_schema=False)
async def assignment_history_report(
    panel_hash: str,
    start_date: str = ASSIGNMENT_HISTORY_START_DATE.isoformat(),
):
    _assert_hash(panel_hash)

    try:
        parsed_start_date = _parse_start_date(start_date)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))

    try:
        report = _load_assignment_history_report(start_date=parsed_start_date)
        return HTMLResponse(_render_assignment_history_report_html(panel_hash, report))
    except Exception as error:
        logger.error("Error generando informe de asignados/eliminados: %s", error)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.get("/{panel_hash}/history/rotacion-mora", response_class=HTMLResponse, include_in_schema=False)
async def mora_rotation_report(
    panel_hash: str,
    start_date: str = ASSIGNMENT_HISTORY_START_DATE.isoformat(),
):
    _assert_hash(panel_hash)

    try:
        parsed_start_date = _parse_start_date(start_date)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))

    try:
        report = _load_mora_rotation_report(start_date=parsed_start_date)
        return HTMLResponse(_render_mora_rotation_report_html(panel_hash, report))
    except Exception as error:
        logger.error("Error generando informe de rotacion de mora: %s", error)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.get("/{panel_hash}/history/fijos", response_class=HTMLResponse, include_in_schema=False)
async def fixed_history_report(panel_hash: str):
    _assert_hash(panel_hash)

    try:
        fixed_contract_ids = _load_fixed_contract_ids()
        report = _load_assignment_history_report(
            start_date=None,
            only_fixed=True,
            fixed_contract_ids=fixed_contract_ids,
        )
        return HTMLResponse(
            _render_assignment_history_report_html(
                panel_hash,
                report,
                title="Informe Contratos Fijos",
            )
        )
    except Exception as error:
        logger.error("Error generando informe de contratos fijos: %s", error)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.post("/{panel_hash}/save", include_in_schema=False)
async def save_panel_config(
    request: Request,
    panel_hash: str,
    serlefin_percent: float = Form(...),
    cobyser_percent: float = Form(...),
    min_days: int = Form(...),
    max_days: int = Form(...),
    actor_email: str = Form(AUDITOR_EMAIL),
    reason: str = Form(""),
) -> RedirectResponse:
    _assert_hash(panel_hash)

    client_ip = request.headers.get("x-forwarded-for", "")
    if client_ip:
        client_ip = client_ip.split(",")[0].strip()
    elif request.client:
        client_ip = request.client.host

    # El actor de auditoria se mantiene fijo por politica operativa.
    actor_email = AUDITOR_EMAIL

    try:
        update_result = runtime_config_service.update_assignment_config(
            actor_email=actor_email,
            serlefin_percent=serlefin_percent,
            cobyser_percent=cobyser_percent,
            min_days=min_days,
            max_days=max_days,
            reason=reason,
            client_ip=client_ip,
        )

        if update_result.get("changed"):
            notify_ok = _send_audit_change_notification(
                actor_email=actor_email,
                reason=reason,
                client_ip=client_ip,
                update_result=update_result,
                serlefin_percent=float(serlefin_percent),
                cobyser_percent=float(cobyser_percent),
                min_days=int(min_days),
                max_days=int(max_days),
            )
            if notify_ok:
                message = (
                    "Configuracion actualizada con auditoria. "
                    f"Notificacion enviada a {AUDITOR_EMAIL}."
                )
            else:
                message = (
                    "Configuracion actualizada con auditoria, "
                    f"pero fallo notificacion a {AUDITOR_EMAIL}."
                )
        else:
            message = "No se detectaron cambios en los parametros."

        query = urlencode({"ok": message})
    except Exception as error:
        logger.error("Error actualizando configuracion desde panel: %s", error)
        query = urlencode({"error": str(error)})

    return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("admin_panel:app", host="0.0.0.0", port=9007, reload=False)
