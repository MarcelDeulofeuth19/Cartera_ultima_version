"""
Panel visual protegido por hash para configurar parametros de asignacion.
"""
import html
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import psycopg2
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

from app.core.config import settings
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
ASSIGNMENT_HISTORY_START_DATE = date(2026, 2, 28)
USER_LABELS = {
    45: "45 Cobyser",
    81: "81 Serlefin",
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
    start_date: date = ASSIGNMENT_HISTORY_START_DATE,
    only_fixed: bool = False,
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
        WHERE h."Fecha Inicial"::date >= %s
        """
        if only_fixed:
            query += "\n AND UPPER(COALESCE(h.tipo, '')) LIKE 'FIJO%%'"
        query += "\n ORDER BY h.\"Fecha Inicial\" DESC, h.id DESC"
        with conn.cursor() as cur:
            cur.execute(query, (start_date,))
            rows = cur.fetchall()
    finally:
        conn.close()

    asignados = sum(1 for row in rows if row[10] == "ASIGNADO")
    eliminados = sum(1 for row in rows if row[10] == "ELIMINADO")

    return {
        "start_date": start_date.isoformat(),
        "total_rows": len(rows),
        "asignados": asignados,
        "eliminados": eliminados,
        "only_fixed": only_fixed,
        "rows": rows,
    }


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
        table_rows = (
            "<tr><td colspan='11'>Sin registros para Fecha Inicial desde "
            + html.escape(str(report["start_date"]))
            + ".</td></tr>"
        )

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
      <div class="kpi">Fecha Inicial desde: <strong>{html.escape(str(report["start_date"]))}</strong></div>
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
    assignment_history_path = f"/{panel_hash}/history/asignados-eliminados?start_date=2026-02-28"
    fixed_history_path = f"/{panel_hash}/history/fijos?start_date=2026-02-28"

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
        <a class="btn btn-link" href="{html.escape(assignment_history_path)}">Ver Asignados/Eliminados (desde 2026-02-28)</a>
        <a class="btn btn-link" href="{html.escape(fixed_history_path)}">Ver Fijos (desde 2026-02-28)</a>
      </article>
    </section>

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
async def assignment_history_report(panel_hash: str, start_date: str = "2026-02-28"):
    _assert_hash(panel_hash)

    try:
        parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="start_date debe tener formato YYYY-MM-DD")

    try:
        report = _load_assignment_history_report(start_date=parsed_start_date)
        return HTMLResponse(_render_assignment_history_report_html(panel_hash, report))
    except Exception as error:
        logger.error("Error generando informe de asignados/eliminados: %s", error)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.get("/{panel_hash}/history/fijos", response_class=HTMLResponse, include_in_schema=False)
async def fixed_history_report(panel_hash: str, start_date: str = "2026-02-28"):
    _assert_hash(panel_hash)

    try:
        parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="start_date debe tener formato YYYY-MM-DD")

    try:
        report = _load_assignment_history_report(
            start_date=parsed_start_date,
            only_fixed=True,
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

