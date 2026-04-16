"""
Panel visual protegido por hash para configurar parametros de asignacion.
"""
import html
import io
import logging
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Set
from urllib.parse import urlencode

import psycopg2
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from sqlalchemy import text

from app.core.config import settings
from app.core.file_lock import ProcessLockError, acquire_process_lock
from app.core.dpd import get_dpd_range
from app.database.connections import db_manager
from app.runtime_config.auth_service import admin_panel_auth_service
from app.runtime_config.service import RuntimeConfigService
from app.services.assignment_service import AssignmentService
from app.services.blacklist_service import blacklist_service
from app.services.email_service import email_service
from app.services.report_service_extended import report_service_extended

logger = logging.getLogger(__name__)
runtime_config_service = RuntimeConfigService()
runtime_config_service.initialize_defaults_if_needed()
if settings.ADMIN_AUTH_ENABLED:
    admin_panel_auth_service.initialize_default_user_if_needed()

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


NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


def _safe_next_path(raw_next: str) -> str:
    candidate = (raw_next or "").strip()
    default_path = f"/"
    if not candidate:
        return default_path
    if candidate.startswith("//"):
        return default_path
    if candidate.startswith(default_path):
        return candidate
    return default_path


def _build_login_redirect(next_path: str) -> RedirectResponse:
    safe_next = _safe_next_path(next_path)
    query = urlencode({"next": safe_next})
    return RedirectResponse(
        url=f"/login?{query}",
        status_code=303,
        headers=NO_CACHE_HEADERS,
    )


def _require_panel_auth(request: Request) -> Optional[RedirectResponse]:
    if not settings.ADMIN_AUTH_ENABLED:
        return None

    cookie_name = settings.ADMIN_AUTH_COOKIE_NAME
    token = request.cookies.get(cookie_name)
    username = admin_panel_auth_service.validate_session_token(token)
    if not username:
        query = request.url.query
        current_path = request.url.path + (f"?{query}" if query else "")
        return _build_login_redirect( next_path=current_path)

    request.state.panel_user = username
    return None


def _render_login_html(
    *,
    error_message: str = "",
    next_path: str = "",
) -> str:
    safe_next = _safe_next_path(next_path)
    error_block = ""
    if error_message:
        error_block = (
            "<div style='padding:12px 16px;border:1px solid #FECACA;"
            "background:#FEF2F2;color:#DC2626;border-radius:12px;"
            "margin-bottom:16px;font-size:.88rem'>"
            + html.escape(error_message)
            + "</div>"
        )

    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Login - Alo Credit</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet" />
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: linear-gradient(135deg, #FFF5ED 0%, #FFE8D6 50%, #F5F5F7 100%);
      font-family: 'Inter', -apple-system, sans-serif;
      color: #3e4a60;
      padding: 20px;
    }}
    .card {{
      width: min(400px, 100%);
      background: #FFFFFF;
      border: 1px solid #E5E7EB;
      border-radius: 20px;
      box-shadow: 0 8px 40px rgba(0,0,0,0.08);
      padding: 36px 32px;
    }}
    .logo {{
      width: 56px; height: 56px; margin: 0 auto 18px;
      background: linear-gradient(135deg, #FF8C42, #FF7A28);
      border-radius: 16px; display: grid; place-items: center;
      font-weight: 800; font-size: 24px; color: #fff;
      box-shadow: 0 4px 16px rgba(255,140,66,0.3);
    }}
    h1 {{
      text-align: center;
      margin: 0 0 4px;
      font-size: 1.3rem;
      font-weight: 800;
      color: #3e4a60;
    }}
    .subtitle {{
      text-align: center;
      margin: 0 0 24px;
      color: #5A6B8C;
      font-size: .88rem;
    }}
    label {{
      display: block;
      font-size: .78rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin: 14px 0 5px;
      color: #5A6B8C;
    }}
    input {{
      width: 100%;
      border: 1px solid #E5E7EB;
      border-radius: 10px;
      padding: 11px 14px;
      font-size: .95rem;
      background: #F9F9F9;
      color: #3e4a60;
      font-family: inherit;
      outline: none;
      transition: border-color 0.15s, box-shadow 0.15s;
    }}
    input:focus {{
      border-color: #FF8C42;
      box-shadow: 0 0 0 3px rgba(255,140,66,0.15);
      background: #fff;
    }}
    button {{
      border: 0;
      border-radius: 12px;
      padding: 12px;
      margin-top: 20px;
      width: 100%;
      color: #fff;
      font-size: .95rem;
      font-weight: 700;
      font-family: inherit;
      background: linear-gradient(135deg, #FF8C42, #FF7A28);
      cursor: pointer;
      transition: filter 0.15s;
      box-shadow: 0 4px 12px rgba(255,140,66,0.25);
    }}
    button:hover {{ filter: brightness(1.08); }}
    .meta {{
      margin-top: 16px;
      font-size: .78rem;
      color: #A0A0A0;
      text-align: center;
    }}
  </style>
</head>
<body>
  <main class="card">
    <div class="logo">A</div>
    <h1>Alo Credit</h1>
    <p class="subtitle">Panel de Asignacion de Contratos</p>
    {error_block}
    <form method="post" action="/login">
      <input type="hidden" name="next" value="{html.escape(safe_next)}" />
      <label for="username">Usuario</label>
      <input id="username" name="username" type="text" autocomplete="username" required />
      <label for="password">Clave</label>
      <input id="password" name="password" type="password" autocomplete="current-password" required />
      <button type="submit">Ingresar</button>
    </form>
    <div class="meta">Si falla el acceso, valida credenciales del panel en configuracion interna.</div>
  </main>
</body>
</html>
"""


def _assert_hash() -> None:
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


def _current_month_start() -> date:
    today = datetime.now().date()
    return today.replace(day=1)


def _format_date_ddmmyyyy(value: date) -> str:
    return value.strftime("%d/%m/%Y")


def _load_db_process_snapshot(long_running_seconds: int = 60) -> dict:
    """
    Valida procesos/sesiones con 1 consulta a MySQL y 1 consulta a PostgreSQL.
    """
    threshold = int(long_running_seconds)

    with db_manager.get_mysql_session() as mysql_session:
        mysql_row = mysql_session.execute(
            text(
                """
                SELECT
                    COUNT(*) AS total_connections,
                    COALESCE(SUM(CASE WHEN COMMAND <> 'Sleep' THEN 1 ELSE 0 END), 0) AS active_connections,
                    COALESCE(
                        SUM(
                            CASE
                                WHEN COMMAND <> 'Sleep' AND TIME >= :long_seconds THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS long_running_connections,
                    COALESCE(MAX(CASE WHEN COMMAND <> 'Sleep' THEN TIME ELSE 0 END), 0) AS max_active_seconds
                FROM information_schema.PROCESSLIST
                WHERE DB = DATABASE()
                  AND USER = :db_user
                """
            ),
            {
                "long_seconds": threshold,
                "db_user": settings.MYSQL_USER,
            },
        ).mappings().one()

    with db_manager.get_postgres_session() as postgres_session:
        postgres_row = postgres_session.execute(
            text(
                """
                SELECT
                    COUNT(*) AS total_sessions,
                    COUNT(*) FILTER (WHERE state IS DISTINCT FROM 'idle') AS active_sessions,
                    COUNT(*) FILTER (
                        WHERE state IS DISTINCT FROM 'idle'
                          AND query_start IS NOT NULL
                          AND (NOW() - query_start) >= make_interval(secs => :long_seconds)
                    ) AS long_running_sessions,
                    COALESCE(
                        MAX(EXTRACT(EPOCH FROM (NOW() - query_start))) FILTER (
                            WHERE state IS DISTINCT FROM 'idle'
                              AND query_start IS NOT NULL
                        ),
                        0
                    )::bigint AS max_active_seconds
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND usename = current_user
                  AND pid <> pg_backend_pid()
                """
            ),
            {"long_seconds": threshold},
        ).mappings().one()

    return {
        "threshold_seconds": threshold,
        "mysql": {
            "total_connections": int(mysql_row["total_connections"] or 0),
            "active_connections": int(mysql_row["active_connections"] or 0),
            "long_running_connections": int(mysql_row["long_running_connections"] or 0),
            "max_active_seconds": int(mysql_row["max_active_seconds"] or 0),
        },
        "postgres": {
            "total_sessions": int(postgres_row["total_sessions"] or 0),
            "active_sessions": int(postgres_row["active_sessions"] or 0),
            "long_running_sessions": int(postgres_row["long_running_sessions"] or 0),
            "max_active_seconds": int(postgres_row["max_active_seconds"] or 0),
        },
    }


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


def _get_history_contracts(user_id: int) -> list[int]:
    """Get contracts from the LATEST assignment cycle only (same Fecha Inicial date)."""
    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DATABASE,
        port=settings.POSTGRES_PORT,
    )
    try:
        with conn.cursor() as cur:
            # Get the most recent assignment date for this user
            cur.execute(
                """
                SELECT DISTINCT contract_id
                FROM alocreditindicators.contract_advisors_history
                WHERE user_id = %s
                  AND "Fecha Inicial"::date = (
                      SELECT MAX("Fecha Inicial"::date)
                      FROM alocreditindicators.contract_advisors_history
                      WHERE user_id = %s
                  )
                ORDER BY contract_id
                """,
                (user_id, user_id),
            )
            return [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def _generate_history_excel(user_id: int, user_name: str, house_tag: str) -> Path:
    """Generate enriched Excel from history + production data, processed in batches."""
    import pandas as pd

    # Step 1: Get contracts from latest cycle in history
    pg_conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DATABASE,
        port=settings.POSTGRES_PORT,
    )
    try:
        hist_query = """
        SELECT
            h.contract_id,
            h."Fecha Inicial",
            h."Fecha Terminal",
            h.tipo,
            h.dpd_inicial,
            h.dpd_final,
            h.dpd_actual,
            h.dias_atraso_incial,
            h.dias_atraso_terminal,
            COALESCE(NULLIF(TRIM(h.estado_actual::text), ''), 'SIN_ESTADO') AS estado_actual,
            CASE WHEN h."Fecha Terminal" IS NULL THEN 'ASIGNADO' ELSE 'ELIMINADO' END AS estado
        FROM alocreditindicators.contract_advisors_history h
        WHERE h.user_id = %s
          AND h."Fecha Inicial"::date = (
              SELECT MAX("Fecha Inicial"::date)
              FROM alocreditindicators.contract_advisors_history
              WHERE user_id = %s
          )
        ORDER BY h.contract_id
        """
        df_hist = pd.read_sql(hist_query, pg_conn, params=(user_id, user_id))
    finally:
        pg_conn.close()

    if df_hist.empty:
        raise ValueError(f"No hay registros historicos para {user_name}")

    contract_ids = df_hist["contract_id"].tolist()
    logger.info("Generando informe historico %s: %d contratos", house_tag, len(contract_ids))

    # Step 2: Enrich with production data in batches of 2000
    BATCH_SIZE = 2000
    enriched_frames = []

    for i in range(0, len(contract_ids), BATCH_SIZE):
        batch = contract_ids[i:i + BATCH_SIZE]
        lista = ",".join(str(int(c)) for c in batch)

        try:
            enriched_frames.append(
                report_service_extended.generate_report_for_user(
                    user_id=user_id,
                    user_name=user_name,
                    contracts=batch,
                )[1]
            )
        except Exception as err:
            logger.warning("Batch %d-%d fallo enriquecimiento: %s, usando datos basicos", i, i + len(batch), err)
            # Fallback: just use history data for this batch
            enriched_frames.append(
                df_hist[df_hist["contract_id"].isin(batch)].copy()
            )

    # Merge all batches
    if enriched_frames:
        df_enriched = pd.concat([f for f in enriched_frames if f is not None and not f.empty], ignore_index=True)
    else:
        df_enriched = df_hist.copy()

    # If enriched data has the detailed columns, merge history tracking columns
    cols_lower = {str(c).lower(): c for c in df_enriched.columns}
    contrato_col = cols_lower.get("contrato_x") or cols_lower.get("contrato")

    if contrato_col and contrato_col in df_enriched.columns:
        # Merge history columns (DPD, fechas, estado) onto enriched data
        hist_merge = df_hist[["contract_id", "Fecha Inicial", "Fecha Terminal", "tipo",
                              "dpd_inicial", "dpd_final", "dpd_actual",
                              "dias_atraso_incial", "dias_atraso_terminal",
                              "estado_actual", "estado"]].copy()
        hist_merge = hist_merge.rename(columns={
            "Fecha Inicial": "Asig. Fecha Inicial",
            "Fecha Terminal": "Asig. Fecha Terminal",
            "tipo": "Asig. Tipo",
            "dpd_inicial": "Asig. DPD Inicial",
            "dpd_final": "Asig. DPD Final",
            "dpd_actual": "Asig. DPD Actual",
            "dias_atraso_incial": "Asig. Dias Ini",
            "dias_atraso_terminal": "Asig. Dias Term",
            "estado_actual": "Asig. Estado Actual",
            "estado": "Asig. Estado",
        })

        df_enriched[contrato_col] = pd.to_numeric(df_enriched[contrato_col], errors="coerce")
        hist_merge["contract_id"] = pd.to_numeric(hist_merge["contract_id"], errors="coerce")
        df_final = df_enriched.merge(hist_merge, left_on=contrato_col, right_on="contract_id", how="left")
        df_final = df_final.drop(columns=["contract_id"], errors="ignore")
    else:
        df_final = df_enriched

    reports_dir = _reports_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_INFORME_{house_tag}.xlsx"
    file_path = reports_dir / filename
    df_final.to_excel(str(file_path), index=False, engine="openpyxl")
    logger.info("Informe historico generado: %s (%d filas)", file_path, len(df_final))
    return file_path


def _generate_house_report(user_id: int, user_name: str, house_tag: str, use_history: bool = False) -> Path:
    last_report = _latest_house_report_path(house_tag)

    # If history mode or no active contracts, use lightweight history Excel
    if use_history:
        return _generate_history_excel(user_id, user_name, house_tag)

    house_user_ids = HOUSE_USER_IDS.get(house_tag.lower(), set())
    if house_user_ids:
        contracts = report_service_extended.get_assigned_contracts_for_house(
            list(house_user_ids)
        )
    else:
        contracts = report_service_extended.get_assigned_contracts(user_id)
    if not contracts:
        # No active contracts - try history Excel instead
        try:
            return _generate_history_excel(user_id, user_name, house_tag)
        except ValueError:
            if last_report and last_report.exists():
                return last_report
            raise ValueError(f"No hay contratos para {user_name}")

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
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    min_days: Optional[int] = None,
    max_days: Optional[int] = None,
    status_filter: str = "",
    house_filter: str = "",
    user_id_filter: Optional[int] = None,
    contract_id_filter: Optional[int] = None,
    page: int = 1,
    page_size: int = 100,
) -> dict:
    """Carga reporte de asignados/eliminados desde contract_advisors_history."""
    selected_status = (status_filter or "").strip().upper()
    if selected_status not in {"", "ASIGNADO", "ELIMINADO"}:
        selected_status = ""

    selected_house = (house_filter or "").strip().lower()
    if selected_house not in {"", "cobyser", "serlefin"}:
        selected_house = ""

    safe_page_size = max(10, min(500, int(page_size or 100)))
    safe_page = max(1, int(page or 1))

    # Normaliza dias_atraso_* (que puede venir como texto) a entero de forma segura.
    # Si no es numerico, se considera NULL y no rompe los filtros.
    days_expr = (
        "COALESCE("
        "CASE WHEN NULLIF(TRIM(h.dias_atraso_terminal::text), '') ~ '^-?\\d+$' "
        "THEN TRIM(h.dias_atraso_terminal::text)::int ELSE NULL END, "
        "CASE WHEN NULLIF(TRIM(h.dias_atraso_incial::text), '') ~ '^-?\\d+$' "
        "THEN TRIM(h.dias_atraso_incial::text)::int ELSE NULL END"
        ")"
    )

    where_clauses = ["1=1"]
    params: dict[str, object] = {}

    if start_date is not None:
        where_clauses.append('h."Fecha Inicial"::date >= %(start_date)s')
        params["start_date"] = start_date
    if end_date is not None:
        where_clauses.append('h."Fecha Inicial"::date <= %(end_date)s')
        params["end_date"] = end_date
    if min_days is not None:
        where_clauses.append(f"{days_expr} >= %(min_days)s")
        params["min_days"] = int(min_days)
    if max_days is not None:
        where_clauses.append(f"{days_expr} <= %(max_days)s")
        params["max_days"] = int(max_days)
    if selected_status == "ASIGNADO":
        where_clauses.append('h."Fecha Terminal" IS NULL')
    elif selected_status == "ELIMINADO":
        where_clauses.append('h."Fecha Terminal" IS NOT NULL')
    if selected_house:
        house_user_ids = sorted(HOUSE_USER_IDS.get(selected_house, set()))
        if house_user_ids:
            where_clauses.append(
                "h.user_id IN (" + ",".join(str(int(user_id)) for user_id in house_user_ids) + ")"
            )
    if user_id_filter is not None:
        where_clauses.append("h.user_id = %(user_id_filter)s")
        params["user_id_filter"] = int(user_id_filter)
    if contract_id_filter is not None:
        where_clauses.append("h.contract_id = %(contract_id_filter)s")
        params["contract_id_filter"] = int(contract_id_filter)

    where_sql = " AND ".join(where_clauses)

    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DATABASE,
        port=settings.POSTGRES_PORT,
    )
    try:
        with conn.cursor() as cur:
            serlefin_ids = ",".join(str(int(u)) for u in sorted(HOUSE_USER_IDS.get("serlefin", set())))
            cobyser_ids = ",".join(str(int(u)) for u in sorted(HOUSE_USER_IDS.get("cobyser", set())))

            summary_query = f"""
            SELECT
                COUNT(*)::bigint AS total_rows,
                COUNT(*) FILTER (WHERE h."Fecha Terminal" IS NULL)::bigint AS asignados,
                COUNT(*) FILTER (WHERE h."Fecha Terminal" IS NOT NULL)::bigint AS eliminados,
                COUNT(*) FILTER (WHERE h."Fecha Terminal" IS NULL AND h.user_id IN ({serlefin_ids}))::bigint AS serlefin_asignados,
                COUNT(*) FILTER (WHERE h."Fecha Terminal" IS NULL AND h.user_id IN ({cobyser_ids}))::bigint AS cobyser_asignados
            FROM alocreditindicators.contract_advisors_history h
            WHERE {where_sql}
            """
            cur.execute(summary_query, params)
            summary_row = cur.fetchone() or (0, 0, 0, 0, 0)

            total_rows = int(summary_row[0] or 0)
            asignados = int(summary_row[1] or 0)
            eliminados = int(summary_row[2] or 0)
            serlefin_asignados = int(summary_row[3] or 0)
            cobyser_asignados = int(summary_row[4] or 0)

            total_pages = 1
            if total_rows > 0:
                total_pages = (total_rows + safe_page_size - 1) // safe_page_size
            safe_page = max(1, min(safe_page, total_pages))
            offset = (safe_page - 1) * safe_page_size

            rows_query = f"""
            SELECT
                h.id,
                h.user_id,
                h.contract_id,
                h."Fecha Inicial" AS fecha_inicial,
                h."Fecha Terminal" AS fecha_terminal,
                h.tipo,
                h.dpd_inicial,
                h.dpd_final,
                h.dpd_actual,
                h.dias_atraso_incial,
                h.dias_atraso_terminal,
                COALESCE(NULLIF(TRIM(h.estado_actual::text), ''), 'SIN_ESTADO') AS estado_actual,
                CASE
                    WHEN h."Fecha Terminal" IS NULL THEN 'ASIGNADO'
                    ELSE 'ELIMINADO'
                END AS estado
            FROM alocreditindicators.contract_advisors_history h
            WHERE {where_sql}
            ORDER BY h."Fecha Inicial" DESC, h.id DESC
            LIMIT %(limit)s
            OFFSET %(offset)s
            """
            page_params = dict(params)
            page_params["limit"] = safe_page_size
            page_params["offset"] = offset
            cur.execute(rows_query, page_params)
            rows = cur.fetchall()
    finally:
        conn.close()

    start_date_label = start_date.isoformat() if start_date else "TODAS"
    end_date_label = end_date.isoformat() if end_date else "TODAS"

    return {
        "start_date": start_date.isoformat() if start_date else None,
        "start_date_label": start_date_label,
        "end_date": end_date.isoformat() if end_date else None,
        "end_date_label": end_date_label,
        "min_days": min_days,
        "max_days": max_days,
        "status_filter": selected_status,
        "house_filter": selected_house,
        "user_id_filter": user_id_filter,
        "contract_id_filter": contract_id_filter,
        "total_rows": total_rows,
        "asignados": asignados,
        "eliminados": eliminados,
        "serlefin_asignados": serlefin_asignados,
        "cobyser_asignados": cobyser_asignados,
        "page": safe_page,
        "page_size": safe_page_size,
        "total_pages": total_pages,
        "has_prev": safe_page > 1,
        "has_next": safe_page < total_pages,
        "rows": rows,
    }


def _get_active_promise_contract_ids() -> Set[int]:
    """Retorna set de contract_ids con promesas activas (acuerdo_de_pago con promise_date >= hoy)."""
    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DATABASE,
        port=settings.POSTGRES_PORT,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT m.contract_id::BIGINT
                FROM alocreditindicators.managements m
                WHERE m.effect = %s
                  AND m.promise_date IS NOT NULL
                  AND m.promise_date >= CURRENT_DATE
                """,
                (settings.EFFECT_ACUERDO_PAGO,),
            )
            return {int(row[0]) for row in cur.fetchall()}
    finally:
        conn.close()


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
            ca.user_id,
            ca.contract_id,
            h."Fecha Inicial" AS fecha_inicial,
            h."Fecha Terminal" AS fecha_terminal,
            h.dpd_inicial,
            h.dpd_final,
            h.dias_atraso_incial,
            h.dias_atraso_terminal
        FROM alocreditindicators.contract_advisors ca
        LEFT JOIN LATERAL (
            SELECT
                hh."Fecha Inicial",
                hh."Fecha Terminal",
                hh.dpd_inicial,
                hh.dpd_final,
                hh.dias_atraso_incial,
                hh.dias_atraso_terminal
            FROM alocreditindicators.contract_advisors_history hh
            WHERE hh.user_id = ca.user_id
              AND hh.contract_id = ca.contract_id
            ORDER BY hh."Fecha Inicial" DESC, hh.id DESC
            LIMIT 1
        ) h ON TRUE
        WHERE ca.user_id IN ({users_sql})
          AND (
              h."Fecha Inicial" IS NULL
              OR h."Fecha Inicial"::date >= %s
          )
        ORDER BY COALESCE(h."Fecha Inicial", NOW()) DESC
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
        # Este reporte es SOLO de asignaciones activas (tabla contract_advisors).
        estado = "ASIGNADO"

        target = house_reports[house_key]
        target["records"] += 1
        target["asignados"] += 1

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


def _render_mora_rotation_report_html(report: dict) -> str:
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

    back_path = f"/"
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
      <div class="kpi">Asignaciones activas analizadas: <strong>{report["total_rows"]}</strong></div>
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


def _render_assignment_history_report_html(
    report: dict,
    title: str = "Historial de Asignaciones",
) -> str:
    """Renderiza HTML moderno del informe de asignados/eliminados con AJAX."""
    selected_status = str(report.get("status_filter") or "")
    selected_house = str(report.get("house_filter") or "")
    selected_user_id = report.get("user_id_filter")
    selected_contract_id = report.get("contract_id_filter")

    filter_start_date = str(report.get("start_date") or "")
    filter_end_date = str(report.get("end_date") or "")
    filter_min_days = str(report.get("min_days")) if report.get("min_days") is not None else ""
    filter_max_days = str(report.get("max_days")) if report.get("max_days") is not None else ""
    filter_user_id = str(selected_user_id) if selected_user_id is not None else ""
    filter_contract_id = str(selected_contract_id) if selected_contract_id is not None else ""

    download_path = f"/history/asignados-eliminados/download"
    download_params: dict[str, str | int] = {}
    if report.get("start_date"):
        download_params["start_date"] = str(report["start_date"])
    if report.get("end_date"):
        download_params["end_date"] = str(report["end_date"])
    if report.get("min_days") is not None:
        download_params["min_days"] = int(report["min_days"])
    if report.get("max_days") is not None:
        download_params["max_days"] = int(report["max_days"])
    if selected_status:
        download_params["status"] = selected_status
    if selected_house:
        download_params["house"] = selected_house
    if selected_user_id is not None:
        download_params["user_id"] = int(selected_user_id)
    if selected_contract_id is not None:
        download_params["contract_id"] = int(selected_contract_id)
    download_url = f"{download_path}?{urlencode(download_params)}" if download_params else download_path

    back_path = f"/"
    api_base = f"/api/history"

    # Build initial data JSON for SSR hydration
    initial_rows_json = []
    for row in report["rows"]:
        (row_id, uid, cid, fi, ft, tipo, dpd_i, dpd_f, dpd_a, di, dt, ea, est) = row
        initial_rows_json.append({
            "id": row_id, "user_id": uid, "user_label": _format_user_label(uid),
            "contract_id": cid, "fecha_inicial": _format_datetime(fi),
            "fecha_terminal": _format_datetime(ft), "tipo": str(tipo or "-"),
            "dpd_inicial": str(dpd_i or "-"), "dpd_final": str(dpd_f or "-"),
            "dpd_actual": str(dpd_a or "-"), "dias_inicial": di,
            "dias_terminal": dt, "estado_actual": str(ea or "-"), "estado": est,
        })

    import json as _json
    initial_data = _json.dumps({
        "total_rows": report["total_rows"], "asignados": report["asignados"],
        "eliminados": report["eliminados"],
        "serlefin_asignados": report.get("serlefin_asignados", 0),
        "cobyser_asignados": report.get("cobyser_asignados", 0),
        "page": report["page"],
        "page_size": report["page_size"], "total_pages": report["total_pages"],
        "has_prev": report["has_prev"], "has_next": report["has_next"],
        "rows": initial_rows_json,
    })

    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
  <style>
    :root {{
      --bg: #F5F5F7;
      --surface: #FFFFFF;
      --surface2: #F9F9F9;
      --border: #E5E7EB;
      --border-light: #F0F0F0;
      --text: #3e4a60;
      --text-secondary: #5A6B8C;
      --accent: #FF8C42;
      --accent-hover: #FF7A28;
      --accent-soft: rgba(255,140,66,0.12);
      --green: #10B981;
      --green-soft: #ECFDF5;
      --red: #EF4444;
      --red-soft: #FEF2F2;
      --amber: #F59E0B;
      --amber-soft: #FFF5ED;
      --blue: #3B82F6;
      --blue-soft: #EEF2FF;
      --teal: #14B8A6;
      --teal-soft: rgba(20,184,166,0.12);
      --radius: 12px;
      --radius-sm: 8px;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      -webkit-font-smoothing: antialiased;
    }}

    .topbar {{
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 0 24px;
      height: 56px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky;
      top: 0;
      z-index: 100;
      backdrop-filter: blur(12px);
    }}
    .topbar-brand {{ display: flex; align-items: center; gap: 10px; }}
    .topbar-brand .logo {{
      width: 28px; height: 28px;
      background: linear-gradient(135deg, #FF8C42, #FF7A28);
      border-radius: 8px;
      display: grid; place-items: center;
      font-weight: 800; font-size: 14px; color: #fff;
    }}
    .topbar h1 {{ font-size: 15px; font-weight: 600; letter-spacing: -0.01em; }}
    .topbar-actions {{ display: flex; gap: 8px; align-items: center; }}
    .topbar a, .topbar button {{
      font-family: inherit; font-size: 13px; font-weight: 500;
      padding: 7px 14px; border-radius: var(--radius-sm);
      border: 1px solid var(--border); background: var(--surface2);
      color: var(--text); cursor: pointer; text-decoration: none; transition: all 0.15s;
    }}
    .topbar a:hover, .topbar button:hover {{ background: var(--border); }}
    .btn-primary {{ background: var(--accent) !important; border-color: var(--accent) !important; color: #fff !important; }}
    .btn-primary:hover {{ background: var(--accent-hover) !important; }}
    .btn-excel {{ background: #16a34a !important; border-color: #FF8C42 !important; color: #fff !important; }}

    .container {{ max-width: 1600px; margin: 0 auto; padding: 20px 24px; }}

    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(6, 1fr);
      gap: 12px;
      margin-bottom: 20px;
    }}
    .kpi-card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 18px 20px;
      transition: border-color 0.2s, transform 0.15s;
    }}
    .kpi-card:hover {{ border-color: var(--border-light); transform: translateY(-1px); }}
    .kpi-label {{
      font-size: 11px; font-weight: 600; text-transform: uppercase;
      letter-spacing: 0.06em; color: var(--text-secondary); margin-bottom: 8px;
    }}
    .kpi-value {{
      font-size: 28px; font-weight: 700; letter-spacing: -0.02em;
      font-variant-numeric: tabular-nums;
    }}
    .kpi-value.green {{ color: var(--green); }}
    .kpi-value.red {{ color: var(--red); }}
    .kpi-value.blue {{ color: var(--blue); }}
    .kpi-value.teal {{ color: var(--teal); }}

    .filters-panel {{
      background: var(--surface); border: 1px solid var(--border);
      border-radius: var(--radius); padding: 16px 20px; margin-bottom: 16px;
    }}
    .filters-toggle {{
      display: flex; align-items: center; justify-content: space-between;
      cursor: pointer; user-select: none;
    }}
    .filters-toggle h3 {{
      font-size: 13px; font-weight: 600; text-transform: uppercase;
      letter-spacing: 0.04em; color: var(--text-secondary);
    }}
    .filters-toggle .arrow {{
      font-size: 18px; color: var(--text-secondary); transition: transform 0.2s;
    }}
    .filters-toggle.open .arrow {{ transform: rotate(180deg); }}
    .filters-body {{
      display: grid; grid-template-columns: repeat(auto-fit, minmax(155px, 1fr));
      gap: 12px; margin-top: 14px; padding-top: 14px; border-top: 1px solid var(--border);
    }}
    .filter-group label {{
      display: block; font-size: 11px; font-weight: 600; text-transform: uppercase;
      letter-spacing: 0.04em; color: var(--text-secondary); margin-bottom: 5px;
    }}
    .filter-group input, .filter-group select {{
      width: 100%; font-family: inherit; font-size: 13px; padding: 8px 10px;
      background: var(--surface2); border: 1px solid var(--border);
      border-radius: var(--radius-sm); color: var(--text); outline: none; transition: border-color 0.15s;
    }}
    .filter-group input:focus, .filter-group select:focus {{
      border-color: var(--accent); box-shadow: 0 0 0 2px var(--accent-soft);
    }}
    .filter-group select {{ cursor: pointer; }}
    .filter-group select option {{ background: var(--surface2); color: var(--text); }}
    .filters-actions {{ grid-column: 1 / -1; display: flex; gap: 8px; padding-top: 4px; }}
    .btn-filter {{
      font-family: inherit; font-size: 13px; font-weight: 600;
      padding: 8px 20px; border-radius: var(--radius-sm); border: none; cursor: pointer; transition: all 0.15s;
    }}
    .btn-apply {{ background: var(--accent); color: #fff; }}
    .btn-apply:hover {{ background: var(--accent-hover); }}
    .btn-clear {{ background: var(--surface2); color: var(--text-secondary); border: 1px solid var(--border); }}
    .btn-clear:hover {{ color: var(--text); background: var(--border); }}

    .search-bar {{ position: relative; margin-bottom: 16px; }}
    .search-bar input {{
      width: 100%; font-family: inherit; font-size: 14px; padding: 10px 14px 10px 38px;
      background: var(--surface); border: 1px solid var(--border);
      border-radius: var(--radius); color: var(--text); outline: none; transition: border-color 0.15s;
    }}
    .search-bar input:focus {{ border-color: var(--accent); box-shadow: 0 0 0 2px var(--accent-soft); }}
    .search-bar input::placeholder {{ color: var(--text-secondary); }}
    .search-bar svg {{ position: absolute; left: 12px; top: 50%; transform: translateY(-50%); color: var(--text-secondary); }}

    .table-container {{
      background: var(--surface); border: 1px solid var(--border);
      border-radius: var(--radius); overflow: hidden;
    }}
    .table-scroll {{ overflow-x: auto; max-height: 70vh; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1300px; }}
    thead {{ position: sticky; top: 0; z-index: 10; }}
    th {{
      background: var(--surface2); color: var(--text-secondary);
      font-size: 11px; font-weight: 600; text-transform: uppercase;
      letter-spacing: 0.05em; padding: 10px 12px; text-align: left;
      border-bottom: 1px solid var(--border); white-space: nowrap;
      cursor: pointer; user-select: none; transition: color 0.15s;
    }}
    th:hover {{ color: var(--text); }}
    th .sort-icon {{ display: inline-block; margin-left: 4px; opacity: 0.3; font-size: 10px; }}
    th.sorted .sort-icon {{ opacity: 1; color: var(--accent); }}
    td {{
      padding: 9px 12px; font-size: 13px; border-bottom: 1px solid var(--border);
      white-space: nowrap; font-variant-numeric: tabular-nums;
    }}
    tbody tr {{ transition: background 0.1s; }}
    tbody tr:hover {{ background: var(--surface2); }}

    .badge {{
      display: inline-block; padding: 3px 10px; border-radius: 999px;
      font-size: 11px; font-weight: 600; letter-spacing: 0.02em;
    }}
    .badge-asignado {{ background: var(--green-soft); color: var(--green); }}
    .badge-eliminado {{ background: var(--red-soft); color: var(--red); }}
    .badge-fijo {{ background: var(--amber-soft); color: var(--amber); }}
    .badge-tipo {{ background: var(--blue-soft); color: var(--blue); }}
    .badge-cierre {{ background: var(--red-soft); color: var(--red); }}
    .badge-blacklist {{ background: #7c3aed20; color: #a78bfa; }}

    .pagination {{
      display: flex; align-items: center; justify-content: space-between;
      padding: 12px 16px; border-top: 1px solid var(--border);
      font-size: 13px; color: var(--text-secondary);
    }}
    .pagination .page-info strong {{ color: var(--text); }}
    .page-buttons {{ display: flex; gap: 4px; }}
    .page-btn {{
      font-family: inherit; font-size: 13px; font-weight: 500;
      padding: 6px 12px; border-radius: var(--radius-sm);
      border: 1px solid var(--border); background: var(--surface2);
      color: var(--text); cursor: pointer; transition: all 0.15s;
    }}
    .page-btn:hover:not(:disabled) {{ background: var(--border); }}
    .page-btn:disabled {{ opacity: 0.3; cursor: default; }}
    .page-btn.active {{ background: var(--accent); border-color: var(--accent); color: #fff; }}

    .loading-overlay {{
      display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(255,255,255,0.7); backdrop-filter: blur(4px);
      z-index: 200; align-items: center; justify-content: center;
    }}
    .loading-overlay.active {{ display: flex; }}
    .spinner {{
      width: 40px; height: 40px; border: 3px solid var(--border);
      border-top-color: var(--accent); border-radius: 50%;
      animation: spin 0.7s linear infinite;
    }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}

    @media (max-width: 900px) {{
      .container {{ padding: 12px; }}
      .kpi-grid {{ grid-template-columns: repeat(3, 1fr); }}
      .topbar {{ padding: 0 12px; }}
    }}
  </style>
</head>
<body>
  <div class="loading-overlay" id="loadingOverlay"><div class="spinner"></div></div>

  <nav class="topbar">
    <div class="topbar-brand">
      <div class="logo">A</div>
      <h1>Alo Credit</h1>
    </div>
    <div class="topbar-actions" style="display:flex;gap:8px;align-items:center">
      <a href="{html.escape(back_path)}/dashboard" style="font-family:inherit;font-size:13px;font-weight:500;padding:7px 14px;border-radius:10px;border:1px solid var(--border);background:var(--surface);color:var(--text);text-decoration:none">Dashboard</a>
      <a href="#" style="font-family:inherit;font-size:13px;font-weight:500;padding:7px 14px;border-radius:10px;border:1px solid var(--accent);background:var(--accent);color:#fff;text-decoration:none">Historial</a>
      <a href="{html.escape(back_path)}/config" style="font-family:inherit;font-size:13px;font-weight:500;padding:7px 14px;border-radius:10px;border:1px solid var(--border);background:var(--surface);color:var(--text);text-decoration:none">Configuracion</a>
      <a href="{html.escape(download_url)}" style="font-family:inherit;font-size:13px;font-weight:600;padding:7px 14px;border-radius:10px;border:none;background:var(--accent);color:#fff;text-decoration:none">Descargar Excel</a>
    </div>
  </nav>

  <div class="container">
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Total Registros</div>
        <div class="kpi-value teal" id="kpiTotal">{report["total_rows"]:,}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Asignados</div>
        <div class="kpi-value green" id="kpiAsignados">{report["asignados"]:,}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Eliminados</div>
        <div class="kpi-value red" id="kpiEliminados">{report["eliminados"]:,}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Serlefin Asignados</div>
        <div class="kpi-value blue" id="kpiSerlefin">{report.get("serlefin_asignados", 0):,}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Cobyser Asignados</div>
        <div class="kpi-value" id="kpiCobyser" style="color:#8B5CF6">{report.get("cobyser_asignados", 0):,}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Pagina</div>
        <div class="kpi-value" id="kpiPage">{int(report.get("page",1) or 1)}<span style="font-size:14px;color:var(--text-secondary)"> / {int(report.get("total_pages",1) or 1)}</span></div>
      </div>
    </div>

    <div class="filters-panel">
      <div class="filters-toggle open" id="filtersToggle" onclick="toggleFilters()">
        <h3>Filtros avanzados</h3>
        <span class="arrow">&#9660;</span>
      </div>
      <div class="filters-body" id="filtersBody">
        <div class="filter-group">
          <label>Fecha Inicio</label>
          <input type="date" id="fStartDate" value="{html.escape(filter_start_date)}" />
        </div>
        <div class="filter-group">
          <label>Fecha Fin</label>
          <input type="date" id="fEndDate" value="{html.escape(filter_end_date)}" />
        </div>
        <div class="filter-group">
          <label>Dias Min</label>
          <input type="number" id="fMinDays" min="0" value="{html.escape(filter_min_days)}" placeholder="61" />
        </div>
        <div class="filter-group">
          <label>Dias Max</label>
          <input type="number" id="fMaxDays" min="0" value="{html.escape(filter_max_days)}" placeholder="240" />
        </div>
        <div class="filter-group">
          <label>Estado</label>
          <select id="fStatus">
            <option value="" {"selected" if not selected_status else ""}>Todos</option>
            <option value="ASIGNADO" {"selected" if selected_status == "ASIGNADO" else ""}>Asignado</option>
            <option value="ELIMINADO" {"selected" if selected_status == "ELIMINADO" else ""}>Eliminado</option>
          </select>
        </div>
        <div class="filter-group">
          <label>Casa</label>
          <select id="fHouse">
            <option value="" {"selected" if not selected_house else ""}>Todas</option>
            <option value="cobyser" {"selected" if selected_house == "cobyser" else ""}>Cobyser</option>
            <option value="serlefin" {"selected" if selected_house == "serlefin" else ""}>Serlefin</option>
          </select>
        </div>
        <div class="filter-group">
          <label>User ID</label>
          <input type="number" id="fUserId" min="1" value="{html.escape(filter_user_id)}" placeholder="45" />
        </div>
        <div class="filter-group">
          <label>Contrato</label>
          <input type="number" id="fContractId" min="1" value="{html.escape(filter_contract_id)}" placeholder="ID" />
        </div>
        <div class="filter-group">
          <label>Filas / Pagina</label>
          <select id="fPageSize">
            <option value="25" {"selected" if int(report.get("page_size",100) or 100) == 25 else ""}>25</option>
            <option value="50" {"selected" if int(report.get("page_size",100) or 100) == 50 else ""}>50</option>
            <option value="100" {"selected" if int(report.get("page_size",100) or 100) == 100 else ""}>100</option>
            <option value="200" {"selected" if int(report.get("page_size",100) or 100) == 200 else ""}>200</option>
          </select>
        </div>
        <div class="filters-actions">
          <button class="btn-filter btn-apply" onclick="applyFilters()">Aplicar Filtros</button>
          <button class="btn-filter btn-clear" onclick="clearFilters()">Limpiar</button>
        </div>
      </div>
    </div>

    <div class="search-bar">
      <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
      <input type="text" id="quickSearch" placeholder="Buscar en esta pagina (contrato, casa, tipo, estado...)" />
    </div>

    <div class="table-container">
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th data-col="id" onclick="sortBy('id')">ID <span class="sort-icon">&#9650;</span></th>
              <th data-col="user_label" onclick="sortBy('user_label')">Casa <span class="sort-icon">&#9650;</span></th>
              <th data-col="contract_id" onclick="sortBy('contract_id')">Contrato <span class="sort-icon">&#9650;</span></th>
              <th data-col="fecha_inicial" onclick="sortBy('fecha_inicial')">F. Inicial <span class="sort-icon">&#9650;</span></th>
              <th data-col="fecha_terminal" onclick="sortBy('fecha_terminal')">F. Terminal <span class="sort-icon">&#9650;</span></th>
              <th data-col="tipo" onclick="sortBy('tipo')">Tipo <span class="sort-icon">&#9650;</span></th>
              <th data-col="dpd_inicial" onclick="sortBy('dpd_inicial')">DPD Ini <span class="sort-icon">&#9650;</span></th>
              <th data-col="dpd_final" onclick="sortBy('dpd_final')">DPD Fin <span class="sort-icon">&#9650;</span></th>
              <th data-col="dpd_actual" onclick="sortBy('dpd_actual')">DPD Act <span class="sort-icon">&#9650;</span></th>
              <th data-col="dias_inicial" onclick="sortBy('dias_inicial')">Dias Ini <span class="sort-icon">&#9650;</span></th>
              <th data-col="dias_terminal" onclick="sortBy('dias_terminal')">Dias Term <span class="sort-icon">&#9650;</span></th>
              <th data-col="estado_actual" onclick="sortBy('estado_actual')">Estado Actual <span class="sort-icon">&#9650;</span></th>
              <th data-col="estado" onclick="sortBy('estado')">Estado <span class="sort-icon">&#9650;</span></th>
            </tr>
          </thead>
          <tbody id="tableBody"></tbody>
        </table>
      </div>
      <div class="pagination" id="pagination"></div>
    </div>
  </div>

  <script>
    const API = '{html.escape(api_base)}';
    let state = {{
      data: {initial_data},
      sortCol: null,
      sortDir: 'asc',
      searchTerm: '',
    }};

    function esc(s) {{ const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }}

    function tipoBadge(tipo) {{
      const t = (tipo || '').toUpperCase();
      if (t.includes('FIJO')) return '<span class="badge badge-fijo">' + esc(tipo) + '</span>';
      if (t.includes('CIERRE')) return '<span class="badge badge-cierre">' + esc(tipo) + '</span>';
      if (t.includes('BLACKLIST')) return '<span class="badge badge-blacklist">' + esc(tipo) + '</span>';
      if (t === '-') return '-';
      return '<span class="badge badge-tipo">' + esc(tipo) + '</span>';
    }}

    function estadoBadge(estado) {{
      return estado === 'ASIGNADO'
        ? '<span class="badge badge-asignado">ASIGNADO</span>'
        : '<span class="badge badge-eliminado">ELIMINADO</span>';
    }}

    function renderRows(rows) {{
      const tbody = document.getElementById('tableBody');
      if (!rows.length) {{
        tbody.innerHTML = '<tr><td colspan="13" style="text-align:center;padding:48px;color:var(--text-secondary);font-size:14px">Sin registros para los filtros seleccionados</td></tr>';
        return;
      }}
      let h = '';
      for (const r of rows) {{
        h += '<tr>' +
          '<td style="color:var(--text-secondary)">' + r.id + '</td>' +
          '<td><strong>' + esc(r.user_label) + '</strong></td>' +
          '<td><strong style="color:var(--teal)">' + r.contract_id + '</strong></td>' +
          '<td>' + esc(r.fecha_inicial) + '</td>' +
          '<td>' + esc(r.fecha_terminal) + '</td>' +
          '<td>' + tipoBadge(r.tipo) + '</td>' +
          '<td>' + esc(r.dpd_inicial) + '</td>' +
          '<td>' + esc(r.dpd_final) + '</td>' +
          '<td>' + esc(r.dpd_actual) + '</td>' +
          '<td>' + (r.dias_inicial != null ? r.dias_inicial : '-') + '</td>' +
          '<td>' + (r.dias_terminal != null ? r.dias_terminal : '-') + '</td>' +
          '<td>' + esc(r.estado_actual) + '</td>' +
          '<td>' + estadoBadge(r.estado) + '</td></tr>';
      }}
      tbody.innerHTML = h;
    }}

    function renderPagination(d) {{
      const pg = document.getElementById('pagination');
      if (!d.total_rows) {{ pg.innerHTML = ''; return; }}
      const p = d.page, tp = d.total_pages;
      let b = '';
      b += '<button class="page-btn" onclick="goPage(1)"' + (p<=1?' disabled':'') + '>&#171;</button>';
      b += '<button class="page-btn" onclick="goPage('+(p-1)+')"' + (p<=1?' disabled':'') + '>Ant</button>';
      const s = Math.max(1, p-3), e = Math.min(tp, p+3);
      for (let i = s; i <= e; i++) b += '<button class="page-btn'+(i===p?' active':'')+'" onclick="goPage('+i+')">'+i+'</button>';
      b += '<button class="page-btn" onclick="goPage('+(p+1)+')"' + (p>=tp?' disabled':'') + '>Sig</button>';
      b += '<button class="page-btn" onclick="goPage('+tp+')"' + (p>=tp?' disabled':'') + '>&#187;</button>';
      pg.innerHTML = '<span class="page-info"><strong>' + d.rows.length + '</strong> de <strong>' + d.total_rows.toLocaleString() + '</strong> registros</span><div class="page-buttons">' + b + '</div>';
    }}

    function updateKPIs(d) {{
      document.getElementById('kpiTotal').textContent = d.total_rows.toLocaleString();
      document.getElementById('kpiAsignados').textContent = d.asignados.toLocaleString();
      document.getElementById('kpiEliminados').textContent = d.eliminados.toLocaleString();
      document.getElementById('kpiSerlefin').textContent = (d.serlefin_asignados || 0).toLocaleString();
      document.getElementById('kpiCobyser').textContent = (d.cobyser_asignados || 0).toLocaleString();
      document.getElementById('kpiPage').innerHTML = d.page + '<span style="font-size:14px;color:var(--text-secondary)"> / ' + d.total_pages + '</span>';
    }}

    function getFilteredRows() {{
      let rows = state.data.rows;
      if (state.searchTerm) {{
        const q = state.searchTerm.toLowerCase();
        rows = rows.filter(r => Object.values(r).some(v => String(v).toLowerCase().includes(q)));
      }}
      if (state.sortCol) {{
        rows = [...rows].sort((a, b) => {{
          let va = a[state.sortCol], vb = b[state.sortCol];
          if (va == null) va = ''; if (vb == null) vb = '';
          if (typeof va === 'number' && typeof vb === 'number') return state.sortDir === 'asc' ? va - vb : vb - va;
          return state.sortDir === 'asc' ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
        }});
      }}
      return rows;
    }}

    function render() {{ renderRows(getFilteredRows()); renderPagination(state.data); updateKPIs(state.data); }}

    function sortBy(col) {{
      document.querySelectorAll('th').forEach(th => {{ th.classList.remove('sorted'); th.querySelector('.sort-icon').innerHTML = '&#9650;'; }});
      if (state.sortCol === col) state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
      else {{ state.sortCol = col; state.sortDir = 'asc'; }}
      const th = document.querySelector('th[data-col="'+col+'"]');
      if (th) {{ th.classList.add('sorted'); th.querySelector('.sort-icon').innerHTML = state.sortDir === 'asc' ? '&#9650;' : '&#9660;'; }}
      render();
    }}

    function toggleFilters() {{
      const t = document.getElementById('filtersToggle');
      const b = document.getElementById('filtersBody');
      t.classList.toggle('open');
      b.style.display = t.classList.contains('open') ? 'grid' : 'none';
    }}

    async function fetchData(page) {{
      document.getElementById('loadingOverlay').classList.add('active');
      const p = new URLSearchParams();
      p.set('page', page || 1);
      p.set('page_size', document.getElementById('fPageSize').value);
      ['fStartDate:start_date','fEndDate:end_date','fMinDays:min_days','fMaxDays:max_days',
       'fStatus:status','fHouse:house','fUserId:user_id','fContractId:contract_id'].forEach(pair => {{
        const [id, key] = pair.split(':');
        const v = document.getElementById(id).value;
        if (v) p.set(key, v);
      }});
      try {{
        const resp = await fetch(API + '?' + p.toString());
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        state.data = await resp.json();
        state.sortCol = null; state.sortDir = 'asc'; state.searchTerm = '';
        document.getElementById('quickSearch').value = '';
        document.querySelectorAll('th').forEach(th => th.classList.remove('sorted'));
        render();
      }} catch (e) {{ alert('Error: ' + e.message); }}
      document.getElementById('loadingOverlay').classList.remove('active');
    }}

    function applyFilters() {{ fetchData(1); }}
    function goPage(p) {{ fetchData(p); }}
    function clearFilters() {{
      ['fStartDate','fEndDate','fMinDays','fMaxDays','fUserId','fContractId'].forEach(id => document.getElementById(id).value = '');
      ['fStatus','fHouse'].forEach(id => document.getElementById(id).value = '');
      document.getElementById('fPageSize').value = '100';
      fetchData(1);
    }}

    let st;
    document.getElementById('quickSearch').addEventListener('input', function() {{
      clearTimeout(st);
      st = setTimeout(() => {{ state.searchTerm = this.value.trim(); renderRows(getFilteredRows()); }}, 150);
    }});

    document.querySelectorAll('.filter-group input, .filter-group select').forEach(el => {{
      el.addEventListener('keydown', e => {{ if (e.key === 'Enter') applyFilters(); }});
    }});

    render();
  </script>
</body>
</html>
"""


def _render_panel_html(
    *,
    ok_message: str,
    error_message: str,
    current_user: str,
    load_mora_auto: bool,
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
        audit_html_rows = "<tr><td colspan='7' style='text-align:center;padding:24px;color:#8b92a5'>Sin cambios registrados aun.</td></tr>"

    action_path = f"/save"
    download_serlefin_path = f"/reports/serlefin"
    download_cobyser_path = f"/reports/cobyser"
    default_start_date = _current_month_start().isoformat()
    default_end_date = datetime.now().date().isoformat()
    assignment_history_path = (
        f"/history/asignados-eliminados"
        f"?start_date={default_start_date}&end_date={default_end_date}"
    )
    mora_rotation_path = (
        f"/history/rotacion-mora?start_date={default_start_date}"
    )
    run_assignment_now_path = f"/run-assignment-now"
    finalize_assignments_path = f"/finalize-assignments"
    validate_db_processes_path = f"/validate-db-processes"
    upload_blacklist_path = f"/blacklist/upload"
    download_blacklist_path = f"/blacklist/download"
    logout_path = f"/logout"
    blacklist_status = blacklist_service.status()

    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Panel de Asignacion - Alo Credit</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet" />
  <style>
    :root {{
      --bg: #F5F5F7;
      --surface: #FFFFFF;
      --surface2: #F9F9F9;
      --border: #E5E7EB;
      --border-light: #F0F0F0;
      --text: #3e4a60;
      --text-secondary: #5A6B8C;
      --accent: #FF8C42;
      --accent-hover: #FF7A28;
      --accent-soft: rgba(255,140,66,0.12);
      --green: #10B981;
      --green-soft: #ECFDF5;
      --red: #EF4444;
      --red-soft: #FEF2F2;
      --amber: #F59E0B;
      --amber-soft: #FFF5ED;
      --blue: #3B82F6;
      --blue-soft: #EEF2FF;
      --teal: #14B8A6;
      --teal-soft: rgba(20,184,166,0.12);
      --radius: 12px;
      --radius-sm: 8px;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      -webkit-font-smoothing: antialiased;
    }}

    .topbar {{
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 0 24px;
      height: 56px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky;
      top: 0;
      z-index: 100;
    }}
    .topbar-brand {{ display: flex; align-items: center; gap: 10px; }}
    .topbar-brand .logo {{
      width: 28px; height: 28px;
      background: linear-gradient(135deg, #FF8C42, #FF7A28);
      border-radius: 8px; display: grid; place-items: center;
      font-weight: 800; font-size: 14px; color: #fff;
    }}
    .topbar h1 {{ font-size: 15px; font-weight: 600; }}
    .topbar-right {{ display: flex; gap: 10px; align-items: center; font-size: 13px; color: var(--text-secondary); }}
    .topbar-right strong {{ color: var(--text); }}
    .btn-sm {{
      font-family: inherit; font-size: 12px; font-weight: 600;
      padding: 6px 12px; border-radius: var(--radius-sm);
      border: 1px solid var(--border); background: var(--surface2);
      color: var(--text); cursor: pointer; text-decoration: none; transition: all 0.15s;
    }}
    .btn-sm:hover {{ background: var(--border); }}
    .btn-danger {{ background: rgba(239,68,68,0.15) !important; border-color: rgba(239,68,68,0.3) !important; color: #fca5a5 !important; }}

    .container {{ max-width: 1300px; margin: 0 auto; padding: 20px 24px; }}

    .alert {{
      border-radius: var(--radius); padding: 14px 18px;
      margin-bottom: 16px; font-size: 13px; font-weight: 500;
      animation: fadeIn 0.3s ease;
    }}
    .alert.ok {{ background: var(--green-soft); color: var(--green); border: 1px solid rgba(34,197,94,0.2); }}
    .alert.error {{ background: var(--red-soft); color: #fca5a5; border: 1px solid rgba(239,68,68,0.2); }}

    /* KPI Cards */
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }}
    .kpi-card {{
      background: var(--surface); border: 1px solid var(--border);
      border-radius: var(--radius); padding: 16px 18px;
    }}
    .kpi-label {{
      font-size: 11px; font-weight: 600; text-transform: uppercase;
      letter-spacing: 0.05em; color: var(--text-secondary); margin-bottom: 6px;
    }}
    .kpi-value {{
      font-size: 22px; font-weight: 700;
      font-variant-numeric: tabular-nums;
    }}
    .kpi-value.teal {{ color: var(--teal); }}
    .kpi-value.accent {{ color: var(--accent); }}
    .kpi-sub {{ font-size: 12px; color: var(--text-secondary); margin-top: 4px; }}

    /* Cards */
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(380px, 1fr)); gap: 16px; margin-bottom: 20px; }}
    .card {{
      background: var(--surface); border: 1px solid var(--border);
      border-radius: var(--radius); padding: 20px;
    }}
    .card h2 {{
      font-size: 13px; font-weight: 700; text-transform: uppercase;
      letter-spacing: 0.05em; color: var(--text-secondary);
      margin-bottom: 16px; padding-bottom: 10px; border-bottom: 1px solid var(--border);
    }}
    .form-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }}
    label {{
      display: block; font-size: 11px; font-weight: 600;
      text-transform: uppercase; letter-spacing: 0.04em;
      color: var(--text-secondary); margin-bottom: 5px;
    }}
    input, textarea, select {{
      width: 100%; font-family: inherit; font-size: 13px;
      padding: 9px 12px; background: var(--surface2);
      border: 1px solid var(--border); border-radius: var(--radius-sm);
      color: var(--text); outline: none; transition: border-color 0.15s;
    }}
    input:focus, textarea:focus {{ border-color: var(--accent); box-shadow: 0 0 0 2px var(--accent-soft); }}
    textarea {{ resize: vertical; min-height: 60px; }}

    .btn {{
      font-family: inherit; font-size: 13px; font-weight: 600;
      padding: 9px 18px; border-radius: var(--radius-sm);
      border: none; cursor: pointer; transition: all 0.15s;
      display: inline-block; text-decoration: none; text-align: center;
      margin-top: 8px; margin-right: 6px;
    }}
    .btn-primary {{ background: var(--accent); color: #fff; }}
    .btn-primary:hover {{ background: var(--accent-hover); }}
    .btn-secondary {{ background: var(--surface2); color: var(--text); border: 1px solid var(--border); }}
    .btn-secondary:hover {{ background: var(--border); }}
    .btn-green {{ background: var(--green); color: #fff; }}
    .btn-blue {{ background: var(--blue); color: #fff; }}
    .btn-red {{ background: #dc2626; color: #fff; }}
    .btn-red:hover {{ background: #ef4444; }}
    .btn-teal {{ background: var(--teal); color: #fff; }}
    .btn-block {{ display: block; width: 100%; text-align: center; }}

    .section-divider {{
      border: none; border-top: 1px solid var(--border);
      margin: 16px 0;
    }}

    /* Info badges */
    .info-row {{
      display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 10px;
    }}
    .info-chip {{
      font-size: 12px; padding: 5px 10px;
      background: var(--surface2); border: 1px solid var(--border);
      border-radius: var(--radius-sm); color: var(--text-secondary);
    }}
    .info-chip strong {{ color: var(--text); }}

    /* Table section */
    .table-section {{
      background: var(--surface); border: 1px solid var(--border);
      border-radius: var(--radius); overflow: hidden; margin-bottom: 20px;
    }}
    .table-header {{
      padding: 14px 18px; border-bottom: 1px solid var(--border);
      background: var(--surface2);
      font-size: 13px; font-weight: 700; text-transform: uppercase;
      letter-spacing: 0.04em; color: var(--text-secondary);
    }}
    .table-scroll {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 800px; }}
    th {{
      background: var(--surface2); color: var(--text-secondary);
      font-size: 11px; font-weight: 600; text-transform: uppercase;
      letter-spacing: 0.05em; padding: 10px 14px; text-align: left;
      border-bottom: 1px solid var(--border);
    }}
    td {{
      padding: 10px 14px; font-size: 13px; border-bottom: 1px solid var(--border);
      color: var(--text);
    }}
    tbody tr:hover {{ background: var(--surface2); }}

    @keyframes fadeIn {{ from {{ opacity: 0; transform: translateY(-4px); }} to {{ opacity: 1; transform: translateY(0); }} }}
    @media (max-width: 900px) {{
      .container {{ padding: 12px; }}
      .grid {{ grid-template-columns: 1fr; }}
      .form-row {{ grid-template-columns: 1fr; }}
      .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
    }}
  </style>
</head>
<body>
  <nav class="topbar">
    <div class="topbar-brand">
      <div class="logo">A</div>
      <h1>Panel de Asignacion</h1>
    </div>
    <div class="topbar-right" style="display:flex;gap:8px;align-items:center">
      <a href="/dashboard" style="font-family:inherit;font-size:13px;font-weight:500;padding:7px 14px;border-radius:10px;border:1px solid var(--border);background:var(--surface2);color:var(--text);text-decoration:none">Dashboard</a>
      <a href="{html.escape(assignment_history_path)}" style="font-family:inherit;font-size:13px;font-weight:500;padding:7px 14px;border-radius:10px;border:1px solid var(--border);background:var(--surface2);color:var(--text);text-decoration:none">Historial</a>
      <span style="font-size:12px;color:var(--text-secondary)">Sesion: <strong>{html.escape(current_user)}</strong></span>
      <form method="post" action="{html.escape(logout_path)}" style="display:inline">
        <button class="btn-sm btn-danger" type="submit">Salir</button>
      </form>
    </div>
  </nav>

  <div class="container">
    {message_block}

    <!-- KPIs -->
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Serlefin %</div>
        <div class="kpi-value teal">{config.serlefin_percent:.2f}%</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Cobyser %</div>
        <div class="kpi-value accent">{config.cobyser_percent:.2f}%</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Rango Minimo</div>
        <div class="kpi-value">{int(config.min_days)}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Rango Maximo</div>
        <div class="kpi-value">{int(config.max_days)}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Actualizado por</div>
        <div class="kpi-sub" style="font-size:13px;margin-top:8px"><strong>{html.escape(config.updated_by)}</strong></div>
        <div class="kpi-sub">{html.escape(_format_datetime(config.updated_at))}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Blacklist</div>
        <div class="kpi-value" style="font-size:18px">{int(blacklist_status["contracts_loaded"])}</div>
        <div class="kpi-sub">contratos bloqueados</div>
      </div>
    </div>

    <!-- Main Grid -->
    <div class="grid">
      <!-- Config Form -->
      <div class="card">
        <h2>Actualizar Parametros</h2>
        <form method="post" action="{html.escape(action_path)}">
          <div class="form-row">
            <div>
              <label for="serlefin_percent">Porcentaje Serlefin (%)</label>
              <input id="serlefin_percent" name="serlefin_percent" type="number" step="0.01" min="0" max="100" value="{config.serlefin_percent:.2f}" required />
            </div>
            <div>
              <label for="cobyser_percent">Porcentaje Cobyser (%)</label>
              <input id="cobyser_percent" name="cobyser_percent" type="number" step="0.01" min="0" max="100" value="{config.cobyser_percent:.2f}" required />
            </div>
          </div>
          <div class="form-row">
            <div>
              <label for="min_days">Rango minimo de atraso</label>
              <input id="min_days" name="min_days" type="number" min="{int(settings.DAYS_THRESHOLD)}" value="{int(config.min_days)}" required />
            </div>
            <div>
              <label for="max_days">Rango maximo de atraso</label>
              <input id="max_days" name="max_days" type="number" min="0" value="{int(config.max_days)}" required />
            </div>
          </div>
          <div style="margin-bottom:12px">
            <label for="reason">Motivo del cambio</label>
            <textarea id="reason" name="reason" rows="2" placeholder="Ej: ajustar meta operativa de la semana"></textarea>
          </div>
          <button class="btn btn-primary btn-block" type="submit">Guardar Cambios</button>
        </form>
      </div>

      <!-- Actions Card -->
      <div class="card">
        <h2>Informes y Descargas</h2>
        <p style="font-size:12px;color:var(--text-muted);margin-bottom:10px">Contratos activos (si hay) o del ultimo ciclo historico</p>
        <a class="btn btn-brand" href="{html.escape(download_serlefin_path)}">Serlefin Excel</a>
        <a class="btn btn-secondary" href="{html.escape(download_cobyser_path)}">Cobyser Excel</a>
        <br/>
        <p style="font-size:12px;color:var(--text-muted);margin-top:10px;margin-bottom:6px">Forzar descarga desde historico completo</p>
        <a class="btn btn-secondary" href="{html.escape(download_serlefin_path)}?source=history">Serlefin (Historico)</a>
        <a class="btn btn-secondary" href="{html.escape(download_cobyser_path)}?source=history">Cobyser (Historico)</a>
        <br/>
        <a class="btn btn-blue" style="margin-top:10px" href="{html.escape(assignment_history_path)}">Historial Asignados/Eliminados</a>
        <a class="btn btn-secondary" href="{html.escape(mora_rotation_path)}">Rotacion de Mora</a>

        <form method="get" action="/history/asignados-eliminados" style="margin-top:14px">
          <div class="form-row">
            <div>
              <label for="hist_start_date">Inicio</label>
              <input id="hist_start_date" name="start_date" type="date" value="{html.escape(default_start_date)}" />
            </div>
            <div>
              <label for="hist_end_date">Fin</label>
              <input id="hist_end_date" name="end_date" type="date" value="{html.escape(default_end_date)}" />
            </div>
          </div>
          <button class="btn btn-secondary" type="submit">Filtrar historial</button>
        </form>

        <hr class="section-divider" />
        <h2 style="border:none;padding:0;margin-bottom:10px">Lista Negra</h2>
        <div class="info-row">
          <span class="info-chip">Archivo: <strong>{html.escape(blacklist_status["path"])}</strong></span>
          <span class="info-chip">Cargados: <strong>{int(blacklist_status["contracts_loaded"])}</strong></span>
        </div>
        <a class="btn btn-secondary" href="{html.escape(download_blacklist_path)}">Descargar TXT</a>
        <form method="post" action="{html.escape(upload_blacklist_path)}" enctype="multipart/form-data" style="margin-top:8px">
          <input type="file" name="blacklist_file" accept=".txt,text/plain" required style="margin-bottom:6px" />
          <button class="btn btn-secondary" type="submit">Subir TXT</button>
        </form>

        <hr class="section-divider" />
        <h2 style="border:none;padding:0;margin-bottom:10px">Acciones</h2>
        <form method="post" action="{html.escape(run_assignment_now_path)}" style="display:inline">
          <button class="btn btn-green" type="submit">Ejecutar Asignacion</button>
        </form>
        <form method="post" action="{html.escape(validate_db_processes_path)}" style="display:inline">
          <button class="btn btn-secondary" type="submit">Validar BD</button>
        </form>
        <form method="post" action="{html.escape(finalize_assignments_path)}" style="display:inline"
              onsubmit="return confirm('Esto ejecutara el cierre masivo. Confirmar?')">
          <button class="btn btn-red" type="submit">Cierre Masivo</button>
        </form>
      </div>
    </div>

    <!-- Audit Table -->
    <div class="table-section">
      <div class="table-header">Historico de Cambios de Configuracion</div>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Fecha</th>
              <th>Actor</th>
              <th>Campo</th>
              <th>Anterior</th>
              <th>Nuevo</th>
              <th>Motivo</th>
              <th>IP</th>
            </tr>
          </thead>
          <tbody>
            {audit_html_rows}
          </tbody>
        </table>
      </div>
    </div>
  </div>
</body>
</html>
"""


def _load_dashboard_data(start_date: Optional[date] = None, end_date: Optional[date] = None) -> dict:
    """Load all dashboard metrics from PostgreSQL in a single connection."""
    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        dbname=settings.POSTGRES_DATABASE,
        port=settings.POSTGRES_PORT,
    )

    # Build date filter
    date_where = ""
    date_where_and = ""
    date_params: dict[str, object] = {}
    if start_date:
        date_where += ' AND "Fecha Inicial"::date >= %(d_start)s'
        date_params["d_start"] = start_date
    if end_date:
        date_where += ' AND "Fecha Inicial"::date <= %(d_end)s'
        date_params["d_end"] = end_date
    if date_where:
        date_where_and = " WHERE 1=1" + date_where
        date_where_having = date_where  # for queries that already have WHERE
    else:
        date_where_and = ""
        date_where_having = ""

    serlefin_ids = ",".join(str(int(u)) for u in sorted(HOUSE_USER_IDS.get("serlefin", set())))
    cobyser_ids = ",".join(str(int(u)) for u in sorted(HOUSE_USER_IDS.get("cobyser", set())))

    try:
        with conn.cursor() as cur:
            # 1. Overview KPIs
            cur.execute(f"""
                SELECT
                    COUNT(*)::bigint AS total,
                    COUNT(*) FILTER (WHERE "Fecha Terminal" IS NULL)::bigint AS asignados,
                    COUNT(*) FILTER (WHERE "Fecha Terminal" IS NOT NULL)::bigint AS eliminados,
                    MIN("Fecha Inicial")::date AS fecha_min,
                    MAX("Fecha Inicial")::date AS fecha_max,
                    COUNT(DISTINCT contract_id)::bigint AS contratos_unicos,
                    COUNT(*) FILTER (WHERE "Fecha Terminal" IS NULL AND user_id IN ({serlefin_ids}))::bigint AS serlefin_asignados,
                    COUNT(*) FILTER (WHERE "Fecha Terminal" IS NULL AND user_id IN ({cobyser_ids}))::bigint AS cobyser_asignados
                FROM alocreditindicators.contract_advisors_history
                WHERE 1=1 {date_where}
            """, date_params)
            overview = cur.fetchone()

            # 2. By user_id
            cur.execute(f"""
                SELECT user_id, COUNT(*)::bigint AS cnt
                FROM alocreditindicators.contract_advisors_history
                WHERE 1=1 {date_where}
                GROUP BY user_id ORDER BY cnt DESC
            """, date_params)
            by_user = cur.fetchall()

            # 3. By tipo
            cur.execute(f"""
                SELECT COALESCE(tipo, 'SIN_TIPO') AS tipo, COUNT(*)::bigint AS cnt
                FROM alocreditindicators.contract_advisors_history
                WHERE 1=1 {date_where}
                GROUP BY tipo ORDER BY cnt DESC
            """, date_params)
            by_tipo = cur.fetchall()

            # 4. By DPD inicial
            cur.execute(f"""
                SELECT COALESCE(dpd_inicial, 'SIN_DPD') AS dpd, COUNT(*)::bigint AS cnt
                FROM alocreditindicators.contract_advisors_history
                WHERE 1=1 {date_where}
                GROUP BY dpd_inicial ORDER BY cnt DESC
            """, date_params)
            by_dpd = cur.fetchall()

            # 5. By estado_actual
            cur.execute(f"""
                SELECT COALESCE(NULLIF(TRIM(estado_actual::text), ''), 'SIN_ESTADO') AS estado,
                       COUNT(*)::bigint AS cnt
                FROM alocreditindicators.contract_advisors_history
                WHERE 1=1 {date_where}
                GROUP BY estado ORDER BY cnt DESC
            """, date_params)
            by_estado = cur.fetchall()

            # 6. Daily volume (Fecha Inicial by date)
            cur.execute(f"""
                SELECT "Fecha Inicial"::date AS dia, COUNT(*)::bigint AS cnt
                FROM alocreditindicators.contract_advisors_history
                WHERE "Fecha Inicial" IS NOT NULL {date_where}
                GROUP BY dia ORDER BY dia
            """, date_params)
            daily_volume = cur.fetchall()

            # 7. Average dias atraso
            cur.execute(f"""
                SELECT
                    AVG(CASE WHEN NULLIF(TRIM(dias_atraso_incial::text), '') ~ '^-?\d+$'
                        THEN TRIM(dias_atraso_incial::text)::numeric ELSE NULL END)::numeric(10,2) AS avg_inicial,
                    AVG(CASE WHEN NULLIF(TRIM(dias_atraso_terminal::text), '') ~ '^-?\d+$'
                        THEN TRIM(dias_atraso_terminal::text)::numeric ELSE NULL END)::numeric(10,2) AS avg_terminal
                FROM alocreditindicators.contract_advisors_history
                WHERE 1=1 {date_where}
            """, date_params)
            avg_dias = cur.fetchone()

            # 8. Monthly breakdown
            cur.execute(f"""
                SELECT
                    TO_CHAR("Fecha Inicial", 'YYYY-MM') AS mes,
                    COUNT(*)::bigint AS total,
                    COUNT(*) FILTER (WHERE "Fecha Terminal" IS NULL)::bigint AS asignados,
                    COUNT(*) FILTER (WHERE "Fecha Terminal" IS NOT NULL)::bigint AS eliminados
                FROM alocreditindicators.contract_advisors_history
                WHERE "Fecha Inicial" IS NOT NULL {date_where}
                GROUP BY mes ORDER BY mes
            """, date_params)
            monthly = cur.fetchall()

            # 9. DPD migration (initial vs terminal comparison)
            cur.execute(f"""
                SELECT
                    COALESCE(dpd_inicial, 'SIN_DPD') AS dpd_ini,
                    COALESCE(dpd_final, 'SIN_DPD') AS dpd_fin,
                    COUNT(*)::bigint AS cnt
                FROM alocreditindicators.contract_advisors_history
                WHERE dpd_inicial IS NOT NULL AND dpd_final IS NOT NULL {date_where}
                GROUP BY dpd_ini, dpd_fin
                ORDER BY cnt DESC
                LIMIT 20
            """, date_params)
            dpd_migration = cur.fetchall()

            # 10. Active promises
            cur.execute("""
                SELECT COUNT(*)::bigint
                FROM alocreditindicators.managements
                WHERE promise_date IS NOT NULL AND promise_date >= CURRENT_DATE
            """)
            active_promises = cur.fetchone()[0]

            # 11. DPD terminal distribution for comparison
            cur.execute(f"""
                SELECT COALESCE(dpd_final, 'SIN_DPD') AS dpd, COUNT(*)::bigint AS cnt
                FROM alocreditindicators.contract_advisors_history
                WHERE dpd_final IS NOT NULL {date_where}
                GROUP BY dpd_final ORDER BY cnt DESC
            """, date_params)
            by_dpd_terminal = cur.fetchall()

            # 12. User split by house with percentages
            cur.execute(f"""
                SELECT user_id,
                    COUNT(*)::bigint AS total,
                    COUNT(*) FILTER (WHERE "Fecha Terminal" IS NOT NULL)::bigint AS cerrados
                FROM alocreditindicators.contract_advisors_history
                WHERE 1=1 {date_where}
                GROUP BY user_id ORDER BY total DESC
            """, date_params)
            user_detail = cur.fetchall()

            # 13. Ultimos asignados (last 50 assignments)
            cur.execute(f"""
                SELECT
                    h.id, h.user_id, h.contract_id,
                    h."Fecha Inicial" AS fecha_inicial,
                    h."Fecha Terminal" AS fecha_terminal,
                    h.tipo, h.dpd_inicial, h.dpd_actual,
                    h.dias_atraso_incial,
                    COALESCE(NULLIF(TRIM(h.estado_actual::text), ''), 'SIN_ESTADO') AS estado_actual,
                    CASE WHEN h."Fecha Terminal" IS NULL THEN 'ASIGNADO' ELSE 'ELIMINADO' END AS estado
                FROM alocreditindicators.contract_advisors_history h
                WHERE 1=1 {date_where}
                ORDER BY h."Fecha Inicial" DESC, h.id DESC
                LIMIT 50
            """, date_params)
            latest_rows = cur.fetchall()

    finally:
        conn.close()

    return {
        "overview": {
            "total": int(overview[0] or 0),
            "asignados": int(overview[1] or 0),
            "eliminados": int(overview[2] or 0),
            "fecha_min": str(overview[3] or "-"),
            "fecha_max": str(overview[4] or "-"),
            "contratos_unicos": int(overview[5] or 0),
            "serlefin_asignados": int(overview[6] or 0),
            "cobyser_asignados": int(overview[7] or 0),
        },
        "by_user": [{"user_id": r[0], "label": _format_user_label(r[0]), "count": int(r[1])} for r in by_user],
        "by_tipo": [{"tipo": r[0], "count": int(r[1])} for r in by_tipo],
        "by_dpd_inicial": [{"dpd": r[0], "count": int(r[1])} for r in by_dpd],
        "by_dpd_terminal": [{"dpd": r[0], "count": int(r[1])} for r in by_dpd_terminal],
        "by_estado": [{"estado": r[0], "count": int(r[1])} for r in by_estado],
        "daily_volume": [{"date": str(r[0]), "count": int(r[1])} for r in daily_volume],
        "avg_dias": {
            "inicial": float(avg_dias[0] or 0),
            "terminal": float(avg_dias[1] or 0),
        },
        "monthly": [{"mes": r[0], "total": int(r[1]), "asignados": int(r[2]), "eliminados": int(r[3])} for r in monthly],
        "dpd_migration": [{"from": r[0], "to": r[1], "count": int(r[2])} for r in dpd_migration],
        "active_promises": int(active_promises or 0),
        "user_detail": [{"user_id": r[0], "label": _format_user_label(r[0]), "total": int(r[1]), "cerrados": int(r[2])} for r in user_detail],
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        "latest": [{
            "id": r[0], "user_id": r[1], "user_label": _format_user_label(r[1]),
            "contract_id": r[2], "fecha_inicial": _format_datetime(r[3]),
            "fecha_terminal": _format_datetime(r[4]), "tipo": str(r[5] or "-"),
            "dpd_inicial": str(r[6] or "-"), "dpd_actual": str(r[7] or "-"),
            "dias_inicial": r[8], "estado_actual": str(r[9] or "-"), "estado": r[10],
        } for r in latest_rows],
    }


def _render_dashboard_html(data: dict) -> str:
    """Render dashboard with Alo Credit branding and Chart.js."""
    import json as _json

    config_path = f"/config"
    default_start = data.get("start_date") or _current_month_start().isoformat()
    default_end = data.get("end_date") or datetime.now().date().isoformat()
    hist_path = f"/history/asignados-eliminados?start_date={default_start}&end_date={default_end}"
    logout_path = f"/logout"
    ov = data["overview"]
    avg = data["avg_dias"]
    data_json = _json.dumps(data)
    delta = avg["terminal"] - avg["inicial"]
    delta_color = "var(--red)" if delta > 0 else "var(--green)"
    delta_sign = "+" if delta >= 0 else ""

    # Build HTML parts separately to avoid f-string brace issues with CSS
    css = """
    :root {
      --brand: #FF8C42; --brand-hover: #FF7A28; --brand-light: #FFF5ED;
      --bg: #F5F5F7; --surface: #FFFFFF; --surface2: #F9F9F9;
      --border: #E5E7EB; --border-light: #F0F0F0;
      --text: #3e4a60; --text-secondary: #5A6B8C; --text-muted: #7A7A7A;
      --green: #10B981; --red: #EF4444; --blue: #3B82F6;
      --amber: #F59E0B; --purple: #8B5CF6; --teal: #14B8A6;
      --radius: 14px; --radius-sm: 10px;
      --shadow: 0 1px 3px rgba(0,0,0,0.06), 0 4px 16px rgba(0,0,0,0.04);
      --shadow-lg: 0 4px 24px rgba(0,0,0,0.08);
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Inter', -apple-system, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; -webkit-font-smoothing: antialiased; }
    .topbar { background: var(--surface); border-bottom: 1px solid var(--border); padding: 0 24px; height: 56px; display: flex; align-items: center; justify-content: space-between; position: sticky; top: 0; z-index: 100; box-shadow: 0 1px 4px rgba(0,0,0,0.04); }
    .topbar-brand { display: flex; align-items: center; gap: 10px; }
    .topbar-brand .logo { width: 32px; height: 32px; background: linear-gradient(135deg, #FF8C42, #FF7A28); border-radius: 10px; display: grid; place-items: center; font-weight: 800; font-size: 15px; color: #fff; }
    .topbar h1 { font-size: 15px; font-weight: 700; color: var(--text); }
    .topbar-right { display: flex; gap: 8px; align-items: center; }
    .nav-link { font-family: inherit; font-size: 13px; font-weight: 500; padding: 7px 14px; border-radius: 10px; border: 1px solid var(--border); background: var(--surface); color: var(--text); text-decoration: none; cursor: pointer; transition: all 0.15s; }
    .nav-link:hover { background: var(--surface2); }
    .nav-link.active { background: #FF8C42; color: #fff; border-color: #FF8C42; }
    .container { max-width: 1500px; margin: 0 auto; padding: 20px 24px; }
    .date-bar { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 14px 20px; margin-bottom: 16px; display: flex; align-items: center; gap: 12px; flex-wrap: wrap; box-shadow: var(--shadow); }
    .date-bar label { font-size: 12px; font-weight: 600; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.04em; }
    .date-bar input[type=date] { font-family: inherit; font-size: 13px; padding: 7px 10px; border: 1px solid var(--border); border-radius: var(--radius-sm); background: var(--surface2); color: var(--text); outline: none; }
    .date-bar input[type=date]:focus { border-color: var(--brand); box-shadow: 0 0 0 2px rgba(255,140,66,0.15); }
    .date-bar .btn-refresh { font-family: inherit; font-size: 13px; font-weight: 600; padding: 7px 18px; border: none; border-radius: var(--radius-sm); background: var(--brand); color: #fff; cursor: pointer; transition: background 0.15s; }
    .date-bar .btn-refresh:hover { background: var(--brand-hover); }
    .date-bar .btn-refresh:disabled { opacity: 0.6; cursor: wait; }
    .date-bar .spinner-sm { display: none; width: 16px; height: 16px; border: 2px solid #fff3; border-top-color: #fff; border-radius: 50%; animation: spin 0.6s linear infinite; }
    .date-bar .loading .spinner-sm { display: inline-block; }
    .kpi-row { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 20px; }
    .kpi { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 18px 20px; box-shadow: var(--shadow); transition: transform 0.15s, box-shadow 0.15s; cursor: default; }
    .kpi:hover { transform: translateY(-2px); box-shadow: var(--shadow-lg); }
    .kpi.clickable { cursor: pointer; }
    .kpi.clickable:hover { border-color: var(--brand); }
    .kpi-label { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.07em; color: var(--text-secondary); margin-bottom: 8px; }
    .kpi-num { font-size: 26px; font-weight: 800; letter-spacing: -0.02em; font-variant-numeric: tabular-nums; line-height: 1; }
    .kpi-sub { font-size: 11px; color: var(--text-muted); margin-top: 6px; }
    .chart-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; margin-bottom: 20px; }
    .chart-card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 20px; box-shadow: var(--shadow); }
    .chart-card.full { grid-column: 1 / -1; }
    .chart-card h3 { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-secondary); margin-bottom: 16px; }
    .chart-wrap { position: relative; width: 100%; height: 280px; }
    .chart-wrap.tall { height: 320px; }
    .flow-list { list-style: none; }
    .flow-item { display: flex; align-items: center; gap: 10px; padding: 7px 0; border-bottom: 1px solid var(--border-light); font-size: 13px; }
    .flow-from { color: #FF8C42; font-weight: 700; min-width: 70px; text-align: right; }
    .flow-arrow { color: var(--text-muted); }
    .flow-to { color: var(--teal); font-weight: 700; min-width: 70px; }
    .flow-count { color: var(--text-secondary); margin-left: auto; font-variant-numeric: tabular-nums; }
    .mini-table { width: 100%; border-collapse: collapse; }
    .mini-table th { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-secondary); padding: 6px 8px; text-align: left; border-bottom: 1px solid var(--border); }
    .mini-table td { font-size: 13px; padding: 7px 8px; border-bottom: 1px solid var(--border-light); font-variant-numeric: tabular-nums; }
    .mini-table tbody tr:hover { background: var(--surface2); }
    .bar-fill { height: 6px; border-radius: 3px; display: inline-block; }
    .latest-section { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); box-shadow: var(--shadow); margin-bottom: 20px; overflow: hidden; }
    .latest-header { display: flex; align-items: center; justify-content: space-between; padding: 16px 20px; border-bottom: 1px solid var(--border); }
    .latest-header h3 { font-size: 13px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; color: var(--text-secondary); margin: 0; }
    .latest-search { font-family: inherit; font-size: 13px; padding: 7px 12px; border: 1px solid var(--border); border-radius: var(--radius-sm); background: var(--surface2); color: var(--text); outline: none; width: 260px; }
    .latest-search:focus { border-color: var(--brand); box-shadow: 0 0 0 2px rgba(255,140,66,0.15); }
    .latest-scroll { overflow-x: auto; max-height: 480px; overflow-y: auto; }
    .latest-table { width: 100%; border-collapse: collapse; min-width: 900px; }
    .latest-table thead { position: sticky; top: 0; z-index: 5; }
    .latest-table th { background: var(--surface2); color: var(--text-secondary); font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; padding: 9px 12px; text-align: left; border-bottom: 1px solid var(--border); white-space: nowrap; cursor: pointer; user-select: none; }
    .latest-table th:hover { color: var(--text); }
    .latest-table th.sorted { color: var(--brand); }
    .latest-table td { padding: 8px 12px; font-size: 13px; border-bottom: 1px solid var(--border-light); white-space: nowrap; font-variant-numeric: tabular-nums; }
    .latest-table tbody tr { transition: background 0.1s; }
    .latest-table tbody tr:hover { background: var(--surface2); }
    .badge-sm { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 10px; font-weight: 700; }
    .badge-asig { background: #ECFDF5; color: #10B981; }
    .badge-elim { background: #FEF2F2; color: #EF4444; }
    .ver-mas-row { text-align: center; padding: 14px; }
    .ver-mas-row a { color: var(--brand); font-weight: 600; font-size: 13px; text-decoration: none; }
    .ver-mas-row a:hover { text-decoration: underline; }
    @keyframes spin { to { transform: rotate(360deg); } }
    @media (max-width: 900px) { .chart-grid { grid-template-columns: 1fr; } .kpi-row { grid-template-columns: repeat(2, 1fr); } .date-bar { flex-direction: column; align-items: stretch; } }
    """

    js = """
    const P=['#FF8C42','#3B82F6','#10B981','#F59E0B','#EF4444','#8B5CF6','#14B8A6','#EC4899','#06B6D4','#84CC16','#F97316','#64748B'];
    const O=['0','1_3','4_15','16_30','31_45','46_60','61_90','91_120','121_150','151_180','181_209','210_MAS','SIN_DPD'];
    Chart.defaults.color='#5A6B8C';Chart.defaults.borderColor='#E5E7EB';Chart.defaults.font.family="'Inter',sans-serif";

    let charts = {};
    function initCharts() {
      Object.values(charts).forEach(c => c.destroy());
      charts = {};

      charts.cD = new Chart(document.getElementById('cD'),{type:'line',data:{labels:D.daily_volume.map(d=>d.date),datasets:[{label:'Asignaciones',data:D.daily_volume.map(d=>d.count),borderColor:'#FF8C42',backgroundColor:'rgba(255,140,66,0.08)',fill:true,tension:.35,pointRadius:3,pointBackgroundColor:'#FF8C42'}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{grid:{display:false},ticks:{maxRotation:45,font:{size:10}}},y:{beginAtZero:true}}}});

      const m={};D.by_dpd_inicial.forEach(d=>m[d.dpd]=d.count);const l=O.filter(k=>m[k]);
      charts.cDPD = new Chart(document.getElementById('cDPD'),{type:'bar',data:{labels:l,datasets:[{data:l.map(k=>m[k]||0),backgroundColor:P.slice(0,l.length),borderRadius:6}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{grid:{display:false}},y:{beginAtZero:true}}}});

      charts.cU = new Chart(document.getElementById('cU'),{type:'doughnut',data:{labels:D.by_user.map(u=>u.label),datasets:[{data:D.by_user.map(u=>u.count),backgroundColor:['#FF8C42','#3B82F6','#10B981','#8B5CF6'],borderColor:'#fff',borderWidth:3}]},options:{responsive:true,maintainAspectRatio:false,cutout:'65%',plugins:{legend:{position:'bottom',labels:{padding:16,usePointStyle:true}},tooltip:{callbacks:{label:c=>c.label+': '+c.parsed.toLocaleString()+' ('+(c.parsed/D.overview.total*100).toFixed(1)+'%)'}}}}});

      const t=D.by_estado.slice(0,8);
      charts.cE = new Chart(document.getElementById('cE'),{type:'bar',data:{labels:t.map(e=>e.estado),datasets:[{data:t.map(e=>e.count),backgroundColor:P.slice(0,t.length),borderRadius:6}]},options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{legend:{display:false}},scales:{x:{beginAtZero:true},y:{grid:{display:false}}}}});

      charts.cT = new Chart(document.getElementById('cT'),{type:'doughnut',data:{labels:D.by_tipo.map(t=>t.tipo),datasets:[{data:D.by_tipo.map(t=>t.count),backgroundColor:['#EF4444','#F59E0B','#3B82F6','#10B981','#8B5CF6','#14B8A6','#EC4899'],borderColor:'#fff',borderWidth:3}]},options:{responsive:true,maintainAspectRatio:false,cutout:'55%',plugins:{legend:{position:'bottom',labels:{padding:12,usePointStyle:true}}}}});

      const im={},tm={};D.by_dpd_inicial.forEach(d=>im[d.dpd]=d.count);D.by_dpd_terminal.forEach(d=>tm[d.dpd]=d.count);const lc=O.filter(k=>(im[k]||0)+(tm[k]||0)>0);
      charts.cC = new Chart(document.getElementById('cC'),{type:'bar',data:{labels:lc,datasets:[{label:'DPD Inicial',data:lc.map(k=>im[k]||0),backgroundColor:'rgba(255,140,66,0.7)',borderRadius:4},{label:'DPD Terminal',data:lc.map(k=>tm[k]||0),backgroundColor:'rgba(20,184,166,0.7)',borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'top',labels:{usePointStyle:true}}},scales:{x:{grid:{display:false}},y:{beginAtZero:true}}}});

      // Flow list
      const fl=document.getElementById('fL'); fl.innerHTML='';
      D.dpd_migration.slice(0,15).forEach(f=>{const i=document.createElement('li');i.className='flow-item';i.innerHTML='<span class="flow-from">'+f.from+'</span><span class="flow-arrow">&#10132;</span><span class="flow-to">'+f.to+'</span><span class="flow-count">'+f.count.toLocaleString()+'</span>';fl.appendChild(i);});

      // User detail table
      const ut=document.getElementById('uT'); ut.innerHTML='';
      const mx=Math.max(...D.user_detail.map(u=>u.total),1);
      D.user_detail.forEach((u,i)=>{const r=document.createElement('tr');r.innerHTML='<td><strong>'+u.label+'</strong></td><td>'+u.total.toLocaleString()+'</td><td>'+u.cerrados.toLocaleString()+'</td><td>'+(u.total>0?(u.cerrados/u.total*100).toFixed(1):'0')+'%</td><td><div class="bar-fill" style="width:'+(u.total/mx*100)+'%;background:'+P[i%P.length]+'"></div></td>';ut.appendChild(r);});
    }

    function updateKPIs() {
      const o = D.overview;
      document.getElementById('kTotal').textContent = o.total.toLocaleString();
      document.getElementById('kUnicos').textContent = o.contratos_unicos.toLocaleString();
      document.getElementById('kAsignados').textContent = o.asignados.toLocaleString();
      document.getElementById('kEliminados').textContent = o.eliminados.toLocaleString();
      document.getElementById('kSerlefin').textContent = (o.serlefin_asignados||0).toLocaleString();
      document.getElementById('kCobyser').textContent = (o.cobyser_asignados||0).toLocaleString();
      document.getElementById('kPromesas').textContent = D.active_promises.toLocaleString();
      document.getElementById('kAvgIni').textContent = Math.round(D.avg_dias.inicial);
      document.getElementById('kAvgTerm').textContent = Math.round(D.avg_dias.terminal);
      const dd = D.avg_dias.terminal - D.avg_dias.inicial;
      const de = document.getElementById('kDelta');
      de.textContent = (dd>=0?'+':'')+Math.round(dd)+'d';
      de.style.color = dd > 0 ? 'var(--red)' : 'var(--green)';
      document.getElementById('kRange').textContent = (o.fecha_min||'-')+' a '+(o.fecha_max||'-');
    }

    // Latest assignments table
    let latestSort = {col: null, dir: 'asc'};
    function renderLatest(searchTerm) {
      let rows = D.latest || [];
      if (searchTerm) {
        const q = searchTerm.toLowerCase();
        rows = rows.filter(r => Object.values(r).some(v => String(v).toLowerCase().includes(q)));
      }
      if (latestSort.col) {
        rows = [...rows].sort((a,b) => {
          let va=a[latestSort.col], vb=b[latestSort.col];
          if (va==null) va=''; if (vb==null) vb='';
          if (typeof va==='number'&&typeof vb==='number') return latestSort.dir==='asc'?va-vb:vb-va;
          return latestSort.dir==='asc'?String(va).localeCompare(String(vb)):String(vb).localeCompare(String(va));
        });
      }
      const tb = document.getElementById('latestBody');
      tb.innerHTML = rows.map(r =>
        '<tr>'+
        '<td>'+r.user_label+'</td>'+
        '<td><strong>'+r.contract_id+'</strong></td>'+
        '<td>'+r.fecha_inicial+'</td>'+
        '<td>'+(r.fecha_terminal||'-')+'</td>'+
        '<td>'+r.tipo+'</td>'+
        '<td>'+r.dpd_inicial+'</td>'+
        '<td>'+r.dpd_actual+'</td>'+
        '<td>'+(r.dias_inicial!=null?r.dias_inicial:'-')+'</td>'+
        '<td>'+r.estado_actual+'</td>'+
        '<td><span class="badge-sm '+(r.estado==='ASIGNADO'?'badge-asig':'badge-elim')+'">'+r.estado+'</span></td>'+
        '</tr>'
      ).join('');
      document.getElementById('latestCount').textContent = rows.length + ' de ' + (D.latest||[]).length;
    }

    function sortLatest(col) {
      if (latestSort.col===col) latestSort.dir = latestSort.dir==='asc'?'desc':'asc';
      else { latestSort.col=col; latestSort.dir='asc'; }
      document.querySelectorAll('.latest-table th').forEach(th=>th.classList.remove('sorted'));
      const th=document.querySelector('.latest-table th[data-col="'+col+'"]');
      if(th) th.classList.add('sorted');
      renderLatest(document.getElementById('latestSearch').value.trim());
    }

    // AJAX refresh
    async function refreshDashboard() {
      const btn = document.getElementById('btnRefresh');
      btn.disabled = true;
      btn.classList.add('loading');
      btn.querySelector('.spinner-sm').style.display='inline-block';
      const sd = document.getElementById('dStart').value;
      const ed = document.getElementById('dEnd').value;
      const p = new URLSearchParams();
      if (sd) p.set('start_date', sd);
      if (ed) p.set('end_date', ed);
      try {
        const resp = await fetch('/api/dashboard?' + p.toString());
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        D = await resp.json();
        updateKPIs();
        initCharts();
        renderLatest('');
        document.getElementById('latestSearch').value='';
        // Update hist link
        const hl = document.getElementById('histLink');
        if (hl) hl.href = '/history/asignados-eliminados?start_date='+(sd||'')+'&end_date='+(ed||'');
      } catch(e) { alert('Error: '+e.message); }
      btn.disabled = false;
      btn.classList.remove('loading');
      btn.querySelector('.spinner-sm').style.display='none';
    }

    // Init
    initCharts();
    updateKPIs();
    renderLatest('');

    let _st;
    document.getElementById('latestSearch').addEventListener('input', function(){
      clearTimeout(_st);
      _st = setTimeout(()=>renderLatest(this.value.trim()), 150);
    });

    document.getElementById('dStart').addEventListener('keydown', e=>{if(e.key==='Enter')refreshDashboard();});
    document.getElementById('dEnd').addEventListener('keydown', e=>{if(e.key==='Enter')refreshDashboard();});
    """

    return (
        '<!doctype html><html lang="es"><head>'
        '<meta charset="utf-8" />'
        '<meta name="viewport" content="width=device-width, initial-scale=1" />'
        '<title>Dashboard - Alo Credit</title>'
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet" />'
        '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>'
        '<style>' + css + '</style>'
        '</head><body>'
        '<nav class="topbar">'
        '<div class="topbar-brand"><div class="logo">A</div><h1>Alo Credit</h1></div>'
        '<div class="topbar-right">'
        '<a href="/dashboard" class="nav-link active">Dashboard</a>'
        f'<a href="{html.escape(hist_path)}" class="nav-link" id="histLink">Historial</a>'
        f'<a href="{html.escape(config_path)}" class="nav-link">Configuracion</a>'
        f'<form method="post" action="{html.escape(logout_path)}" style="display:inline">'
        '<button class="nav-link" type="submit" style="color:var(--red);border-color:#FEF2F2">Salir</button></form>'
        '</div></nav>'
        '<div class="container">'
        # Date range bar
        '<div class="date-bar">'
        f'<label>Desde</label><input type="date" id="dStart" value="{html.escape(str(default_start))}" />'
        f'<label>Hasta</label><input type="date" id="dEnd" value="{html.escape(str(default_end))}" />'
        '<button class="btn-refresh" id="btnRefresh" onclick="refreshDashboard()"><span class="spinner-sm"></span> Actualizar</button>'
        '</div>'
        # KPIs - 2 rows of 5
        '<div class="kpi-row">'
        f'<div class="kpi"><div class="kpi-label">Total Registros</div><div class="kpi-num" style="color:#FF8C42" id="kTotal">{ov["total"]:,}</div><div class="kpi-sub" id="kRange">{html.escape(ov["fecha_min"])} a {html.escape(ov["fecha_max"])}</div></div>'
        f'<div class="kpi"><div class="kpi-label">Contratos Unicos</div><div class="kpi-num" style="color:#3B82F6" id="kUnicos">{ov["contratos_unicos"]:,}</div></div>'
        f'<div class="kpi clickable" onclick="document.getElementById(\'dStart\').value&&(location.href=\'/history/asignados-eliminados?start_date=\'+document.getElementById(\'dStart\').value+\'&end_date=\'+document.getElementById(\'dEnd\').value+\'&status=ASIGNADO\')"><div class="kpi-label">Asignados Activos</div><div class="kpi-num" style="color:#10B981" id="kAsignados">{ov["asignados"]:,}</div></div>'
        f'<div class="kpi clickable" onclick="document.getElementById(\'dStart\').value&&(location.href=\'/history/asignados-eliminados?start_date=\'+document.getElementById(\'dStart\').value+\'&end_date=\'+document.getElementById(\'dEnd\').value+\'&status=ELIMINADO\')"><div class="kpi-label">Eliminados</div><div class="kpi-num" style="color:#EF4444" id="kEliminados">{ov["eliminados"]:,}</div></div>'
        f'<div class="kpi"><div class="kpi-label">Promesas Activas</div><div class="kpi-num" style="color:#F59E0B" id="kPromesas">{data["active_promises"]:,}</div></div>'
        '</div>'
        '<div class="kpi-row">'
        f'<div class="kpi clickable" onclick="location.href=\'/history/asignados-eliminados?start_date=\'+document.getElementById(\'dStart\').value+\'&end_date=\'+document.getElementById(\'dEnd\').value+\'&house=serlefin&status=ASIGNADO\'"><div class="kpi-label">Serlefin Asignados</div><div class="kpi-num" style="color:var(--blue)" id="kSerlefin">{ov.get("serlefin_asignados", 0):,}</div></div>'
        f'<div class="kpi clickable" onclick="location.href=\'/history/asignados-eliminados?start_date=\'+document.getElementById(\'dStart\').value+\'&end_date=\'+document.getElementById(\'dEnd\').value+\'&house=cobyser&status=ASIGNADO\'"><div class="kpi-label">Cobyser Asignados</div><div class="kpi-num" style="color:var(--purple)" id="kCobyser">{ov.get("cobyser_asignados", 0):,}</div></div>'
        f'<div class="kpi"><div class="kpi-label">Avg Dias Ini</div><div class="kpi-num" style="color:#8B5CF6" id="kAvgIni">{avg["inicial"]:.0f}</div></div>'
        f'<div class="kpi"><div class="kpi-label">Avg Dias Term</div><div class="kpi-num" style="color:#14B8A6" id="kAvgTerm">{avg["terminal"]:.0f}</div></div>'
        f'<div class="kpi"><div class="kpi-label">Delta Mora</div><div class="kpi-num" style="color:{delta_color}" id="kDelta">{delta_sign}{delta:.0f}d</div></div>'
        '</div>'
        # Charts
        '<div class="chart-grid">'
        '<div class="chart-card full"><h3>Volumen Diario de Asignaciones</h3><div class="chart-wrap tall"><canvas id="cD"></canvas></div></div>'
        '<div class="chart-card"><h3>Distribucion DPD Inicial</h3><div class="chart-wrap"><canvas id="cDPD"></canvas></div></div>'
        '<div class="chart-card"><h3>Distribucion por Casa</h3><div class="chart-wrap"><canvas id="cU"></canvas></div></div>'
        '<div class="chart-card"><h3>Estado Actual</h3><div class="chart-wrap"><canvas id="cE"></canvas></div></div>'
        '<div class="chart-card"><h3>Tipo de Cierre</h3><div class="chart-wrap"><canvas id="cT"></canvas></div></div>'
        '<div class="chart-card full"><h3>Migracion de Mora: DPD Inicial vs Terminal</h3><div class="chart-wrap tall"><canvas id="cC"></canvas></div></div>'
        '<div class="chart-card"><h3>Top Flujos Migracion</h3><ul class="flow-list" id="fL"></ul></div>'
        '<div class="chart-card"><h3>Detalle por Asesor</h3><table class="mini-table"><thead><tr><th>Casa</th><th>Total</th><th>Cerrados</th><th>%</th><th></th></tr></thead><tbody id="uT"></tbody></table></div>'
        '</div>'
        # Latest assignments table
        '<div class="latest-section">'
        '<div class="latest-header">'
        '<h3>Ultimos Asignados <span style="font-weight:400;color:var(--text-muted);font-size:11px" id="latestCount"></span></h3>'
        '<input type="text" class="latest-search" id="latestSearch" placeholder="Buscar contrato, casa, estado..." />'
        '</div>'
        '<div class="latest-scroll">'
        '<table class="latest-table"><thead><tr>'
        '<th data-col="user_label" onclick="sortLatest(\'user_label\')">Casa</th>'
        '<th data-col="contract_id" onclick="sortLatest(\'contract_id\')">Contrato</th>'
        '<th data-col="fecha_inicial" onclick="sortLatest(\'fecha_inicial\')">Fecha Inicial</th>'
        '<th data-col="fecha_terminal" onclick="sortLatest(\'fecha_terminal\')">Fecha Terminal</th>'
        '<th data-col="tipo" onclick="sortLatest(\'tipo\')">Tipo</th>'
        '<th data-col="dpd_inicial" onclick="sortLatest(\'dpd_inicial\')">DPD Ini</th>'
        '<th data-col="dpd_actual" onclick="sortLatest(\'dpd_actual\')">DPD Act</th>'
        '<th data-col="dias_inicial" onclick="sortLatest(\'dias_inicial\')">Dias Atraso</th>'
        '<th data-col="estado_actual" onclick="sortLatest(\'estado_actual\')">Estado Actual</th>'
        '<th data-col="estado" onclick="sortLatest(\'estado\')">Estado</th>'
        '</tr></thead><tbody id="latestBody"></tbody></table>'
        '</div>'
        '<div class="ver-mas-row">'
        f'<a href="{html.escape(hist_path)}" id="histLink2">Ver historial completo &rarr;</a>'
        '</div>'
        '</div>'
        '</div>'
        f'<script>let D={data_json};' + js + '</script>'
        '</body></html>'
    )

@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_page(request: Request, start_date: str = "", end_date: str = ""):
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    parsed_start = None
    parsed_end = None
    if not (start_date or "").strip():
        parsed_start = _current_month_start()
    else:
        try:
            parsed_start = _parse_start_date(start_date)
        except ValueError:
            parsed_start = _current_month_start()
    if (end_date or "").strip():
        try:
            parsed_end = _parse_start_date(end_date)
        except ValueError:
            pass

    try:
        data = _load_dashboard_data(start_date=parsed_start, end_date=parsed_end)
        return HTMLResponse(
            _render_dashboard_html(data),
            headers=NO_CACHE_HEADERS,
        )
    except Exception as error:
        logger.error("Error cargando dashboard: %s", error, exc_info=True)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/config?{query}", status_code=303)


@app.get("/api/dashboard", include_in_schema=False)
async def api_dashboard_data(request: Request, start_date: str = "", end_date: str = "") -> JSONResponse:
    """JSON API for dashboard data."""
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    parsed_start = None
    parsed_end = None
    if not (start_date or "").strip():
        parsed_start = _current_month_start()
    else:
        try:
            parsed_start = _parse_start_date(start_date)
        except ValueError:
            parsed_start = _current_month_start()
    if (end_date or "").strip():
        try:
            parsed_end = _parse_start_date(end_date)
        except ValueError:
            pass

    try:
        data = _load_dashboard_data(start_date=parsed_start, end_date=parsed_end)
        return JSONResponse(data, headers=NO_CACHE_HEADERS)
    except Exception as error:
        logger.error("Error API dashboard: %s", error)
        return JSONResponse({"error": str(error)}, status_code=500)




@app.get("/_health", include_in_schema=False)
async def panel_health() -> JSONResponse:
    return JSONResponse({"status": "ok", "panel": "running"})




@app.get("/login", include_in_schema=False)
async def panel_login_page(
    request: Request,
    next: str = "",
    error: str = "",
) -> HTMLResponse:
    pass # _assert_hash removed
    if not settings.ADMIN_AUTH_ENABLED:
        return HTMLResponse("", status_code=404)

    token = request.cookies.get(settings.ADMIN_AUTH_COOKIE_NAME)
    username = admin_panel_auth_service.validate_session_token(token)
    if username:
        return RedirectResponse(
            url=_safe_next_path(next),
            status_code=303,
            headers=NO_CACHE_HEADERS,
        )

    page = _render_login_html(
        
        error_message=error,
        next_path=next,
    )
    return HTMLResponse(page, headers=NO_CACHE_HEADERS)


@app.post("/login", include_in_schema=False)
async def panel_login_submit(
    username: str = Form(""),
    password: str = Form(""),
    next: str = Form(""),
) -> RedirectResponse:
    pass # _assert_hash removed
    if not settings.ADMIN_AUTH_ENABLED:
        return RedirectResponse(url=f"/", status_code=303, headers=NO_CACHE_HEADERS)

    safe_next = _safe_next_path(next)
    normalized_user = (username or "").strip().lower()
    if not admin_panel_auth_service.verify_credentials(normalized_user, password):
        query = urlencode(
            {
                "next": safe_next,
                "error": "Credenciales invalidas",
            }
        )
        return RedirectResponse(
            url=f"/login?{query}",
            status_code=303,
            headers=NO_CACHE_HEADERS,
        )

    token = admin_panel_auth_service.create_session_token(normalized_user)
    response = RedirectResponse(
        url=safe_next,
        status_code=303,
        headers=NO_CACHE_HEADERS,
    )
    response.set_cookie(
        key=settings.ADMIN_AUTH_COOKIE_NAME,
        value=token,
        httponly=True,
        secure=settings.ADMIN_AUTH_COOKIE_SECURE,
        samesite="lax",
        max_age=max(60, int(settings.ADMIN_AUTH_SESSION_HOURS) * 3600),
        path="/",
    )
    return response


@app.post("/logout", include_in_schema=False)
async def panel_logout() -> RedirectResponse:
    pass # _assert_hash removed
    response = RedirectResponse(
        url=f"/login",
        status_code=303,
        headers=NO_CACHE_HEADERS,
    )
    response.delete_cookie(key=settings.ADMIN_AUTH_COOKIE_NAME, path="/")
    return response


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def panel_home(
    request: Request,
    ok: str = "",
    error: str = "",
    load_mora_auto: str = "",
) -> HTMLResponse:
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    page = _render_panel_html(
        
        ok_message=ok,
        error_message=error,
        current_user=str(getattr(request.state, "panel_user", "-")),
        load_mora_auto=(load_mora_auto or "").strip().lower() in {"1", "true", "yes", "on"},
    )
    return HTMLResponse(page, headers=NO_CACHE_HEADERS)


@app.get("/reports/{house_key}", include_in_schema=False)
async def download_house_report(request: Request, house_key: str, source: str = ""):
    """Download Excel report. source=history to use historical data."""
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    houses = {
        "serlefin": (81, "Serlefin", "Serlefin"),
        "cobyser": (45, "Cobyser", "Cobyser"),
    }
    house = houses.get(house_key.strip().lower())
    if not house:
        raise HTTPException(status_code=404, detail="Not Found")

    user_id, user_name, house_tag = house
    use_history = (source or "").strip().lower() == "history"

    try:
        file_path = _generate_house_report(
            user_id=user_id,
            user_name=user_name,
            house_tag=house_tag,
            use_history=use_history,
        )
        return FileResponse(
            path=str(file_path),
            filename=file_path.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as error:
        logger.error("Error generando/descargando informe %s: %s", house_key, error)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/config?{query}", status_code=303)


@app.get("/api/history", include_in_schema=False)
async def api_history_data(
    request: Request,
    start_date: str = "",
    end_date: str = "",
    min_days: str = "",
    max_days: str = "",
    status: str = "",
    house: str = "",
    user_id: str = "",
    contract_id: str = "",
    page: int = 1,
    page_size: int = 100,
) -> JSONResponse:
    """JSON API for history data - supports AJAX calls from the frontend."""
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    if not start_date:
        start_date = _current_month_start().isoformat()

    try:
        parsed_start_date = _parse_start_date(start_date)
    except ValueError as error:
        return JSONResponse({"error": str(error)}, status_code=400)

    parsed_end_date = None
    if (end_date or "").strip():
        try:
            parsed_end_date = _parse_start_date(end_date)
        except ValueError as error:
            return JSONResponse({"error": str(error)}, status_code=400)

    parsed_min_days = int(min_days) if (min_days or "").strip() else None
    parsed_max_days = int(max_days) if (max_days or "").strip() else None
    selected_status = (status or "").strip().upper()
    selected_house = (house or "").strip().lower()
    selected_user_id = int(user_id) if (user_id or "").strip() else None
    selected_contract_id = int(contract_id) if (contract_id or "").strip() else None

    try:
        report = _load_assignment_history_report(
            start_date=parsed_start_date,
            end_date=parsed_end_date,
            min_days=parsed_min_days,
            max_days=parsed_max_days,
            status_filter=selected_status,
            house_filter=selected_house,
            user_id_filter=selected_user_id,
            contract_id_filter=selected_contract_id,
            page=max(1, int(page)),
            page_size=max(10, min(500, int(page_size))),
        )

        rows_serialized = []
        for row in report["rows"]:
            (
                row_id, uid, contract_id_val, fecha_inicial, fecha_terminal,
                tipo, dpd_inicial, dpd_final, dpd_actual,
                dias_inicial, dias_terminal, estado_actual, estado,
            ) = row
            rows_serialized.append({
                "id": row_id,
                "user_id": uid,
                "user_label": _format_user_label(uid),
                "contract_id": contract_id_val,
                "fecha_inicial": _format_datetime(fecha_inicial),
                "fecha_terminal": _format_datetime(fecha_terminal),
                "tipo": str(tipo or "-"),
                "dpd_inicial": str(dpd_inicial or "-"),
                "dpd_final": str(dpd_final or "-"),
                "dpd_actual": str(dpd_actual or "-"),
                "dias_inicial": dias_inicial,
                "dias_terminal": dias_terminal,
                "estado_actual": str(estado_actual or "-"),
                "estado": estado,
            })

        return JSONResponse({
            "total_rows": report["total_rows"],
            "asignados": report["asignados"],
            "eliminados": report["eliminados"],
            "serlefin_asignados": report.get("serlefin_asignados", 0),
            "cobyser_asignados": report.get("cobyser_asignados", 0),
            "page": report["page"],
            "page_size": report["page_size"],
            "total_pages": report["total_pages"],
            "has_prev": report["has_prev"],
            "has_next": report["has_next"],
            "rows": rows_serialized,
        }, headers=NO_CACHE_HEADERS)

    except Exception as error:
        logger.error("Error API history: %s", error)
        return JSONResponse({"error": str(error)}, status_code=500)


@app.get("/history/asignados-eliminados", response_class=HTMLResponse, include_in_schema=False)
async def assignment_history_report(
    request: Request,
    start_date: str = "",
    end_date: str = "",
    min_days: str = "",
    max_days: str = "",
    status: str = "",
    house: str = "",
    user_id: str = "",
    contract_id: str = "",
    page: int = 1,
    page_size: int = 100,
):
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    if not start_date:
        start_date = _current_month_start().isoformat()

    try:
        parsed_start_date = _parse_start_date(start_date)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))

    parsed_end_date = None
    if (end_date or "").strip():
        try:
            parsed_end_date = _parse_start_date(end_date)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error))

    parsed_min_days = None
    if (min_days or "").strip():
        try:
            parsed_min_days = int(min_days)
        except ValueError:
            raise HTTPException(status_code=400, detail="min_days debe ser numerico")

    parsed_max_days = None
    if (max_days or "").strip():
        try:
            parsed_max_days = int(max_days)
        except ValueError:
            raise HTTPException(status_code=400, detail="max_days debe ser numerico")

    selected_status = (status or "").strip().upper()
    if selected_status not in {"", "ASIGNADO", "ELIMINADO"}:
        raise HTTPException(status_code=400, detail="status invalido")

    selected_house = (house or "").strip().lower()
    if selected_house not in {"", "cobyser", "serlefin"}:
        raise HTTPException(status_code=400, detail="house invalida")

    selected_user_id = None
    if (user_id or "").strip():
        try:
            selected_user_id = int(user_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="user_id debe ser numerico")

    selected_contract_id = None
    if (contract_id or "").strip():
        try:
            selected_contract_id = int(contract_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="contract_id debe ser numerico")

    if int(page) < 1:
        raise HTTPException(status_code=400, detail="page debe ser mayor o igual a 1")
    if int(page_size) < 10 or int(page_size) > 500:
        raise HTTPException(status_code=400, detail="page_size debe estar entre 10 y 500")

    try:
        report = _load_assignment_history_report(
            start_date=parsed_start_date,
            end_date=parsed_end_date,
            min_days=parsed_min_days,
            max_days=parsed_max_days,
            status_filter=selected_status,
            house_filter=selected_house,
            user_id_filter=selected_user_id,
            contract_id_filter=selected_contract_id,
            page=int(page),
            page_size=int(page_size),
        )
        return HTMLResponse(
            _render_assignment_history_report_html( report),
            headers=NO_CACHE_HEADERS,
        )
    except Exception as error:
        logger.error("Error generando informe de asignados/eliminados: %s", error)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.get("/history/asignados-eliminados/download", include_in_schema=False)
async def download_assignment_history(
    request: Request,
    start_date: str = "",
    end_date: str = "",
    min_days: str = "",
    max_days: str = "",
    status: str = "",
    house: str = "",
    user_id: str = "",
    contract_id: str = "",
):
    """Descarga Excel con todo el historial de asignados/eliminados y columna de promesa activa."""
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    if not start_date:
        start_date = _current_month_start().isoformat()

    try:
        parsed_start_date = _parse_start_date(start_date)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))

    parsed_end_date = None
    if (end_date or "").strip():
        try:
            parsed_end_date = _parse_start_date(end_date)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error))

    parsed_min_days = None
    if (min_days or "").strip():
        try:
            parsed_min_days = int(min_days)
        except ValueError:
            raise HTTPException(status_code=400, detail="min_days debe ser numerico")

    parsed_max_days = None
    if (max_days or "").strip():
        try:
            parsed_max_days = int(max_days)
        except ValueError:
            raise HTTPException(status_code=400, detail="max_days debe ser numerico")

    selected_status = (status or "").strip().upper()
    if selected_status not in {"", "ASIGNADO", "ELIMINADO"}:
        raise HTTPException(status_code=400, detail="status invalido")

    selected_house = (house or "").strip().lower()
    if selected_house not in {"", "cobyser", "serlefin"}:
        raise HTTPException(status_code=400, detail="house invalida")

    selected_user_id = None
    if (user_id or "").strip():
        try:
            selected_user_id = int(user_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="user_id debe ser numerico")

    selected_contract_id = None
    if (contract_id or "").strip():
        try:
            selected_contract_id = int(contract_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="contract_id debe ser numerico")

    try:
        import pandas as pd

        days_expr = (
            "COALESCE("
            "CASE WHEN NULLIF(TRIM(h.dias_atraso_terminal::text), '') ~ '^-?\\d+$' "
            "THEN TRIM(h.dias_atraso_terminal::text)::int ELSE NULL END, "
            "CASE WHEN NULLIF(TRIM(h.dias_atraso_incial::text), '') ~ '^-?\\d+$' "
            "THEN TRIM(h.dias_atraso_incial::text)::int ELSE NULL END"
            ")"
        )

        where_clauses = ["1=1"]
        params: dict[str, object] = {}

        if parsed_start_date is not None:
            where_clauses.append('h."Fecha Inicial"::date >= %(start_date)s')
            params["start_date"] = parsed_start_date
        if parsed_end_date is not None:
            where_clauses.append('h."Fecha Inicial"::date <= %(end_date)s')
            params["end_date"] = parsed_end_date
        if parsed_min_days is not None:
            where_clauses.append(f"{days_expr} >= %(min_days)s")
            params["min_days"] = int(parsed_min_days)
        if parsed_max_days is not None:
            where_clauses.append(f"{days_expr} <= %(max_days)s")
            params["max_days"] = int(parsed_max_days)
        if selected_status == "ASIGNADO":
            where_clauses.append('h."Fecha Terminal" IS NULL')
        elif selected_status == "ELIMINADO":
            where_clauses.append('h."Fecha Terminal" IS NOT NULL')
        if selected_house:
            house_user_ids = sorted(HOUSE_USER_IDS.get(selected_house, set()))
            if house_user_ids:
                where_clauses.append(
                    "h.user_id IN (" + ",".join(str(int(uid)) for uid in house_user_ids) + ")"
                )
        if selected_user_id is not None:
            where_clauses.append("h.user_id = %(user_id_filter)s")
            params["user_id_filter"] = int(selected_user_id)
        if selected_contract_id is not None:
            where_clauses.append("h.contract_id = %(contract_id_filter)s")
            params["contract_id_filter"] = int(selected_contract_id)

        where_sql = " AND ".join(where_clauses)

        conn = psycopg2.connect(
            host=settings.POSTGRES_HOST,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            dbname=settings.POSTGRES_DATABASE,
            port=settings.POSTGRES_PORT,
        )
        try:
            with conn.cursor() as cur:
                query_sql = f"""
                SELECT
                    h.id,
                    h.user_id,
                    h.contract_id,
                    h."Fecha Inicial" AS fecha_inicial,
                    h."Fecha Terminal" AS fecha_terminal,
                    h.tipo,
                    h.dpd_inicial,
                    h.dpd_final,
                    h.dpd_actual,
                    h.dias_atraso_incial,
                    h.dias_atraso_terminal,
                    COALESCE(NULLIF(TRIM(h.estado_actual::text), ''), 'SIN_ESTADO') AS estado_actual,
                    CASE
                        WHEN h."Fecha Terminal" IS NULL THEN 'ASIGNADO'
                        ELSE 'ELIMINADO'
                    END AS estado
                FROM alocreditindicators.contract_advisors_history h
                WHERE {where_sql}
                ORDER BY h."Fecha Inicial" DESC, h.id DESC
                """
                cur.execute(query_sql, params)
                rows = cur.fetchall()
        finally:
            conn.close()

        promise_contract_ids = _get_active_promise_contract_ids()

        data = []
        for row in rows:
            (
                row_id, uid, cid, fecha_ini, fecha_term,
                tipo, dpd_ini, dpd_fin, dpd_act,
                dias_ini, dias_term, estado_actual, estado,
            ) = row
            casa = _format_user_label(uid)
            data.append({
                "ID": row_id,
                "Casa": casa,
                "User ID": uid,
                "Contrato": cid,
                "Fecha Inicial": _format_datetime(fecha_ini),
                "Fecha Terminal": _format_datetime(fecha_term),
                "Tipo": str(tipo or "-"),
                "DPD Inicial": dpd_ini,
                "DPD Final": dpd_fin,
                "DPD Actual": dpd_act,
                "Dias Atraso Inicial": dias_ini,
                "Dias Atraso Terminal": dias_term,
                "Estado Actual": str(estado_actual or "-"),
                "Estado": estado,
                "Promesa Activa": "SI" if int(cid) in promise_contract_ids else "NO",
            })

        df = pd.DataFrame(data)
        buffer = io.BytesIO()
        df.to_excel(buffer, index=False, engine="openpyxl")
        buffer.seek(0)

        today_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"historial_asignados_eliminados_{today_str}.xlsx"

        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                **NO_CACHE_HEADERS,
            },
        )
    except Exception as error:
        logger.error("Error generando descarga Excel de historial: %s", error)
        raise HTTPException(status_code=500, detail=f"Error generando Excel: {error}")


@app.get("/history/rotacion-mora", response_class=HTMLResponse, include_in_schema=False)
async def mora_rotation_report(
    request: Request,
    start_date: str = "",
):
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    if not start_date:
        start_date = _current_month_start().isoformat()

    try:
        parsed_start_date = _parse_start_date(start_date)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))

    try:
        report = _load_mora_rotation_report(start_date=parsed_start_date)
        return HTMLResponse(
            _render_mora_rotation_report_html( report),
            headers=NO_CACHE_HEADERS,
        )
    except Exception as error:
        logger.error("Error generando informe de rotacion de mora: %s", error)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.post("/blacklist/upload", include_in_schema=False)
async def upload_contract_blacklist(
    request: Request,
    blacklist_file: UploadFile = File(...),
) -> RedirectResponse:
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    try:
        file_name = (blacklist_file.filename or "").strip().lower()
        if file_name and not file_name.endswith(".txt"):
            raise ValueError("El archivo debe ser .txt")

        raw_bytes = await blacklist_file.read()
        raw_text = raw_bytes.decode("utf-8", errors="ignore")
        save_result = blacklist_service.save_from_text(raw_text)

        query = urlencode(
            {
                "ok": (
                    "Lista negra actualizada. "
                    f"Contratos cargados: {save_result['contracts_loaded']}"
                )
            }
        )
    except Exception as error:
        logger.error("Error cargando lista negra TXT: %s", error)
        query = urlencode({"error": str(error)})

    return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.post("/save", include_in_schema=False)
async def save_panel_config(
    request: Request,
    serlefin_percent: float = Form(...),
    cobyser_percent: float = Form(...),
    min_days: int = Form(...),
    max_days: int = Form(...),
    actor_email: str = Form(AUDITOR_EMAIL),
    reason: str = Form(""),
) -> RedirectResponse:
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

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


@app.post("/run-assignment-now", include_in_schema=False)
async def run_assignment_now_from_panel(request: Request) -> RedirectResponse:
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    try:
        with acquire_process_lock():
            with db_manager.get_mysql_session() as mysql_session, \
                 db_manager.get_postgres_session() as postgres_session:
                assignment_service = AssignmentService(mysql_session, postgres_session)
                results = assignment_service.execute_assignment_process()

        if not results.get("success"):
            raise RuntimeError(results.get("error") or "La asignacion no finalizo con success=True")

        insert_stats = results.get("insert_stats", {})
        blacklist_count = results.get("blacklist_contracts_count", 0)
        enforcement_stats = results.get("blacklist_enforcement_stats", {})
        message = (
            "Asignacion completada. "
            f"Blacklist: {blacklist_count} | "
            f"Removidos blacklist activos: {enforcement_stats.get('removed_from_contract_advisors', 0)} | "
            f"Insertados: {insert_stats.get('inserted_total', 0)} "
            f"(Serlefin {insert_stats.get('inserted_serlefin', 0)} / "
            f"Cobyser {insert_stats.get('inserted_cobyser', 0)})"
        )
        query = urlencode({"ok": message})
    except ProcessLockError as error:
        logger.warning("Asignacion bloqueada por lock activo: %s", error)
        query = urlencode({"error": str(error)})
    except Exception as error:
        logger.error("Error ejecutando asignacion desde panel: %s", error)
        query = urlencode({"error": str(error)})

    return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.get("/blacklist/download", include_in_schema=False)
async def download_contract_blacklist(
    request: Request,
):
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    path = blacklist_service.path
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    return FileResponse(
        path=str(path),
        filename=path.name,
        media_type="text/plain",
        headers=NO_CACHE_HEADERS,
    )


@app.post("/validate-db-processes", include_in_schema=False)
async def validate_db_processes_from_panel(request: Request) -> RedirectResponse:
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    try:
        snapshot = _load_db_process_snapshot(long_running_seconds=60)
        mysql_stats = snapshot["mysql"]
        postgres_stats = snapshot["postgres"]
        threshold = snapshot["threshold_seconds"]

        message = (
            "Validacion procesos BD. "
            f"MySQL alocreditprod -> total:{mysql_stats['total_connections']} "
            f"activas:{mysql_stats['active_connections']} "
            f"largas>={threshold}s:{mysql_stats['long_running_connections']} "
            f"max:{mysql_stats['max_active_seconds']}s. "
            f"PostgreSQL -> total:{postgres_stats['total_sessions']} "
            f"activas:{postgres_stats['active_sessions']} "
            f"largas>={threshold}s:{postgres_stats['long_running_sessions']} "
            f"max:{postgres_stats['max_active_seconds']}s."
        )
        query = urlencode({"ok": message})
    except Exception as error:
        logger.error("Error validando procesos de base de datos: %s", error)
        query = urlencode({"error": str(error)})

    return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.post("/finalize-assignments", include_in_schema=False)
async def finalize_assignments_from_panel(request: Request) -> RedirectResponse:
    pass # _assert_hash removed
    auth_redirect = _require_panel_auth(request)
    if auth_redirect is not None:
        return auth_redirect

    try:
        with acquire_process_lock():
            with db_manager.get_mysql_session() as mysql_session, \
                 db_manager.get_postgres_session() as postgres_session:
                assignment_service = AssignmentService(mysql_session, postgres_session)
                stats = assignment_service.finalize_all_active_assignments()

        message = (
            "Cierre masivo completado. "
            f"Activos detectados: {stats.get('active_assignments_found', 0)} | "
            f"Historial cerrado: {stats.get('history_closed', 0)} | "
            f"Eliminados contract_advisors: {stats.get('deleted_from_contract_advisors', 0)}"
        )
        query = urlencode({"ok": message})
    except ProcessLockError as error:
        logger.warning("Cierre masivo bloqueado por lock activo: %s", error)
        query = urlencode({"error": str(error)})
    except Exception as error:
        logger.error("Error ejecutando cierre masivo desde panel: %s", error)
        query = urlencode({"error": str(error)})

    return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("admin_panel:app", host="0.0.0.0", port=9006, reload=False)
