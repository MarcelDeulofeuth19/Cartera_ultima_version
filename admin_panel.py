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


def _safe_next_path(raw_next: str, panel_hash: str) -> str:
    candidate = (raw_next or "").strip()
    default_path = f"/{panel_hash}"
    if not candidate:
        return default_path
    if candidate.startswith("//"):
        return default_path
    if candidate.startswith(default_path):
        return candidate
    return default_path


def _build_login_redirect(panel_hash: str, next_path: str) -> RedirectResponse:
    safe_next = _safe_next_path(next_path, panel_hash)
    query = urlencode({"next": safe_next})
    return RedirectResponse(
        url=f"/{panel_hash}/login?{query}",
        status_code=303,
        headers=NO_CACHE_HEADERS,
    )


def _require_panel_auth(request: Request, panel_hash: str) -> Optional[RedirectResponse]:
    if not settings.ADMIN_AUTH_ENABLED:
        return None

    cookie_name = settings.ADMIN_AUTH_COOKIE_NAME
    token = request.cookies.get(cookie_name)
    username = admin_panel_auth_service.validate_session_token(token)
    if not username:
        query = request.url.query
        current_path = request.url.path + (f"?{query}" if query else "")
        return _build_login_redirect(panel_hash=panel_hash, next_path=current_path)

    request.state.panel_user = username
    return None


def _render_login_html(
    *,
    panel_hash: str,
    error_message: str = "",
    next_path: str = "",
) -> str:
    safe_next = _safe_next_path(next_path, panel_hash)
    error_block = ""
    if error_message:
        error_block = (
            "<div style='padding:10px 12px;border:1px solid #f4b2ad;"
            "background:#fce7e6;color:#b42318;border-radius:10px;"
            "margin-bottom:12px;font-size:.9rem'>"
            + html.escape(error_message)
            + "</div>"
        )

    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Login Panel Seguro</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background:
        radial-gradient(circle at 10% 10%, #cff2ec 0, #cff2ec00 48%),
        radial-gradient(circle at 96% 2%, #fde2cc 0, #fde2cc00 42%),
        linear-gradient(135deg, #ece5d6 0%, #f8f4ea 100%);
      font-family: "Sora", "IBM Plex Sans", "Trebuchet MS", sans-serif;
      color: #131a1f;
      padding: 20px;
    }}
    .card {{
      width: min(420px, 100%);
      border: 1px solid #d8d2c5;
      background: #fffef9;
      border-radius: 14px;
      box-shadow: 0 16px 40px rgba(12, 20, 26, 0.12);
      padding: 20px;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 1.1rem;
      text-transform: uppercase;
      letter-spacing: .03em;
    }}
    p {{
      margin: 0 0 14px;
      color: #5a6673;
      font-size: .9rem;
    }}
    label {{
      display: block;
      font-size: .85rem;
      margin: 8px 0 4px;
      color: #5a6673;
    }}
    input {{
      width: 100%;
      border: 1px solid #c7c2b7;
      border-radius: 10px;
      padding: 10px 12px;
      font-size: .95rem;
      background: #fffffc;
      box-sizing: border-box;
    }}
    button {{
      border: 0;
      border-radius: 10px;
      padding: 11px 14px;
      margin-top: 12px;
      width: 100%;
      color: #fff;
      font-weight: 700;
      background: linear-gradient(160deg, #14532d, #0f766e);
      cursor: pointer;
    }}
    .meta {{
      margin-top: 10px;
      font-size: .8rem;
      color: #5a6673;
    }}
  </style>
</head>
<body>
  <main class="card">
    <h1>Panel Seguro - Login</h1>
    <p>Acceso protegido para la URL /{html.escape(panel_hash)}</p>
    {error_block}
    <form method="post" action="/{html.escape(panel_hash)}/login">
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
            summary_query = f"""
            SELECT
                COUNT(*)::bigint AS total_rows,
                COUNT(*) FILTER (WHERE h."Fecha Terminal" IS NULL)::bigint AS asignados,
                COUNT(*) FILTER (WHERE h."Fecha Terminal" IS NOT NULL)::bigint AS eliminados
            FROM alocreditindicators.contract_advisors_history h
            WHERE {where_sql}
            """
            cur.execute(summary_query, params)
            summary_row = cur.fetchone() or (0, 0, 0)

            total_rows = int(summary_row[0] or 0)
            asignados = int(summary_row[1] or 0)
            eliminados = int(summary_row[2] or 0)

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
    panel_hash: str,
    report: dict,
    title: str = "Informe Asignados y Eliminados",
) -> str:
    """Renderiza HTML del informe de asignados/eliminados (solo contract history)."""
    history_path = f"/{panel_hash}/history/asignados-eliminados"
    selected_status = str(report.get("status_filter") or "")
    selected_house = str(report.get("house_filter") or "")
    selected_user_id = report.get("user_id_filter")
    selected_contract_id = report.get("contract_id_filter")

    def _build_history_url(page_value: int) -> str:
        params: dict[str, str | int] = {
            "page": max(1, int(page_value)),
            "page_size": int(report.get("page_size", 100) or 100),
        }
        if report.get("start_date"):
            params["start_date"] = str(report["start_date"])
        if report.get("end_date"):
            params["end_date"] = str(report["end_date"])
        if report.get("min_days") is not None:
            params["min_days"] = int(report["min_days"])
        if report.get("max_days") is not None:
            params["max_days"] = int(report["max_days"])
        if selected_status:
            params["status"] = selected_status
        if selected_house:
            params["house"] = selected_house
        if selected_user_id is not None:
            params["user_id"] = int(selected_user_id)
        if selected_contract_id is not None:
            params["contract_id"] = int(selected_contract_id)
        return f"{history_path}?{urlencode(params)}"

    download_path = f"/{panel_hash}/history/asignados-eliminados/download"
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

    pager_html = ""
    if int(report.get("total_rows", 0) or 0) > 0:
        prev_href = _build_history_url(int(report.get("page", 1)) - 1)
        next_href = _build_history_url(int(report.get("page", 1)) + 1)
        first_href = _build_history_url(1)
        last_href = _build_history_url(int(report.get("total_pages", 1) or 1))

        prev_class = "disabled" if not bool(report.get("has_prev")) else ""
        next_class = "disabled" if not bool(report.get("has_next")) else ""

        pager_html = f"""
        <div class="pager">
          <span>Pagina <strong>{int(report.get("page", 1) or 1)}</strong> de <strong>{int(report.get("total_pages", 1) or 1)}</strong></span>
          <a class="{prev_class}" href="{html.escape(first_href if report.get('has_prev') else '#')}">Primera</a>
          <a class="{prev_class}" href="{html.escape(prev_href if report.get('has_prev') else '#')}">Anterior</a>
          <a class="{next_class}" href="{html.escape(next_href if report.get('has_next') else '#')}">Siguiente</a>
          <a class="{next_class}" href="{html.escape(last_href if report.get('has_next') else '#')}">Ultima</a>
        </div>
        """

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
            dpd_actual,
            dias_inicial,
            dias_terminal,
            estado_actual,
            estado,
        ) = row

        fecha_inicial_txt = _format_datetime(fecha_inicial)
        fecha_terminal_txt = _format_datetime(fecha_terminal)
        user_label = _format_user_label(user_id)
        tipo_text = str(tipo or "-")
        tipo_badge = (
            "<span style='display:inline-block;padding:2px 6px;border-radius:999px;background:#fef3c7;color:#92400e;font-size:.72rem;font-weight:700;margin-left:6px'>FIJO</span>"
            if "FIJO" in tipo_text.upper()
            else ""
        )
        estado_actual_text = str(estado_actual or "-")

        table_rows += (
            "<tr>"
            f"<td>{row_id}</td>"
            f"<td>{html.escape(user_label)}</td>"
            f"<td>{contract_id}</td>"
            f"<td>{html.escape(fecha_inicial_txt)}</td>"
            f"<td>{html.escape(fecha_terminal_txt)}</td>"
            f"<td>{html.escape(tipo_text)}{tipo_badge}</td>"
            f"<td>{html.escape(str(dpd_inicial or '-'))}</td>"
            f"<td>{html.escape(str(dpd_final or '-'))}</td>"
            f"<td>{html.escape(str(dpd_actual or '-'))}</td>"
            f"<td>{html.escape(str(dias_inicial if dias_inicial is not None else '-'))}</td>"
            f"<td>{html.escape(str(dias_terminal if dias_terminal is not None else '-'))}</td>"
            f"<td>{html.escape(estado_actual_text)}</td>"
            f"<td>{html.escape(estado)}</td>"
            "</tr>"
        )

    if not table_rows:
        if int(report.get("total_rows", 0) or 0) > 0:
            table_rows = (
                "<tr><td colspan='13'>No hay filas en esta pagina. Usa el paginador.</td></tr>"
            )
        elif report.get("start_date") or report.get("end_date"):
            table_rows = (
                "<tr><td colspan='13'>Sin registros para filtros aplicados.</td></tr>"
            )
        else:
            table_rows = "<tr><td colspan='13'>Sin registros.</td></tr>"

    back_path = f"/{panel_hash}"
    reset_filters_path = (
        f"{history_path}?{urlencode({'start_date': str(report.get('start_date') or ''), 'page': 1, 'page_size': report.get('page_size', 100)})}"
    )
    filter_start_date = str(report.get("start_date") or "")
    filter_end_date = str(report.get("end_date") or "")
    filter_min_days = (
        str(report.get("min_days"))
        if report.get("min_days") is not None
        else ""
    )
    filter_max_days = (
        str(report.get("max_days"))
        if report.get("max_days") is not None
        else ""
    )
    filter_user_id = str(selected_user_id) if selected_user_id is not None else ""
    filter_contract_id = (
        str(selected_contract_id)
        if selected_contract_id is not None
        else ""
    )

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
    .btn-alt {{
      background: #1f2a33;
      margin-left: 6px;
    }}
    .filters {{
      margin-top: 10px;
      border: 1px solid #e5e0d4;
      border-radius: 10px;
      background: #fcfaf4;
      padding: 10px;
    }}
    .row {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 10px;
      margin-bottom: 8px;
    }}
    label {{
      display: block;
      font-size: .78rem;
      color: #5a6673;
      margin-bottom: 3px;
    }}
    input, select {{
      width: 100%;
      box-sizing: border-box;
      border: 1px solid #d8d2c5;
      border-radius: 8px;
      padding: 7px 8px;
      font-size: .84rem;
      background: #fffef9;
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
    .pager {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      margin-top: 10px;
    }}
    .pager a {{
      text-decoration: none;
      border: 1px solid #d0c9ba;
      border-radius: 8px;
      padding: 5px 9px;
      background: #fffef9;
      color: #1f2a33;
      font-size: .83rem;
    }}
    .pager a.disabled {{
      opacity: .45;
      pointer-events: none;
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="head">
      <h2 style="margin:0 0 8px">{html.escape(title)}</h2>
      <div class="kpi">Fecha Inicial desde: <strong>{html.escape(str(report["start_date_label"]))}</strong></div>
      <div class="kpi">Fecha Inicial hasta: <strong>{html.escape(str(report["end_date_label"]))}</strong></div>
      <div class="kpi">Dias minimo: <strong>{html.escape(str(report.get("min_days", "-") if report.get("min_days") is not None else "-"))}</strong></div>
      <div class="kpi">Dias maximo: <strong>{html.escape(str(report.get("max_days", "-") if report.get("max_days") is not None else "-"))}</strong></div>
      <div class="kpi">Total filas: <strong>{report["total_rows"]}</strong></div>
      <div class="kpi">Asignados: <strong>{report["asignados"]}</strong></div>
      <div class="kpi">Eliminados: <strong>{report["eliminados"]}</strong></div>
      <div class="kpi">Pagina: <strong>{int(report.get("page", 1) or 1)}/{int(report.get("total_pages", 1) or 1)}</strong></div>
      <br />
      <a class="btn" href="{html.escape(back_path)}">Volver al panel</a>
      <a class="btn btn-alt" href="{html.escape(reset_filters_path)}">Limpiar filtros</a>
      <a class="btn" href="{html.escape(download_url)}" style="background:#1d4ed8">Descargar Excel</a>

      <form class="filters" method="get" action="{html.escape(history_path)}">
        <input type="hidden" name="page" value="1" />
        <div class="row">
          <div>
            <label for="start_date">Inicio</label>
            <input id="start_date" name="start_date" type="date" value="{html.escape(filter_start_date)}" />
          </div>
          <div>
            <label for="end_date">Fin</label>
            <input id="end_date" name="end_date" type="date" value="{html.escape(filter_end_date)}" />
          </div>
          <div>
            <label for="min_days">Dias min</label>
            <input id="min_days" name="min_days" type="number" min="0" value="{html.escape(filter_min_days)}" />
          </div>
          <div>
            <label for="max_days">Dias max</label>
            <input id="max_days" name="max_days" type="number" min="0" value="{html.escape(filter_max_days)}" />
          </div>
        </div>
        <div class="row">
          <div>
            <label for="status">Estado</label>
            <select id="status" name="status">
              <option value="" {"selected" if not selected_status else ""}>Todos</option>
              <option value="ASIGNADO" {"selected" if selected_status == "ASIGNADO" else ""}>ASIGNADO</option>
              <option value="ELIMINADO" {"selected" if selected_status == "ELIMINADO" else ""}>ELIMINADO</option>
            </select>
          </div>
          <div>
            <label for="house">Casa</label>
            <select id="house" name="house">
              <option value="" {"selected" if not selected_house else ""}>Todas</option>
              <option value="cobyser" {"selected" if selected_house == "cobyser" else ""}>Cobyser</option>
              <option value="serlefin" {"selected" if selected_house == "serlefin" else ""}>Serlefin</option>
            </select>
          </div>
          <div>
            <label for="user_id">User ID</label>
            <input id="user_id" name="user_id" type="number" min="1" value="{html.escape(filter_user_id)}" />
          </div>
          <div>
            <label for="contract_id">Contrato</label>
            <input id="contract_id" name="contract_id" type="number" min="1" value="{html.escape(filter_contract_id)}" />
          </div>
          <div>
            <label for="page_size">Filas por pagina</label>
            <select id="page_size" name="page_size">
              <option value="25" {"selected" if int(report.get("page_size", 100) or 100) == 25 else ""}>25</option>
              <option value="50" {"selected" if int(report.get("page_size", 100) or 100) == 50 else ""}>50</option>
              <option value="100" {"selected" if int(report.get("page_size", 100) or 100) == 100 else ""}>100</option>
              <option value="200" {"selected" if int(report.get("page_size", 100) or 100) == 200 else ""}>200</option>
            </select>
          </div>
        </div>
        <button class="btn" type="submit" style="margin-top:4px">Aplicar filtros</button>
      </form>
      {pager_html}
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
            <th>DPD Actual</th>
            <th>Días Inicial</th>
            <th>Días Terminal</th>
            <th>Estado Actual</th>
            <th>Estado</th>
          </tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>
    </section>
    {pager_html}
  </main>
</body>
</html>
"""


def _render_panel_html(
    *,
    panel_hash: str,
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
        audit_html_rows = "<tr><td colspan='7'>Sin cambios registrados aun.</td></tr>"

    action_path = f"/{panel_hash}/save"
    download_serlefin_path = f"/{panel_hash}/reports/serlefin"
    download_cobyser_path = f"/{panel_hash}/reports/cobyser"
    default_start_date = _current_month_start().isoformat()
    default_end_date = datetime.now().date().isoformat()
    assignment_history_path = (
        f"/{panel_hash}/history/asignados-eliminados"
        f"?start_date={default_start_date}&end_date={default_end_date}"
    )
    mora_rotation_path = (
        f"/{panel_hash}/history/rotacion-mora?start_date={default_start_date}"
    )
    run_assignment_now_path = f"/{panel_hash}/run-assignment-now"
    finalize_assignments_path = f"/{panel_hash}/finalize-assignments"
    validate_db_processes_path = f"/{panel_hash}/validate-db-processes"
    upload_blacklist_path = f"/{panel_hash}/blacklist/upload"
    download_blacklist_path = f"/{panel_hash}/blacklist/download"
    logout_path = f"/{panel_hash}/logout"
    blacklist_status = blacklist_service.status()
    load_mora_auto_path = f"/{panel_hash}?load_mora_auto=1"
    mora_auto_block = f"""
    <section class="table-card">
      <div class="table-head">Rotacion de Mora Automatica (Cobyser y Serlefin)</div>
      <div style="padding:12px 14px;color:#5a6673">
        Vista resumida bajo demanda para evitar demoras al ingresar al panel.
        <a class="btn btn-link btn-alt" href="{html.escape(load_mora_auto_path)}" style="margin-left:8px">Cargar resumen ahora</a>
      </div>
    </section>
    """
    if load_mora_auto:
        try:
            auto_report = _load_mora_rotation_report(start_date=_current_month_start())
            auto_cards = _render_mora_panel_cards(auto_report)
            mora_auto_block = f"""
            <section class="table-card">
              <div class="table-head">Rotacion de Mora Automatica (Cobyser y Serlefin)</div>
              <div class="auto-head">
                <span>Fecha inicio: <strong>{html.escape(auto_report["start_date_label"])}</strong></span>
                <span>Asignaciones activas analizadas: <strong>{auto_report["total_rows"]}</strong></span>
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
        <span class="chip">Sesion: {html.escape(current_user)}</span>
        <form method="post" action="{html.escape(logout_path)}" style="display:inline-block;margin:0">
          <button class="btn btn-alt" type="submit" style="margin:0;padding:6px 10px;font-size:.78rem;border-radius:999px">Salir</button>
        </form>
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
              <input id="min_days" name="min_days" type="number" min="{int(settings.DAYS_THRESHOLD)}" value="{int(config.min_days)}" required />
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
        <a class="btn btn-link" href="{html.escape(assignment_history_path)}">Ver Asignados/Eliminados (mes actual)</a>
        <a class="btn btn-link" href="{html.escape(mora_rotation_path)}">Ver Rotacion de Mora (Cobyser y Serlefin)</a>
        <form method="get" action="/{html.escape(panel_hash)}/history/asignados-eliminados" style="margin-top:10px">
          <div class="row">
            <div>
              <label for="hist_start_date">Inicio</label>
              <input id="hist_start_date" name="start_date" type="date" value="{html.escape(default_start_date)}" />
            </div>
            <div>
              <label for="hist_end_date">Fin</label>
              <input id="hist_end_date" name="end_date" type="date" value="{html.escape(default_end_date)}" />
            </div>
          </div>
          <div class="row" style="margin-top:10px">
            <div>
              <label for="hist_min_days">Dias min</label>
              <input id="hist_min_days" name="min_days" type="number" min="0" placeholder="Ej: 61" />
            </div>
            <div>
              <label for="hist_max_days">Dias max</label>
              <input id="hist_max_days" name="max_days" type="number" min="0" placeholder="Ej: 240" />
            </div>
          </div>
          <button class="btn btn-alt" type="submit">Filtrar historial</button>
        </form>

        <hr style="margin:16px 0;border:none;border-top:1px solid #ece7db" />
        <h2 style="margin-top:0">Lista Negra</h2>
        <p style="margin-top:0;color:#5a6673">
          Sube un TXT con contratos bloqueados. Nunca se asignaran.
        </p>
        <p style="margin-top:0;color:#5a6673;font-size:.86rem">
          Archivo: <strong>{html.escape(blacklist_status["path"])}</strong> |
          Cargados: <strong>{int(blacklist_status["contracts_loaded"])}</strong>
        </p>
        <a class="btn btn-link btn-alt" href="{html.escape(download_blacklist_path)}">Descargar TXT lista negra</a>
        <form method="post" action="{html.escape(upload_blacklist_path)}" enctype="multipart/form-data">
          <input type="file" name="blacklist_file" accept=".txt,text/plain" required />
          <button class="btn btn-alt" type="submit">Subir TXT lista negra</button>
        </form>

        <hr style="margin:16px 0;border:none;border-top:1px solid #ece7db" />
        <h2 style="margin-top:0">Cierre Masivo</h2>
        <form method="post" action="{html.escape(run_assignment_now_path)}">
          <button class="btn" type="submit">Ejecutar asignacion ahora</button>
        </form>
        <p style="margin-top:0;color:#5a6673">
          Lanza el endpoint de asignacion (rango configurado, balance 60/40 y validacion de lista negra).
        </p>
        <form method="post" action="{html.escape(validate_db_processes_path)}">
          <button class="btn btn-alt" type="submit">Validar procesos BD</button>
        </form>
        <p style="margin:8px 0 0;color:#5a6673;font-size:.86rem">
          Esta validacion ejecuta 1 consulta a alocreditprod y 1 consulta a PostgreSQL.
        </p>
        <form method="post" action="{html.escape(finalize_assignments_path)}">
          <button class="btn" type="submit" style="background:#8a1f1f">Ejecutar cierre masivo</button>
        </form>
        <p style="margin-top:8px;color:#7a2d2d">
          Cierra historial activo y deja vacia la tabla contract_advisors.
        </p>
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


@app.get("/{panel_hash}/login", include_in_schema=False)
async def panel_login_page(
    panel_hash: str,
    request: Request,
    next: str = "",
    error: str = "",
) -> HTMLResponse:
    _assert_hash(panel_hash)
    if not settings.ADMIN_AUTH_ENABLED:
        return HTMLResponse("", status_code=404)

    token = request.cookies.get(settings.ADMIN_AUTH_COOKIE_NAME)
    username = admin_panel_auth_service.validate_session_token(token)
    if username:
        return RedirectResponse(
            url=_safe_next_path(next, panel_hash),
            status_code=303,
            headers=NO_CACHE_HEADERS,
        )

    page = _render_login_html(
        panel_hash=panel_hash,
        error_message=error,
        next_path=next,
    )
    return HTMLResponse(page, headers=NO_CACHE_HEADERS)


@app.post("/{panel_hash}/login", include_in_schema=False)
async def panel_login_submit(
    panel_hash: str,
    username: str = Form(""),
    password: str = Form(""),
    next: str = Form(""),
) -> RedirectResponse:
    _assert_hash(panel_hash)
    if not settings.ADMIN_AUTH_ENABLED:
        return RedirectResponse(url=f"/{panel_hash}", status_code=303, headers=NO_CACHE_HEADERS)

    safe_next = _safe_next_path(next, panel_hash)
    normalized_user = (username or "").strip().lower()
    if not admin_panel_auth_service.verify_credentials(normalized_user, password):
        query = urlencode(
            {
                "next": safe_next,
                "error": "Credenciales invalidas",
            }
        )
        return RedirectResponse(
            url=f"/{panel_hash}/login?{query}",
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


@app.post("/{panel_hash}/logout", include_in_schema=False)
async def panel_logout(panel_hash: str) -> RedirectResponse:
    _assert_hash(panel_hash)
    response = RedirectResponse(
        url=f"/{panel_hash}/login",
        status_code=303,
        headers=NO_CACHE_HEADERS,
    )
    response.delete_cookie(key=settings.ADMIN_AUTH_COOKIE_NAME, path="/")
    return response


@app.get("/{panel_hash}", response_class=HTMLResponse, include_in_schema=False)
async def panel_home(
    request: Request,
    panel_hash: str,
    ok: str = "",
    error: str = "",
    load_mora_auto: str = "",
) -> HTMLResponse:
    _assert_hash(panel_hash)
    auth_redirect = _require_panel_auth(request, panel_hash)
    if auth_redirect is not None:
        return auth_redirect

    page = _render_panel_html(
        panel_hash=panel_hash,
        ok_message=ok,
        error_message=error,
        current_user=str(getattr(request.state, "panel_user", "-")),
        load_mora_auto=(load_mora_auto or "").strip().lower() in {"1", "true", "yes", "on"},
    )
    return HTMLResponse(page, headers=NO_CACHE_HEADERS)


@app.get("/{panel_hash}/reports/{house_key}", include_in_schema=False)
async def download_house_report(request: Request, panel_hash: str, house_key: str):
    _assert_hash(panel_hash)
    auth_redirect = _require_panel_auth(request, panel_hash)
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
    request: Request,
    panel_hash: str,
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
    _assert_hash(panel_hash)
    auth_redirect = _require_panel_auth(request, panel_hash)
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
            _render_assignment_history_report_html(panel_hash, report),
            headers=NO_CACHE_HEADERS,
        )
    except Exception as error:
        logger.error("Error generando informe de asignados/eliminados: %s", error)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.get("/{panel_hash}/history/asignados-eliminados/download", include_in_schema=False)
async def download_assignment_history(
    request: Request,
    panel_hash: str,
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
    _assert_hash(panel_hash)
    auth_redirect = _require_panel_auth(request, panel_hash)
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


@app.get("/{panel_hash}/history/rotacion-mora", response_class=HTMLResponse, include_in_schema=False)
async def mora_rotation_report(
    request: Request,
    panel_hash: str,
    start_date: str = "",
):
    _assert_hash(panel_hash)
    auth_redirect = _require_panel_auth(request, panel_hash)
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
            _render_mora_rotation_report_html(panel_hash, report),
            headers=NO_CACHE_HEADERS,
        )
    except Exception as error:
        logger.error("Error generando informe de rotacion de mora: %s", error)
        query = urlencode({"error": str(error)})
        return RedirectResponse(url=f"/{panel_hash}?{query}", status_code=303)


@app.post("/{panel_hash}/blacklist/upload", include_in_schema=False)
async def upload_contract_blacklist(
    request: Request,
    panel_hash: str,
    blacklist_file: UploadFile = File(...),
) -> RedirectResponse:
    _assert_hash(panel_hash)
    auth_redirect = _require_panel_auth(request, panel_hash)
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
    auth_redirect = _require_panel_auth(request, panel_hash)
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


@app.post("/{panel_hash}/run-assignment-now", include_in_schema=False)
async def run_assignment_now_from_panel(request: Request, panel_hash: str) -> RedirectResponse:
    _assert_hash(panel_hash)
    auth_redirect = _require_panel_auth(request, panel_hash)
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


@app.get("/{panel_hash}/blacklist/download", include_in_schema=False)
async def download_contract_blacklist(
    request: Request,
    panel_hash: str,
):
    _assert_hash(panel_hash)
    auth_redirect = _require_panel_auth(request, panel_hash)
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


@app.post("/{panel_hash}/validate-db-processes", include_in_schema=False)
async def validate_db_processes_from_panel(request: Request, panel_hash: str) -> RedirectResponse:
    _assert_hash(panel_hash)
    auth_redirect = _require_panel_auth(request, panel_hash)
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


@app.post("/{panel_hash}/finalize-assignments", include_in_schema=False)
async def finalize_assignments_from_panel(request: Request, panel_hash: str) -> RedirectResponse:
    _assert_hash(panel_hash)
    auth_redirect = _require_panel_auth(request, panel_hash)
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
