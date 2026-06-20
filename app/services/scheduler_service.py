"""
Scheduler interno para ejecutar asignacion automatica en horarios de negocio.
Incluye limpieza automatica de reportes Excel cada 24 horas.
"""
import asyncio
import calendar
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.core.config import settings
from app.core.file_lock import ProcessLockError, acquire_process_lock
from app.database.connections import db_manager
from app.services.assignment_service import AssignmentService

logger = logging.getLogger(__name__)

REPORT_CLEANUP_INTERVAL_HOURS = 24
REPORT_MAX_AGE_HOURS = 24
DATETIME_FORMAT_WITH_TZ = "%Y-%m-%d %H:%M:%S %Z"


class AutoAssignmentScheduler:
    """
    Programa ejecuciones automaticas de asignacion en dias habiles.
    """

    def __init__(self):
        self._task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
        self._monthly_task: asyncio.Task | None = None
        self._monthly_close_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

        try:
            self._timezone = ZoneInfo(settings.AUTO_ASSIGNMENT_TIMEZONE)
        except ZoneInfoNotFoundError:
            logger.warning(
                "Zona horaria invalida '%s'. Usando UTC.",
                settings.AUTO_ASSIGNMENT_TIMEZONE,
            )
            self._timezone = ZoneInfo("UTC")

    async def start(self) -> None:
        """Inicia el scheduler si esta habilitado."""
        active = any(
            t and not t.done()
            for t in (self._task, self._monthly_task, self._monthly_close_task)
        )
        if active:
            logger.info("Scheduler automatico ya se encuentra activo")
            return

        self._stop_event = asyncio.Event()

        if settings.AUTO_ASSIGNMENT_ENABLED:
            self._task = asyncio.create_task(self._run_loop(), name="auto-assignment-scheduler")
            self._cleanup_task = asyncio.create_task(self._cleanup_loop(), name="report-cleanup-scheduler")
            logger.info(
                "Scheduler automatico iniciado: %02d:%02d (%s), dias=%s",
                settings.AUTO_ASSIGNMENT_HOUR,
                settings.AUTO_ASSIGNMENT_MINUTE,
                settings.AUTO_ASSIGNMENT_TIMEZONE,
                settings.auto_assignment_weekday_list,
            )
        else:
            logger.info("Scheduler de asignacion deshabilitado por configuracion")

        # Informe de finalizacion de ciclo: ultimo dia de cada mes (independiente).
        if settings.MONTHLY_REPORT_ENABLED:
            self._monthly_task = asyncio.create_task(
                self._monthly_report_loop(), name="monthly-collection-report"
            )
            logger.info(
                "Informe mensual de finalizacion de ciclo habilitado: ultimo dia de cada mes %02d:%02d (%s)",
                settings.MONTHLY_REPORT_HOUR,
                settings.MONTHLY_REPORT_MINUTE,
                settings.AUTO_ASSIGNMENT_TIMEZONE,
            )

        # Cierre masivo + reasignacion: ultimo dia de cada mes (independiente).
        if settings.MONTHLY_CLOSE_ENABLED:
            self._monthly_close_task = asyncio.create_task(
                self._monthly_close_loop(), name="monthly-close-reassign"
            )
            logger.info(
                "Cierre masivo + reasignacion de fin de mes habilitado: ultimo dia de cada mes %02d:%02d (%s)",
                settings.MONTHLY_CLOSE_HOUR,
                settings.MONTHLY_CLOSE_MINUTE,
                settings.AUTO_ASSIGNMENT_TIMEZONE,
            )

    async def stop(self) -> None:
        """Detiene el scheduler de forma ordenada."""
        self._stop_event.set()

        for task in (self._task, self._cleanup_task, self._monthly_task, self._monthly_close_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._task = None
        self._cleanup_task = None
        self._monthly_task = None
        self._monthly_close_task = None
        logger.info("Scheduler automatico detenido")

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            now = datetime.now(self._timezone)
            next_run = self._next_business_run(now)
            wait_seconds = max(1.0, (next_run - now).total_seconds())

            logger.info(
                "Proxima asignacion automatica programada para %s",
                next_run.strftime(DATETIME_FORMAT_WITH_TZ),
            )

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait_seconds)
                break
            except asyncio.TimeoutError:
                pass

            if self._stop_event.is_set():
                break

            await self._run_once()

    def _next_business_run(self, now: datetime) -> datetime:
        weekdays = settings.auto_assignment_weekday_list

        candidate = now.replace(
            hour=settings.AUTO_ASSIGNMENT_HOUR,
            minute=settings.AUTO_ASSIGNMENT_MINUTE,
            second=0,
            microsecond=0,
        )

        if candidate <= now:
            candidate += timedelta(days=1)

        while candidate.weekday() not in weekdays:
            candidate += timedelta(days=1)

        return candidate

    async def _run_once(self) -> None:
        logger.info("Iniciando ejecucion programada de asignacion...")
        try:
            await asyncio.to_thread(self._run_assignment_sync)
        except Exception as error:
            logger.error("Fallo la ejecucion programada: %s", error, exc_info=True)

    def _run_assignment_sync(self) -> None:
        try:
            with acquire_process_lock():
                with db_manager.get_mysql_session() as mysql_session, db_manager.get_postgres_session() as postgres_session:
                    assignment_service = AssignmentService(mysql_session, postgres_session)
                    results = assignment_service.execute_assignment_process()

                    if results.get("success"):
                        insert_stats = results.get("insert_stats", {})
                        logger.info(
                            "Ejecucion programada completada: insertados=%s",
                            insert_stats.get("inserted_total", 0),
                        )
                    else:
                        logger.warning(
                            "Ejecucion programada finalizo sin success=True. Error=%s",
                            results.get("error"),
                        )
        except ProcessLockError:
            logger.warning(
                "Se omite ejecucion programada porque ya hay un proceso de asignacion en curso"
            )

    async def _monthly_report_loop(self) -> None:
        """Loop que dispara el informe de finalizacion de ciclo el ultimo dia de cada mes."""
        while not self._stop_event.is_set():
            now = datetime.now(self._timezone)
            next_run = self._next_month_end_run(
                now, settings.MONTHLY_REPORT_HOUR, settings.MONTHLY_REPORT_MINUTE
            )
            wait_seconds = max(1.0, (next_run - now).total_seconds())

            logger.info(
                "Proximo informe de finalizacion de ciclo programado para %s",
                next_run.strftime(DATETIME_FORMAT_WITH_TZ),
            )

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait_seconds)
                break
            except asyncio.TimeoutError:
                pass

            if self._stop_event.is_set():
                break

            await self._run_monthly_report_once()

    def _next_month_end_run(self, now: datetime, hour: int, minute: int) -> datetime:
        """Calcula la proxima ejecucion: ultimo dia del mes a la hora dada."""
        last_day = calendar.monthrange(now.year, now.month)[1]
        candidate = now.replace(
            day=last_day,
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0,
        )
        if candidate <= now:
            # Ya paso este mes: ir al ultimo dia del mes siguiente.
            year = now.year + (1 if now.month == 12 else 0)
            month = 1 if now.month == 12 else now.month + 1
            last_day = calendar.monthrange(year, month)[1]
            candidate = candidate.replace(year=year, month=month, day=last_day)
        return candidate

    @staticmethod
    def _monthly_sentinel_path(now: datetime) -> Path:
        """Marca de envio mensual (evita doble disparo tras reinicios)."""
        state_dir = Path("logs")
        return state_dir / f"cycle_end_report_{now.strftime('%Y-%m')}.done"

    async def _run_monthly_report_once(self) -> None:
        logger.info("Iniciando informe de finalizacion de ciclo...")
        try:
            await asyncio.to_thread(self._run_monthly_report_sync)
        except Exception as error:
            logger.error("Fallo informe de finalizacion de ciclo: %s", error, exc_info=True)

    def _run_monthly_report_sync(self) -> None:
        # Import diferido para evitar ciclos de importacion al cargar el modulo.
        from app.services.cycle_end_report_service import generate_and_send_cycle_end_report

        now = datetime.now(self._timezone)
        sentinel = self._monthly_sentinel_path(now)
        if sentinel.exists():
            logger.info(
                "Informe de finalizacion de ciclo de %s ya fue enviado (%s). Se omite.",
                now.strftime("%Y-%m"), sentinel,
            )
            return

        try:
            result = generate_and_send_cycle_end_report(report_date=now.date())
            if result.get("sent"):
                sentinel.parent.mkdir(parents=True, exist_ok=True)
                sentinel.write_text(now.isoformat())
                logger.info(
                    "Informe de finalizacion de ciclo enviado (%s). Marcado en %s",
                    result.get("subject"), sentinel,
                )
            else:
                logger.error(
                    "Informe de finalizacion de ciclo no se envio; se reintentara en la proxima corrida"
                )
        except Exception as error:
            logger.error(
                "Error generando/enviando informe de finalizacion de ciclo: %s",
                error, exc_info=True,
            )

    async def _monthly_close_loop(self) -> None:
        """Loop que dispara el cierre masivo + reasignacion el ultimo dia de cada mes."""
        while not self._stop_event.is_set():
            now = datetime.now(self._timezone)
            next_run = self._next_month_end_run(
                now, settings.MONTHLY_CLOSE_HOUR, settings.MONTHLY_CLOSE_MINUTE
            )
            wait_seconds = max(1.0, (next_run - now).total_seconds())

            logger.info(
                "Proximo cierre masivo + reasignacion de fin de mes programado para %s",
                next_run.strftime(DATETIME_FORMAT_WITH_TZ),
            )

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait_seconds)
                break
            except asyncio.TimeoutError:
                pass

            if self._stop_event.is_set():
                break

            await self._run_monthly_close_once()

    @staticmethod
    def _monthly_close_sentinel_path(now: datetime) -> Path:
        """Marca de cierre+reasignacion mensual (evita doble disparo tras reinicios)."""
        return Path("logs") / f"month_end_close_{now.strftime('%Y-%m')}.done"

    async def _run_monthly_close_once(self) -> None:
        logger.info("Iniciando cierre masivo + reasignacion de fin de mes...")
        try:
            await asyncio.to_thread(self._run_monthly_close_sync)
        except Exception as error:
            logger.error(
                "Fallo cierre + reasignacion de fin de mes: %s", error, exc_info=True
            )

    def _run_monthly_close_sync(self) -> None:
        now = datetime.now(self._timezone)
        sentinel = self._monthly_close_sentinel_path(now)
        if sentinel.exists():
            logger.info(
                "Cierre + reasignacion de %s ya ejecutado (%s). Se omite.",
                now.strftime("%Y-%m"), sentinel,
            )
            return

        try:
            with acquire_process_lock():
                with db_manager.get_mysql_session() as mysql_session, db_manager.get_postgres_session() as postgres_session:
                    service = AssignmentService(mysql_session, postgres_session)

                    logger.info("Fin de mes [1/2]: cierre masivo de asignaciones...")
                    close_stats = service.finalize_all_active_assignments()
                    logger.info("Fin de mes: cierre masivo completado: %s", close_stats)

                    logger.info("Fin de mes [2/2]: reasignacion (execute_assignment_process)...")
                    results = service.execute_assignment_process()
                    if results.get("success"):
                        insert_stats = results.get("insert_stats", {})
                        logger.info(
                            "Fin de mes: reasignacion completada: insertados=%s",
                            insert_stats.get("inserted_total", 0),
                        )
                    else:
                        logger.warning(
                            "Fin de mes: reasignacion finalizo sin success=True. Error=%s",
                            results.get("error"),
                        )

            # Marca solo si todo el proceso termino sin excepcion.
            sentinel.parent.mkdir(parents=True, exist_ok=True)
            sentinel.write_text(now.isoformat())
            logger.info("Cierre + reasignacion de fin de mes marcado en %s", sentinel)
        except ProcessLockError:
            logger.warning(
                "Cierre + reasignacion de fin de mes omitido: ya hay un proceso de asignacion en curso"
            )
        except Exception as error:
            logger.error(
                "Error en cierre + reasignacion de fin de mes: %s", error, exc_info=True
            )

    async def _cleanup_loop(self) -> None:
        """Background loop that cleans up old Excel/report files every 24 hours."""
        while not self._stop_event.is_set():
            try:
                await asyncio.to_thread(self._cleanup_old_reports)
            except Exception as error:
                logger.error("Error en limpieza de reportes: %s", error, exc_info=True)

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=REPORT_CLEANUP_INTERVAL_HOURS * 3600,
                )
                break
            except asyncio.TimeoutError:
                pass

    @staticmethod
    def _cleanup_old_reports() -> None:
        """Delete Excel/report files older than REPORT_MAX_AGE_HOURS."""
        reports_dir = Path(settings.REPORTS_DIR)
        if not reports_dir.exists():
            return

        cutoff = time.time() - (REPORT_MAX_AGE_HOURS * 3600)
        extensions = {".xlsx", ".xls", ".csv", ".txt"}
        removed = 0

        for file_path in reports_dir.iterdir():
            if not file_path.is_file():
                continue
            if file_path.suffix.lower() not in extensions:
                continue
            try:
                if file_path.stat().st_mtime < cutoff:
                    file_path.unlink()
                    removed += 1
            except OSError:
                pass

        if removed:
            logger.info(
                "Limpieza de reportes: %d archivos eliminados en %s (antiguedad > %dh)",
                removed, reports_dir, REPORT_MAX_AGE_HOURS,
            )


auto_assignment_scheduler = AutoAssignmentScheduler()

