"""
Scheduler interno para ejecutar asignacion automatica en horarios de negocio.
Incluye limpieza automatica de reportes Excel cada 24 horas.
"""
import asyncio
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


class AutoAssignmentScheduler:
    """
    Programa ejecuciones automaticas de asignacion en dias habiles.
    """

    def __init__(self):
        self._task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
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
        if not settings.AUTO_ASSIGNMENT_ENABLED:
            logger.info("Scheduler automatico deshabilitado por configuracion")
            return

        if self._task and not self._task.done():
            logger.info("Scheduler automatico ya se encuentra activo")
            return

        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop(), name="auto-assignment-scheduler")
        self._cleanup_task = asyncio.create_task(self._cleanup_loop(), name="report-cleanup-scheduler")

        logger.info(
            "Scheduler automatico iniciado: %02d:%02d (%s), dias=%s",
            settings.AUTO_ASSIGNMENT_HOUR,
            settings.AUTO_ASSIGNMENT_MINUTE,
            settings.AUTO_ASSIGNMENT_TIMEZONE,
            settings.auto_assignment_weekdays,
        )

    async def stop(self) -> None:
        """Detiene el scheduler de forma ordenada."""
        self._stop_event.set()

        for task in (self._task, self._cleanup_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._task = None
        self._cleanup_task = None
        logger.info("Scheduler automatico detenido")

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            now = datetime.now(self._timezone)
            next_run = self._next_business_run(now)
            wait_seconds = max(1.0, (next_run - now).total_seconds())

            logger.info(
                "Proxima asignacion automatica programada para %s",
                next_run.strftime("%Y-%m-%d %H:%M:%S %Z"),
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
        weekdays = settings.auto_assignment_weekdays

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

