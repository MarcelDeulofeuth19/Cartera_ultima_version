"""
Aplicacion principal FastAPI - Sistema de Asignacion de Contratos.
Punto de entrada de la aplicacion.
"""
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.api.routes.assignment import router as assignment_router
from app.api.routes.collection_agency import router as collection_agency_router
from app.core.config import settings
from app.database.connections import db_manager
from app.runtime_config.service import RuntimeConfigService
from app.services.scheduler_service import auto_assignment_scheduler

# Configuracion de logging
logging.basicConfig(
    level=logging.INFO if not settings.DEBUG else logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("assignment_process.log", encoding="utf-8"),
    ],
)

logger = logging.getLogger(__name__)
runtime_config_service = RuntimeConfigService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestion del ciclo de vida de la aplicacion.
    Ejecuta codigo al inicio y al cierre.
    """
    logger.info("=" * 100)
    logger.info("Iniciando %s v%s", settings.APP_NAME, settings.APP_VERSION)
    logger.info("=" * 100)

    try:
        logger.info("Verificando conexiones de bases de datos...")

        with db_manager.get_mysql_session() as mysql_session:
            mysql_session.execute(text("SELECT 1"))
            logger.info("MySQL conectado correctamente")

        with db_manager.get_postgres_session() as postgres_session:
            postgres_session.execute(text("SELECT 1"))
            logger.info("PostgreSQL conectado correctamente")

        runtime_config_service.initialize_defaults_if_needed()
        logger.info("Configuracion dinamica de asignacion inicializada")

        logger.info("=" * 100)
        logger.info("Aplicacion iniciada correctamente")
        logger.info("  - Documentacion API: http://localhost:8000/docs")
        logger.info("  - Health check: http://localhost:8000/api/v1/health")

        if settings.AUTO_ASSIGNMENT_ENABLED:
            logger.info(
                "  - Scheduler autoasignacion: dias=%s hora=%02d:%02d zona=%s",
                settings.auto_assignment_weekdays,
                settings.AUTO_ASSIGNMENT_HOUR,
                settings.AUTO_ASSIGNMENT_MINUTE,
                settings.AUTO_ASSIGNMENT_TIMEZONE,
            )

        logger.info("=" * 100)

        await auto_assignment_scheduler.start()

        yield

    finally:
        await auto_assignment_scheduler.stop()
        logger.info("Cerrando conexiones de bases de datos...")
        db_manager.close_all()
        logger.info("Aplicacion cerrada correctamente")


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
    API Monolito para Asignacion Automatica de Contratos

    Caracteristicas principales:
    - Singleton Pattern: Garantiza una unica instancia en ejecucion
    - Dual Database: Integracion MySQL + PostgreSQL
    - Balanceo y reglas de negocio por dias de atraso
    - Reportes automaticos
    """,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(assignment_router)
app.include_router(collection_agency_router)


@app.get("/", tags=["root"])
async def root():
    """Endpoint raiz con informacion basica de la API."""
    return {
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "documentation": "/docs",
        "health_check": "/api/v1/health",
    }


if __name__ == "__main__":
    import uvicorn

    logger.info("Ejecutando %s en modo development", settings.APP_NAME)

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level="info",
    )

