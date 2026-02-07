"""
Aplicación principal FastAPI - Sistema de Asignación de Contratos.
Punto de entrada de la aplicación.
"""
import logging
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from app.core.config import settings
from app.database.connections import db_manager
from app.api.routes.assignment import router as assignment_router
from app.api.routes.collection_agency import router as collection_agency_router

# Configuración de logging
logging.basicConfig(
    level=logging.INFO if not settings.DEBUG else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('assignment_process.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestión del ciclo de vida de la aplicación.
    Ejecuta código al inicio y al cierre de la aplicación.
    """
    # Startup
    logger.info("=" * 100)
    logger.info(f"Iniciando {settings.APP_NAME} v{settings.APP_VERSION}")
    logger.info("=" * 100)
    
    try:
        # Verificar conexiones de base de datos
        logger.info("Verificando conexiones de bases de datos...")
        
        with db_manager.get_mysql_session() as mysql_session:
            mysql_session.execute(text("SELECT 1"))
            logger.info("✓ MySQL conectado correctamente")
        
        with db_manager.get_postgres_session() as postgres_session:
            postgres_session.execute(text("SELECT 1"))
            logger.info("✓ PostgreSQL conectado correctamente")
        
        logger.info("=" * 100)
        logger.info("✓ Aplicación iniciada correctamente")
        logger.info(f"  - Documentación API: http://localhost:8000/docs")
        logger.info(f"  - Health check: http://localhost:8000/api/v1/health")
        logger.info("=" * 100)
        
        yield
        
    finally:
        # Shutdown
        logger.info("Cerrando conexiones de bases de datos...")
        db_manager.close_all()
        logger.info("✓ Aplicación cerrada correctamente")


# Crear aplicación FastAPI
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
    API Monolito para Asignación Automática de Contratos
    
    ## Características principales:
    - 🔒 **Singleton Pattern**: Garantiza una única instancia en ejecución
    - 🗄️ **Dual Database**: Integración MySQL + PostgreSQL
    - ⚖️ **Balanceo 50/50**: Distribución equitativa entre usuarios
    - 📊 **Reportes Automáticos**: Generación de TXT y Excel
    - 🔄 **Transaccionalidad**: Commit/Rollback automático
    
    ## Endpoints disponibles:
    - `POST /api/v1/run-assignment`: Ejecuta el proceso completo
    - `GET /api/v1/lock-status`: Verifica estado del proceso
    - `GET /api/v1/health`: Health check de la API
    """,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS (permite peticiones desde cualquier origen en desarrollo)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar routers
app.include_router(assignment_router)
app.include_router(collection_agency_router)


@app.get("/", tags=["root"])
async def root():
    """Endpoint raíz con información básica de la API"""
    return {
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "documentation": "/docs",
        "health_check": "/api/v1/health"
    }


if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Ejecutando {settings.APP_NAME} en modo development")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level="info"
    )
