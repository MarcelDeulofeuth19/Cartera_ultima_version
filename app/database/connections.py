"""
Gestión de conexiones a bases de datos MySQL y PostgreSQL.
Implementa el patrón de sesiones con SQLAlchemy.
"""
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from app.core.config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Administrador centralizado de conexiones a bases de datos.
    Implementa el patrón Singleton para reutilizar engines.
    """
    _instance = None
    _mysql_engine = None
    _postgres_engine = None
    _mysql_session_factory = None
    _postgres_session_factory = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Inicializa los engines de base de datos si no existen"""
        if self._mysql_engine is None:
            self._initialize_mysql()
        if self._postgres_engine is None:
            self._initialize_postgres()
    
    def _initialize_mysql(self):
        """Configura el engine de MySQL"""
        try:
            logger.info(f"Conectando a MySQL: {settings.MYSQL_HOST}")
            self._mysql_engine = create_engine(
                settings.mysql_url,
                pool_pre_ping=True,  # Verifica conexiones antes de usarlas
                pool_recycle=3600,   # Recicla conexiones cada hora
                echo=settings.DEBUG  # Log de queries SQL en modo debug
            )
            self._mysql_session_factory = sessionmaker(
                bind=self._mysql_engine,
                autocommit=False,
                autoflush=False
            )
            # Test de conexión
            with self._mysql_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✓ Conexión a MySQL establecida correctamente")
        except Exception as e:
            logger.error(f"✗ Error al conectar con MySQL: {e}")
            raise
    
    def _initialize_postgres(self):
        """Configura el engine de PostgreSQL"""
        try:
            logger.info(f"Conectando a PostgreSQL: {settings.POSTGRES_HOST}")
            self._postgres_engine = create_engine(
                settings.postgres_url,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=settings.DEBUG
            )
            self._postgres_session_factory = sessionmaker(
                bind=self._postgres_engine,
                autocommit=False,
                autoflush=False
            )
            # Test de conexión
            with self._postgres_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✓ Conexión a PostgreSQL establecida correctamente")
        except Exception as e:
            logger.error(f"✗ Error al conectar con PostgreSQL: {e}")
            raise
    
    @contextmanager
    def get_mysql_session(self) -> Session:
        """
        Context manager que proporciona una sesión de MySQL.
        
        Uso:
            with db_manager.get_mysql_session() as session:
                result = session.execute(query)
        """
        session = self._mysql_session_factory()
        try:
            yield session
        except Exception as e:
            logger.error(f"Error en sesión MySQL: {e}")
            session.rollback()
            raise
        finally:
            session.close()
    
    @contextmanager
    def get_postgres_session(self) -> Session:
        """
        Context manager que proporciona una sesión de PostgreSQL.
        Maneja automáticamente commit/rollback.
        
        Uso:
            with db_manager.get_postgres_session() as session:
                session.add(object)
                session.commit()
        """
        session = self._postgres_session_factory()
        try:
            yield session
        except Exception as e:
            logger.error(f"Error en sesión PostgreSQL: {e}")
            session.rollback()
            raise
        finally:
            session.close()
    
    def close_all(self):
        """Cierra todas las conexiones de bases de datos"""
        if self._mysql_engine:
            self._mysql_engine.dispose()
            logger.info("MySQL engine cerrado")
        if self._postgres_engine:
            self._postgres_engine.dispose()
            logger.info("PostgreSQL engine cerrado")


# Instancia global del administrador de bases de datos (Singleton)
db_manager = DatabaseManager()


# Funciones de conveniencia para dependency injection en FastAPI
def get_mysql_session():
    """Dependency injection para sesiones MySQL en FastAPI"""
    with db_manager.get_mysql_session() as session:
        yield session


def get_postgres_session():
    """Dependency injection para sesiones PostgreSQL en FastAPI"""
    with db_manager.get_postgres_session() as session:
        yield session
