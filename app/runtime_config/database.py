"""
Conexion y sesiones para la base interna de configuracion/auditoria.
"""
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings
from app.runtime_config.models import RuntimeConfigBase

_engine = create_engine(
    settings.INTERNAL_CONFIG_DATABASE_URL,
    pool_pre_ping=True,
    future=True,
)

_SessionLocal = sessionmaker(
    bind=_engine,
    autoflush=False,
    autocommit=False,
)


def ensure_runtime_config_tables() -> None:
    """Crea las tablas internas si aun no existen."""
    RuntimeConfigBase.metadata.create_all(bind=_engine)


@contextmanager
def get_runtime_config_session() -> Session:
    """Context manager de sesion para base interna."""
    session: Session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

