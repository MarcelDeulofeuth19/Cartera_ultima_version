"""
Modelos para configuracion dinamica y auditoria del panel administrativo.
"""
from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import declarative_base

RuntimeConfigBase = declarative_base()


class RuntimeAssignmentConfig(RuntimeConfigBase):
    """
    Configuracion activa de asignacion.
    Se guarda una sola fila (id=1) y aplica a futuras corridas.
    """

    __tablename__ = "runtime_assignment_config"

    id = Column(Integer, primary_key=True, default=1)
    serlefin_percent = Column(Float, nullable=False, default=60.0)
    cobyser_percent = Column(Float, nullable=False, default=40.0)
    min_days = Column(Integer, nullable=False, default=61)
    max_days = Column(Integer, nullable=False, default=209)
    updated_by = Column(String(255), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


class RuntimeAssignmentConfigAudit(RuntimeConfigBase):
    """
    Historial de modificaciones del panel.
    Registra campo, valor anterior, valor nuevo, actor y razon.
    """

    __tablename__ = "runtime_assignment_config_audit"

    id = Column(Integer, primary_key=True, autoincrement=True)
    changed_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    actor_email = Column(String(255), nullable=False, index=True)
    changed_field = Column(String(100), nullable=False, index=True)
    old_value = Column(String(255), nullable=True)
    new_value = Column(String(255), nullable=True)
    reason = Column(Text, nullable=True)
    client_ip = Column(String(100), nullable=True)

