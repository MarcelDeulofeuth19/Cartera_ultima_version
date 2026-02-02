"""Core module - Configuración y utilidades centrales"""
from app.core.config import settings
from app.core.file_lock import acquire_process_lock, ProcessLockError

__all__ = ["settings", "acquire_process_lock", "ProcessLockError"]
