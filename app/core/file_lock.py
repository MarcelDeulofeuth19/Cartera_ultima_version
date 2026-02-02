"""
Implementación de File Lock para garantizar una única instancia del proceso.
Utiliza filelock para manejar el bloqueo a nivel de sistema operativo.
"""
import os
import logging
from filelock import FileLock, Timeout
from contextlib import contextmanager
from app.core.config import settings

logger = logging.getLogger(__name__)


class ProcessLockError(Exception):
    """Excepción personalizada para errores de bloqueo de proceso"""
    pass


@contextmanager
def acquire_process_lock():
    """
    Context manager que garantiza que solo una instancia del proceso
    puede ejecutarse a la vez utilizando file lock.
    
    Uso:
        with acquire_process_lock():
            # código del proceso
    
    Raises:
        ProcessLockError: Si no se puede adquirir el lock (otra instancia corriendo)
    """
    lock_file_path = os.path.join(os.getcwd(), settings.LOCK_FILE)
    lock = FileLock(lock_file_path, timeout=1)
    
    try:
        logger.info(f"Intentando adquirir lock: {lock_file_path}")
        lock.acquire()
        logger.info("✓ Lock adquirido exitosamente. Proceso iniciado.")
        
        try:
            yield lock
        finally:
            lock.release()
            logger.info("✓ Lock liberado exitosamente. Proceso finalizado.")
            
            # Limpieza del archivo de lock
            try:
                if os.path.exists(lock_file_path):
                    os.remove(lock_file_path)
            except Exception as e:
                logger.warning(f"No se pudo eliminar el archivo de lock: {e}")
    
    except Timeout:
        error_msg = (
            "⚠️ Otra instancia del proceso de asignación ya está en ejecución. "
            "Por favor, espera a que termine o verifica si el proceso está bloqueado."
        )
        logger.error(error_msg)
        raise ProcessLockError(error_msg)
    
    except Exception as e:
        logger.error(f"Error inesperado al manejar el lock: {e}")
        raise ProcessLockError(f"Error al gestionar el lock del proceso: {str(e)}")


def check_lock_status() -> dict:
    """
    Verifica el estado actual del lock sin intentar adquirirlo.
    
    Returns:
        dict: Información sobre el estado del lock
    """
    lock_file_path = os.path.join(os.getcwd(), settings.LOCK_FILE)
    
    return {
        "lock_file": lock_file_path,
        "exists": os.path.exists(lock_file_path),
        "is_locked": os.path.exists(lock_file_path) and os.path.isfile(lock_file_path)
    }
