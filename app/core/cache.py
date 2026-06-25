"""
Caché basada en Redis con degradación elegante.

Diseño senior: si Redis está deshabilitado o no está disponible, todas las
operaciones se vuelven *no-op* (``get`` -> ``None``, ``set`` -> ignora). De este
modo la aplicación NUNCA falla por culpa del caché: simplemente recalcula.

El cliente se conecta de forma perezosa y, ante el primer fallo, se marca como
no disponible para no reintentar en cada petición.
"""
import json
import logging
from typing import Any, Optional

from app.core.config import settings

logger = logging.getLogger(__name__)


class RedisCache:
    """Wrapper de caché Redis tolerante a fallos."""

    def __init__(self) -> None:
        self._client = None
        self._unavailable = False

    @property
    def available(self) -> bool:
        """True si el caché está habilitado y operativo."""
        return bool(settings.REDIS_ENABLED) and not self._unavailable and self._get_client() is not None

    def _get_client(self):
        if not settings.REDIS_ENABLED or self._unavailable:
            return None
        if self._client is not None:
            return self._client
        try:
            import redis  # import perezoso: dependencia opcional

            client = redis.Redis.from_url(
                settings.redis_url,
                socket_connect_timeout=2,
                socket_timeout=2,
                decode_responses=True,
            )
            client.ping()
            self._client = client
            logger.info(
                "Caché Redis conectado (%s:%s/%s)",
                settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_DB,
            )
            return client
        except Exception as error:  # noqa: BLE001 - degradar ante cualquier fallo
            self._unavailable = True
            logger.warning("Redis no disponible; caché deshabilitado: %s", error)
            return None

    def get_json(self, key: str) -> Optional[Any]:
        """Devuelve el valor cacheado (deserializado) o None si no hay/falla."""
        client = self._get_client()
        if client is None:
            return None
        try:
            raw = client.get(key)
            return json.loads(raw) if raw is not None else None
        except Exception as error:  # noqa: BLE001
            logger.warning("Fallo leyendo caché '%s': %s", key, error)
            return None

    def set_json(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        """Guarda un valor serializable en JSON con TTL opcional. No lanza."""
        client = self._get_client()
        if client is None:
            return False
        try:
            raw = json.dumps(value, ensure_ascii=False, default=str)
            client.set(key, raw, ex=ttl_seconds)
            return True
        except Exception as error:  # noqa: BLE001
            logger.warning("Fallo guardando caché '%s': %s", key, error)
            return False

    def delete(self, *keys: str) -> None:
        """Elimina una o más claves. No lanza."""
        client = self._get_client()
        if client is None or not keys:
            return
        try:
            client.delete(*keys)
        except Exception as error:  # noqa: BLE001
            logger.warning("Fallo borrando caché %s: %s", keys, error)


# Instancia compartida a nivel de proceso.
cache = RedisCache()
