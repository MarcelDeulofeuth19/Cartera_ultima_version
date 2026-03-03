"""
Servicio para administrar lista negra de contratos.
Lee/escribe un TXT y expone contratos bloqueados para asignacion.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Set

from app.core.config import settings

logger = logging.getLogger(__name__)


class BlacklistService:
    """Gestiona lista negra persistida en archivo TXT."""

    def __init__(self, blacklist_file: str):
        self._path = Path(blacklist_file)
        self._cached_mtime: float | None = None
        self._cached_ids: Set[int] = set()

    @property
    def path(self) -> Path:
        return self._path

    def _parse_contract_ids(self, raw_text: str) -> Set[int]:
        contract_ids: Set[int] = set()
        for token in re.findall(r"\d+", raw_text or ""):
            try:
                contract_id = int(token)
            except ValueError:
                continue
            if contract_id > 0:
                contract_ids.add(contract_id)
        return contract_ids

    def load_contract_ids(self, force_reload: bool = False) -> Set[int]:
        """Carga IDs bloqueados desde TXT (con cache por mtime)."""
        path = self._path
        if not path.exists():
            self._cached_mtime = None
            self._cached_ids = set()
            return set()

        mtime = path.stat().st_mtime
        if (
            not force_reload
            and self._cached_mtime is not None
            and mtime == self._cached_mtime
        ):
            return set(self._cached_ids)

        raw_text = path.read_text(encoding="utf-8", errors="ignore")
        contract_ids = self._parse_contract_ids(raw_text)
        self._cached_ids = set(contract_ids)
        self._cached_mtime = mtime

        logger.info(
            "Lista negra cargada: %s contratos bloqueados (%s)",
            len(contract_ids),
            path,
        )
        return set(contract_ids)

    def save_from_text(self, raw_text: str) -> dict:
        """
        Guarda TXT normalizado (1 contrato por linea) y recarga cache.
        """
        path = self._path
        path.parent.mkdir(parents=True, exist_ok=True)

        contract_ids = sorted(self._parse_contract_ids(raw_text))
        normalized = "\n".join(str(contract_id) for contract_id in contract_ids)
        if normalized:
            normalized += "\n"

        path.write_text(normalized, encoding="utf-8")
        self.load_contract_ids(force_reload=True)

        return {
            "path": str(path),
            "contracts_loaded": len(contract_ids),
        }

    def status(self) -> dict:
        contract_ids = self.load_contract_ids()
        return {
            "path": str(self._path),
            "exists": self._path.exists(),
            "contracts_loaded": len(contract_ids),
        }


blacklist_service = BlacklistService(settings.CONTRACT_BLACKLIST_FILE)

