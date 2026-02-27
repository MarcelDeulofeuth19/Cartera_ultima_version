"""
Servicio de configuracion dinamica de asignacion y auditoria de cambios.
"""
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
from typing import Dict, List, Optional

from sqlalchemy import desc

from app.core.config import settings
from app.runtime_config.database import (
    ensure_runtime_config_tables,
    get_runtime_config_session,
)
from app.runtime_config.models import (
    RuntimeAssignmentConfig,
    RuntimeAssignmentConfigAudit,
)


@dataclass
class AssignmentRuntimeConfig:
    serlefin_percent: float
    cobyser_percent: float
    min_days: int
    max_days: int
    updated_by: str
    updated_at: datetime

    @property
    def serlefin_ratio(self) -> float:
        if self.serlefin_percent <= 0:
            return 0.0
        return self.serlefin_percent / 100.0


class RuntimeConfigService:
    """Gestiona configuracion activa y auditoria."""

    def __init__(self):
        ensure_runtime_config_tables()

    def initialize_defaults_if_needed(self) -> None:
        with get_runtime_config_session() as session:
            config = session.get(RuntimeAssignmentConfig, 1)
            if config:
                return

            config = RuntimeAssignmentConfig(
                id=1,
                serlefin_percent=float(settings.DEFAULT_SERLEFIN_PERCENT),
                cobyser_percent=float(settings.DEFAULT_COBYSER_PERCENT),
                min_days=int(settings.DEFAULT_ASSIGNMENT_MIN_DAYS),
                max_days=int(settings.DEFAULT_ASSIGNMENT_MAX_DAYS),
                updated_by=settings.ADMIN_DEFAULT_AUDIT_ACTOR,
            )
            session.add(config)

    def get_assignment_config(self) -> AssignmentRuntimeConfig:
        self.initialize_defaults_if_needed()
        with get_runtime_config_session() as session:
            config = session.get(RuntimeAssignmentConfig, 1)
            if not config:
                raise RuntimeError("No se pudo cargar la configuracion de asignacion")

            return AssignmentRuntimeConfig(
                serlefin_percent=float(config.serlefin_percent),
                cobyser_percent=float(config.cobyser_percent),
                min_days=int(config.min_days),
                max_days=int(config.max_days),
                updated_by=config.updated_by or settings.ADMIN_DEFAULT_AUDIT_ACTOR,
                updated_at=config.updated_at or datetime.utcnow(),
            )

    def update_assignment_config(
        self,
        *,
        actor_email: str,
        serlefin_percent: float,
        cobyser_percent: float,
        min_days: int,
        max_days: int,
        reason: str = "",
        client_ip: Optional[str] = None,
    ) -> Dict[str, object]:
        actor_email = (actor_email or "").strip().lower()
        reason = (reason or "").strip()
        if not actor_email:
            raise ValueError("actor_email es obligatorio para auditoria")

        self._validate_payload(
            serlefin_percent=serlefin_percent,
            cobyser_percent=cobyser_percent,
            min_days=min_days,
            max_days=max_days,
        )

        self.initialize_defaults_if_needed()

        with get_runtime_config_session() as session:
            config = session.get(RuntimeAssignmentConfig, 1)
            if not config:
                raise RuntimeError("No existe configuracion activa para actualizar")

            changes: List[Dict[str, str]] = []

            changes.extend(
                self._collect_change(
                    field_name="serlefin_percent",
                    old_value=float(config.serlefin_percent),
                    new_value=float(serlefin_percent),
                )
            )
            changes.extend(
                self._collect_change(
                    field_name="cobyser_percent",
                    old_value=float(config.cobyser_percent),
                    new_value=float(cobyser_percent),
                )
            )
            changes.extend(
                self._collect_change(
                    field_name="min_days",
                    old_value=int(config.min_days),
                    new_value=int(min_days),
                )
            )
            changes.extend(
                self._collect_change(
                    field_name="max_days",
                    old_value=int(config.max_days),
                    new_value=int(max_days),
                )
            )

            config.serlefin_percent = float(serlefin_percent)
            config.cobyser_percent = float(cobyser_percent)
            config.min_days = int(min_days)
            config.max_days = int(max_days)
            config.updated_by = actor_email
            config.updated_at = datetime.utcnow()

            for change in changes:
                session.add(
                    RuntimeAssignmentConfigAudit(
                        actor_email=actor_email,
                        changed_field=change["field"],
                        old_value=change["old"],
                        new_value=change["new"],
                        reason=reason,
                        client_ip=client_ip,
                    )
                )

            env_updated = self._sync_runtime_values_to_env(
                serlefin_percent=float(serlefin_percent),
                cobyser_percent=float(cobyser_percent),
                min_days=int(min_days),
                max_days=int(max_days),
            )

            return {
                "changed": len(changes) > 0,
                "changes_count": len(changes),
                "changes": changes,
                "env_updated": env_updated,
            }

    def list_audit(self, limit: int = 100) -> List[Dict[str, object]]:
        with get_runtime_config_session() as session:
            rows = (
                session.query(RuntimeAssignmentConfigAudit)
                .order_by(desc(RuntimeAssignmentConfigAudit.changed_at))
                .limit(limit)
                .all()
            )

            return [
                {
                    "changed_at": row.changed_at,
                    "actor_email": row.actor_email,
                    "changed_field": row.changed_field,
                    "old_value": row.old_value,
                    "new_value": row.new_value,
                    "reason": row.reason,
                    "client_ip": row.client_ip,
                }
                for row in rows
            ]

    @staticmethod
    def _collect_change(field_name: str, old_value: object, new_value: object) -> List[Dict[str, str]]:
        if old_value == new_value:
            return []
        return [
            {
                "field": field_name,
                "old": str(old_value),
                "new": str(new_value),
            }
        ]

    @staticmethod
    def _validate_payload(
        *,
        serlefin_percent: float,
        cobyser_percent: float,
        min_days: int,
        max_days: int,
    ) -> None:
        if serlefin_percent < 0 or cobyser_percent < 0:
            raise ValueError("Los porcentajes no pueden ser negativos")

        total = round(float(serlefin_percent) + float(cobyser_percent), 6)
        if abs(total - 100.0) > 0.001:
            raise ValueError("La suma de porcentajes debe ser 100")

        if min_days < 0:
            raise ValueError("El rango minimo no puede ser negativo")
        if max_days < min_days:
            raise ValueError("El rango maximo no puede ser menor al minimo")

    @staticmethod
    def _sync_runtime_values_to_env(
        *,
        serlefin_percent: float,
        cobyser_percent: float,
        min_days: int,
        max_days: int,
    ) -> bool:
        """
        Sincroniza en .env los valores dinamicos para trazabilidad operativa.
        """
        env_path = Path(".env")
        env_values = {
            "DEFAULT_SERLEFIN_PERCENT": f"{serlefin_percent:.2f}",
            "DEFAULT_COBYSER_PERCENT": f"{cobyser_percent:.2f}",
            "DEFAULT_ASSIGNMENT_MIN_DAYS": str(min_days),
            "DEFAULT_ASSIGNMENT_MAX_DAYS": str(max_days),
            "DAYS_THRESHOLD": str(min_days),
            "MAX_DAYS_THRESHOLD": str(max_days),
        }
        return RuntimeConfigService._upsert_env_values(env_path=env_path, values=env_values)

    @staticmethod
    def _upsert_env_values(env_path: Path, values: Dict[str, str]) -> bool:
        """
        Inserta o actualiza llaves en archivo .env.
        Retorna True si hubo cambios en el archivo.
        """
        key_pattern = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")
        file_exists = env_path.exists()

        if file_exists:
            lines = env_path.read_text(encoding="utf-8").splitlines()
        else:
            lines = []

        pending_keys = set(values.keys())
        new_lines: List[str] = []
        changed = not file_exists

        for line in lines:
            match = key_pattern.match(line)
            if not match:
                new_lines.append(line)
                continue

            key = match.group(1)
            if key not in values:
                new_lines.append(line)
                continue

            replacement = f"{key}={values[key]}"
            if line != replacement:
                changed = True
            new_lines.append(replacement)
            pending_keys.discard(key)

        if pending_keys:
            if new_lines and new_lines[-1].strip():
                new_lines.append("")
            for key in sorted(pending_keys):
                new_lines.append(f"{key}={values[key]}")
            changed = True

        if changed:
            env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

        return changed
