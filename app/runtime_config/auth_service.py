"""
Servicio de autenticacion para el panel administrativo.
Usa la base interna runtime_config para credenciales de acceso.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from datetime import datetime
from typing import Optional

from app.core.config import settings
from app.runtime_config.database import (
    ensure_runtime_config_tables,
    get_runtime_config_session,
)
from app.runtime_config.models import RuntimeAdminPanelUser


class AdminPanelAuthService:
    """Gestiona usuarios del panel y sesiones firmadas en cookie."""

    def __init__(self):
        ensure_runtime_config_tables()

    @staticmethod
    def _hash_password(password: str, salt_hex: str) -> str:
        password_bytes = (password or "").encode("utf-8")
        salt_bytes = bytes.fromhex(salt_hex)
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            password_bytes,
            salt_bytes,
            120000,
        )
        return digest.hex()

    def _ensure_user(self, username: str, password: str) -> None:
        """Create user if not exists."""
        normalized = (username or "").strip().lower()
        if not normalized or not password:
            return

        with get_runtime_config_session() as session:
            existing = (
                session.query(RuntimeAdminPanelUser)
                .filter(RuntimeAdminPanelUser.username == normalized)
                .first()
            )
            if existing:
                return

            salt_hex = os.urandom(16).hex()
            password_hash = self._hash_password(password, salt_hex)
            session.add(
                RuntimeAdminPanelUser(
                    username=normalized,
                    password_salt=salt_hex,
                    password_hash=password_hash,
                    is_active=True,
                )
            )

    def initialize_default_user_if_needed(self) -> None:
        """
        Crea usuario admin y usuarios extra si aun no existen.
        """
        self._ensure_user(
            settings.ADMIN_AUTH_DEFAULT_USERNAME,
            settings.ADMIN_AUTH_DEFAULT_PASSWORD,
        )

        import json
        try:
            extra_users = json.loads(settings.ADMIN_EXTRA_USERS or "[]")
            for user_data in extra_users:
                self._ensure_user(
                    user_data.get("username", ""),
                    user_data.get("password", ""),
                )
        except (json.JSONDecodeError, TypeError):
            pass

    def verify_credentials(self, username: str, password: str) -> bool:
        """
        Valida credenciales contra runtime_admin_panel_users.
        """
        normalized_user = (username or "").strip().lower()
        if not normalized_user or not password:
            return False

        with get_runtime_config_session() as session:
            user = (
                session.query(RuntimeAdminPanelUser)
                .filter(RuntimeAdminPanelUser.username == normalized_user)
                .first()
            )
            if not user or not bool(user.is_active):
                return False

            expected_hash = self._hash_password(password, user.password_salt)
            if not hmac.compare_digest(expected_hash, user.password_hash):
                return False

            user.last_login_at = datetime.utcnow()
            return True

    @staticmethod
    def create_session_token(username: str) -> str:
        """
        Crea token firmado HMAC para cookie de sesion.
        """
        safe_username = (username or "").strip().lower()
        expires_at = int(time.time()) + int(settings.ADMIN_AUTH_SESSION_HOURS) * 3600
        payload = f"{safe_username}|{expires_at}"
        signature = hmac.new(
            settings.ADMIN_AUTH_SECRET.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        raw_token = f"{payload}|{signature}".encode("utf-8")
        return base64.urlsafe_b64encode(raw_token).decode("utf-8")

    @staticmethod
    def validate_session_token(token: Optional[str]) -> Optional[str]:
        """
        Retorna username autenticado si token es valido y vigente.
        """
        if not token:
            return None

        try:
            decoded = base64.urlsafe_b64decode(token.encode("utf-8")).decode("utf-8")
            username, expires_at_raw, signature = decoded.split("|", 2)
            payload = f"{username}|{expires_at_raw}"
            expected_signature = hmac.new(
                settings.ADMIN_AUTH_SECRET.encode("utf-8"),
                payload.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            if not hmac.compare_digest(signature, expected_signature):
                return None

            expires_at = int(expires_at_raw)
            if expires_at < int(time.time()):
                return None

            return username.strip().lower() or None
        except Exception:
            return None


admin_panel_auth_service = AdminPanelAuthService()

