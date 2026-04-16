"""
ConfiguraciÃ³n centralizada de la aplicaciÃ³n.
Gestiona las credenciales de bases de datos y parÃ¡metros del sistema.
"""
import re
from pathlib import Path
from pydantic import field_validator
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    """
    ConfiguraciÃ³n de la aplicaciÃ³n usando Pydantic.
    Permite sobrescribir valores desde variables de entorno.
    """
    # ConfiguraciÃ³n de la aplicaciÃ³n
    APP_NAME: str = "Sistema de AsignaciÃ³n de Contratos"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    @field_validator("DEBUG", mode="before")
    @classmethod
    def _normalize_debug_value(cls, value):
        """
        Acepta valores no estandar de entorno (ej: DEBUG=release) sin romper startup.
        """
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on", "debug"}:
                return True
            if normalized in {"0", "false", "no", "off", "release", "prod", "production"}:
                return False
        return value
    
    # MySQL (alocreditprod) - Base de datos de contratos
    MYSQL_HOST: str = "57.130.40.1"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "alo_estadisticas"
    MYSQL_PASSWORD: str = "4K9ml8e2vqlj"
    MYSQL_DATABASE: str = "alocreditprod"
    
    # PostgreSQL (nexus_db) - Base de datos de asignaciones
    POSTGRES_HOST: str = "3.95.195.63"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "nexus_dev_84"
    POSTGRES_PASSWORD: str = "ZehK7wQTpq95eU8r"
    POSTGRES_DATABASE: str = "nexus_db"
    
    # ConfiguraciÃ³n de negocio
    # Casas de Cobranza:
    # - COBYSER: usuarios 45, 46, 47, 48, 49, 50, 51
    # - SERLEFIN: usuarios 81, 82, 83, 84, 85, 86, 102, 103
    COBYSER_USERS: List[int] = [45, 46, 47, 48, 49, 50, 51]
    SERLEFIN_USERS: List[int] = [81, 82, 83, 84, 85, 86, 102, 103]
    
    # Para retrocompatibilidad (usuarios principales de cada casa)
    USER_IDS: List[int] = [45, 81]
    
    # DivisiÃ³n de contratos (dÃ­as 1-60) - 14 usuarios
    DIVISION_USER_IDS: List[int] = [4, 7, 36, 58, 60, 62, 71, 77, 89, 90, 91, 114, 116, 113]
    DIVISION_MIN_DAYS: int = 1  # DÃ­as de atraso mÃ­nimos para divisiÃ³n
    DIVISION_MAX_DAYS: int = 60  # DÃ­as de atraso mÃ¡ximos para divisiÃ³n
    
    DAYS_THRESHOLD: int = 61  # DÃ­as de atraso mÃ­nimos (casas de cobranza)
    MAX_DAYS_THRESHOLD: int = 240  # DÃ­as de atraso mÃ¡ximos (casas de cobranza)
    
    # Efectos que determinan contratos fijos
    EFFECT_ACUERDO_PAGO: str = "acuerdo_de_pago"
    EFFECT_PAGO_TOTAL: str = "pago_total"
    
    # PerÃ­odo de validez para pago_total (en dÃ­as)
    PAGO_TOTAL_VALIDITY_DAYS: int = 30
    
    # Para retrocompatibilidad
    FIXED_CONTRACT_EFFECT: str = "pago_total"
    
    # ConfiguraciÃ³n de reportes
    REPORTS_DIR: str = "reports"
    REPORT_FILE_USER_45: str = "asignacion_45.txt"
    REPORT_FILE_USER_81: str = "asignacion_81.txt"
    REPORT_EXCEL_FIXED: str = "reporte_fijos_efect.xlsx"
    
    # Reportes para divisiÃ³n de contratos (8 usuarios)
    REPORT_FILE_DIVISION: str = "division_contratos_{user_id}.txt"
    REPORT_EXCEL_DIVISION: str = "reporte_division_contratos.xlsx"
    
    # File Lock para singleton
    LOCK_FILE: str = "assignment_process.lock"
    LOCK_TIMEOUT: int = 300  # 5 minutos de timeout

    # Scheduler automatico de asignacion (diario 7:00 AM)
    AUTO_ASSIGNMENT_ENABLED: bool = True
    AUTO_ASSIGNMENT_HOUR: int = 7
    AUTO_ASSIGNMENT_MINUTE: int = 0
    AUTO_ASSIGNMENT_TIMEZONE: str = "America/Bogota"
    # Python weekday: lunes=0 ... domingo=6
    AUTO_ASSIGNMENT_WEEKDAYS: str = "0,1,2,3,4,5,6"
    # Dias para envio de notificaciones por correo
    # Python weekday: lunes=0 ... domingo=6
    AUTO_NOTIFICATION_WEEKDAYS: str = "1,3"

    # Correos de notificacion (separados por coma)
    # - NOTIFICATION_RECIPIENTS: recibe notificacion con ambas bases
    # - COBYSER_NOTIFICATION_RECIPIENTS: recibe notificacion + base de Cobyser
    # - SERLEFIN_NOTIFICATION_RECIPIENTS: recibe solo notificacion (sin Excel)
    NOTIFICATION_RECIPIENTS: str = (
        "mdeulofeuth@alocredit.co,"
        "fcamacho@alocredit.co,jcarrasco@alocredit.co"
    )
    COBYSER_NOTIFICATION_RECIPIENTS: str = (
        "mdeulofeuth@alocredit.co,fcamacho@alocredit.co,jcarrasco@alocredit.co"
    )
    SERLEFIN_NOTIFICATION_RECIPIENTS: str = (
        "mdeulofeuth@alocredit.co,fcamacho@alocredit.co,jcarrasco@alocredit.co"
    )

    # Lista negra de contratos (TXT)
    BLACKLIST_ENABLED: bool = False
    CONTRACT_BLACKLIST_FILE: str = "app/data/contract_blacklist.txt"
    # Lista negra de clientes por documento/cedula (CSV + TXT)
    CLIENT_DOCUMENT_BLACKLIST: str = "500102"
    CLIENT_DOCUMENT_BLACKLIST_FILE: str = "app/data/client_document_blacklist.txt"

    # SMTP para envio de correos
    SMTP_SERVER: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_HELO_NAME: str = "alocredit.co"
    SMTP_USER: str = "noreply@alocredit.co"
    SMTP_PASSWORD: str = "dzxivlyusuprwesu"
    SMTP_FROM: str = "noreply@alocredit.co"
    SERLEFIN_ATTACHMENT_EXCEPTION_RECIPIENTS: str = (
        "mdeulofeuth@alocredit.co,mdeulfoefeuth@alocredit.co"
    )

    # Conexiones para reportes extendidos (PostgreSQL)
    REPORTS_EXT_PROD_HOST: str = "3.95.195.63"
    REPORTS_EXT_PROD_USER: str = "nexus_dev_84"
    REPORTS_EXT_PROD_PASSWORD: str = "ZehK7wQTpq95eU8r"
    REPORTS_EXT_PROD_DATABASE: str = "alocreditprod"
    REPORTS_EXT_PROD_PORT: int = 5432
    REPORTS_EXT_PROD_SCHEMA: str = "alocreditprod"

    REPORTS_EXT_IND_HOST: str = "3.95.195.63"
    REPORTS_EXT_IND_USER: str = "nexus"
    REPORTS_EXT_IND_PASSWORD: str = "AloCredit2025**"
    REPORTS_EXT_IND_DATABASE: str = "nexus_db"
    REPORTS_EXT_IND_PORT: int = 5432
    REPORTS_EXT_IND_SCHEMA: str = "alocreditindicators"

    # Configuracion dinamica de asignacion (persistida con auditoria)
    DEFAULT_SERLEFIN_PERCENT: float = 60.0
    DEFAULT_COBYSER_PERCENT: float = 40.0
    DEFAULT_ASSIGNMENT_MIN_DAYS: int = 61
    DEFAULT_ASSIGNMENT_MAX_DAYS: int = 240

    # Base interna de configuracion/auditoria
    INTERNAL_CONFIG_DATABASE_URL: str = "postgresql+psycopg2://internal_config_user:internal_config_pass@localhost:5559/internal_config_db"

    # Panel visual protegido por hash (acceso por URL secreta)
    ADMIN_PANEL_HASH: str = "3e63c8d8d94f44288b5f90d2c16fd101"
    ADMIN_DEFAULT_AUDIT_ACTOR: str = "mdeulofeuth@alocredit.co"
    ADMIN_AUTH_ENABLED: bool = True
    ADMIN_AUTH_DEFAULT_USERNAME: str = "admin"
    ADMIN_AUTH_DEFAULT_PASSWORD: str = "ChangeMe123!"
    ADMIN_AUTH_SECRET: str = "replace-this-admin-auth-secret"
    ADMIN_AUTH_SESSION_HOURS: int = 12
    ADMIN_AUTH_COOKIE_NAME: str = "alocredit_admin_session"
    ADMIN_AUTH_COOKIE_SECURE: bool = False
    ADMIN_EXTRA_USERS: str = "[]"

    class Config:
        env_file = ".env"
        case_sensitive = True
    
    @property
    def mysql_url(self) -> str:
        """Genera la URL de conexiÃ³n para MySQL"""
        return f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DATABASE}"
    
    @property
    def postgres_url(self) -> str:
        """Genera la URL de conexiÃ³n para PostgreSQL"""
        return f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DATABASE}"
    
    @property
    def all_users(self) -> List[int]:
        """Retorna todos los usuarios de ambas casas de cobranza"""
        return self.COBYSER_USERS + self.SERLEFIN_USERS

    @staticmethod
    def _parse_recipients(raw_value: str) -> List[str]:
        """Convierte una cadena CSV de correos en lista unica y normalizada."""
        recipients = []
        for raw_recipient in str(raw_value or "").split(","):
            recipient = raw_recipient.strip().lower()
            if recipient and recipient not in recipients:
                recipients.append(recipient)
        return recipients

    @property
    def notification_recipients(self) -> List[str]:
        """
        Destinatarios que reciben notificacion con ambas bases (Serlefin y Cobyser).
        """
        return self._parse_recipients(self.NOTIFICATION_RECIPIENTS)

    @property
    def cobyser_notification_recipients(self) -> List[str]:
        """
        Destinatarios que reciben notificacion y base de Cobyser.
        """
        return self._parse_recipients(self.COBYSER_NOTIFICATION_RECIPIENTS)

    @property
    def serlefin_notification_recipients(self) -> List[str]:
        """
        Destinatarios que reciben solo notificacion de Serlefin (sin adjunto).
        """
        return self._parse_recipients(self.SERLEFIN_NOTIFICATION_RECIPIENTS)

    @staticmethod
    def _parse_weekdays(raw_value: str, fallback: List[int]) -> List[int]:
        """Convierte CSV de weekdays (0-6) a lista unica y ordenada por aparicion."""
        weekdays = []
        for raw_day in str(raw_value or "").split(","):
            raw_day = raw_day.strip()
            if not raw_day:
                continue
            try:
                day = int(raw_day)
            except ValueError:
                continue
            if 0 <= day <= 6 and day not in weekdays:
                weekdays.append(day)

        if weekdays:
            return weekdays
        return list(fallback)

    @staticmethod
    def _normalize_document_value(raw_value: str) -> str:
        """Normaliza cedula/documento a solo digitos."""
        cleaned = re.sub(r"\D+", "", str(raw_value or ""))
        return cleaned.strip()

    def _read_blacklisted_documents_from_file(self) -> List[str]:
        """Lee documentos bloqueados desde TXT (1 por linea o mezclado)."""
        path = Path(str(self.CLIENT_DOCUMENT_BLACKLIST_FILE or "").strip())
        if not path.exists():
            return []

        try:
            raw_text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return []

        docs: List[str] = []
        for token in re.findall(r"\d+", raw_text):
            normalized = self._normalize_document_value(token)
            if normalized and normalized not in docs:
                docs.append(normalized)
        return docs

    @property
    def blocked_customer_documents(self) -> List[str]:
        """Retorna lista unica de documentos bloqueados (CSV + archivo)."""
        docs: List[str] = []

        for raw_doc in str(self.CLIENT_DOCUMENT_BLACKLIST or "").split(","):
            normalized = self._normalize_document_value(raw_doc)
            if normalized and normalized not in docs:
                docs.append(normalized)

        for file_doc in self._read_blacklisted_documents_from_file():
            if file_doc not in docs:
                docs.append(file_doc)

        return docs

    @property
    def auto_assignment_weekdays(self) -> List[int]:
        """
        Dias de ejecucion del scheduler en formato weekday de Python.
        """
        return self._parse_weekdays(
            self.AUTO_ASSIGNMENT_WEEKDAYS,
            [0, 1, 2, 3, 4, 5, 6],
        )

    @property
    def auto_notification_weekdays(self) -> List[int]:
        """
        Dias de envio de notificaciones en formato weekday de Python.
        """
        return self._parse_weekdays(
            self.AUTO_NOTIFICATION_WEEKDAYS,
            [1, 3],
        )

    @property
    def serlefin_attachment_exception_recipients(self) -> List[str]:
        """
        Destinatarios para excepcion de adjunto de Serlefin.
        """
        recipients = []
        for raw_recipient in self.SERLEFIN_ATTACHMENT_EXCEPTION_RECIPIENTS.split(","):
            recipient = raw_recipient.strip().lower()
            if recipient and recipient not in recipients:
                recipients.append(recipient)
        return recipients


# Instancia global de configuraciÃ³n (Singleton)
settings = Settings()
