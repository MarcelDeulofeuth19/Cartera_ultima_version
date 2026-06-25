"""
Configuración centralizada de la aplicación.
Gestiona las credenciales de bases de datos y parámetros del sistema.
"""
import re
from pathlib import Path
from pydantic import field_validator
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    """
    Configuración de la aplicación usando Pydantic.
    Permite sobrescribir valores desde variables de entorno.
    """
    # Configuración de la aplicación
    APP_NAME: str = "Sistema de Asignación de Contratos"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    @field_validator("DEBUG", mode="before")
    @classmethod
    def _normalize_debug_value(cls, value):
        """
        Acepta valores no estándar de entorno (ej: DEBUG=release) sin romper el arranque.
        """
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on", "debug"}:
                return True
            if normalized in {"0", "false", "no", "off", "release", "prod", "production"}:
                return False
        return value

    # MySQL (alocreditprod) - Base de datos de contratos
    # Host y credenciales se cargan desde el .env (ver .env.example)
    MYSQL_HOST: str = ""
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = ""
    MYSQL_PASSWORD: str = ""
    MYSQL_DATABASE: str = "alocreditprod"

    # PostgreSQL (nexus_db) - Base de datos de asignaciones
    # Host y credenciales se cargan desde el .env (ver .env.example)
    POSTGRES_HOST: str = ""
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DATABASE: str = "nexus_db"

    # Configuración de negocio
    # Casas de Cobranza:
    # - COBYSER: usuarios 45, 46, 47, 48, 49, 50, 51
    # - SERLEFIN: usuarios 81, 82, 83, 84, 85, 86, 102, 103
    COBYSER_USERS: List[int] = [45, 46, 47, 48, 49, 50, 51]
    SERLEFIN_USERS: List[int] = [81, 82, 83, 84, 85, 86, 102, 103]

    # Para retrocompatibilidad (usuarios principales de cada casa)
    USER_IDS: List[int] = [45, 81]

    # División de contratos (días 1-60) - 14 usuarios
    DIVISION_USER_IDS: List[int] = [4, 7, 36, 58, 60, 62, 71, 77, 89, 90, 91, 114, 116, 113]
    DIVISION_MIN_DAYS: int = 1   # Días de atraso mínimos para división
    DIVISION_MAX_DAYS: int = 60  # Días de atraso máximos para división

    DAYS_THRESHOLD: int = 61      # Días de atraso mínimos (casas de cobranza)
    MAX_DAYS_THRESHOLD: int = 240  # Días de atraso máximos (casas de cobranza)

    # Franja Cobyser (días 31-60): buckets 31_45 y 46_60.
    # Se asignan SOLO a Cobyser (user 45) y SOLO a cédulas con dígito final
    # impar (1,3,5,7,9). Serlefín 0%. Etiqueta de tipo: "Cédulas Impar".
    FRANJA_COBYSER_ENABLED: bool = True
    FRANJA_COBYSER_MIN_DAYS: int = 31
    FRANJA_COBYSER_MAX_DAYS: int = 60
    FRANJA_COBYSER_USER_ID: int = 45

    # Regla: contratos ENDOSADOS a afianzadora (pagaré) NO se asignan ni se
    # reportan. Estados: 1=Libraval, 2=Fianzavasa, 3=Figarantías.
    PAGARE_EXCLUDE_ENABLED: bool = True
    PAGARE_EXCLUDED_STATUS_IDS: str = "1,2,3"

    # Usuario principal de cada casa para el reparto 40/60.
    # (Cobyser usa FRANJA_COBYSER_USER_ID; aquí el principal de Serlefín.)
    SERLEFIN_PRIMARY_USER_ID: int = 81

    # Estados de contrato EXCLUIDOS del proceso (anulado/fraude). Aplican a Phone y Twist.
    EXCLUDED_CONTRACT_STATUS_IDS: str = "5,7"
    # Estado de pago "Atrasado" (cartera en mora) por producto.
    PHONE_ARREARS_PAYMENT_STATUS_ID: int = 4
    TWIST_ARREARS_PAYMENT_STATUS_ID: int = 3

    # --- Caché Redis (opcional; degradación elegante si no está disponible) ---
    REDIS_ENABLED: bool = True
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""
    CACHE_ASSIGNMENTS_TTL_SECONDS: int = 86400  # 24 h (caché diario)

    # Efectos que determinan contratos fijos
    EFFECT_ACUERDO_PAGO: str = "acuerdo_de_pago"
    EFFECT_PAGO_TOTAL: str = "pago_total"

    # Período de validez para pago_total (en días)
    PAGO_TOTAL_VALIDITY_DAYS: int = 30

    # Para retrocompatibilidad
    FIXED_CONTRACT_EFFECT: str = "pago_total"

    # Configuración de reportes
    REPORTS_DIR: str = "reports"
    REPORT_FILE_USER_45: str = "asignacion_45.txt"
    REPORT_FILE_USER_81: str = "asignacion_81.txt"
    REPORT_EXCEL_FIXED: str = "reporte_fijos_efect.xlsx"

    # Reportes para división de contratos
    REPORT_FILE_DIVISION: str = "division_contratos_{user_id}.txt"
    REPORT_EXCEL_DIVISION: str = "reporte_division_contratos.xlsx"

    # File Lock para singleton
    LOCK_FILE: str = "assignment_process.lock"
    LOCK_TIMEOUT: int = 300  # 5 minutos de timeout

    # Scheduler automático de asignación (diario 7:00 AM)
    AUTO_ASSIGNMENT_ENABLED: bool = True
    AUTO_ASSIGNMENT_HOUR: int = 7
    AUTO_ASSIGNMENT_MINUTE: int = 0
    AUTO_ASSIGNMENT_TIMEZONE: str = "America/Bogota"
    # Python weekday: lunes=0 ... domingo=6
    AUTO_ASSIGNMENT_WEEKDAYS: str = "0,1,2,3,4,5,6"
    # Días para envío de notificaciones por correo
    # Python weekday: lunes=0 ... domingo=6
    AUTO_NOTIFICATION_WEEKDAYS: str = "1,3"

    # Informe de finalización de ciclo (casa de cobranza).
    # Se envía automáticamente el ÚLTIMO día de cada mes.
    MONTHLY_REPORT_ENABLED: bool = True
    MONTHLY_REPORT_HOUR: int = 8       # hora local (America/Bogota)
    MONTHLY_REPORT_MINUTE: int = 0
    # Destinatarios del informe mensual (separados por coma)
    MONTHLY_REPORT_TO: str = "jcarrasco@alocredit.co"
    MONTHLY_REPORT_CC: str = "mdeulofeuth@alocredit.co"

    # Cierre masivo + reasignación de fin de mes.
    # Se ejecuta el ÚLTIMO día de cada mes: cierra todas las asignaciones activas
    # (finalize_all_active_assignments) y luego reasigna (execute_assignment_process).
    MONTHLY_CLOSE_ENABLED: bool = True
    MONTHLY_CLOSE_HOUR: int = 23      # hora local (America/Bogota)
    MONTHLY_CLOSE_MINUTE: int = 0

    # Correos de notificación (separados por coma)
    # - NOTIFICATION_RECIPIENTS: recibe notificación con ambas bases
    # - COBYSER_NOTIFICATION_RECIPIENTS: recibe notificación + base de Cobyser
    # - SERLEFIN_NOTIFICATION_RECIPIENTS: recibe solo notificación (sin Excel)
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
    # Lista negra de clientes por documento/cédula (CSV + TXT)
    CLIENT_DOCUMENT_BLACKLIST: str = "500102"
    CLIENT_DOCUMENT_BLACKLIST_FILE: str = "app/data/client_document_blacklist.txt"

    # SMTP para envío de correos
    SMTP_SERVER: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_HELO_NAME: str = "alocredit.co"
    SMTP_USER: str = "noreply@alocredit.co"
    SMTP_PASSWORD: str = ""  # se carga desde el .env
    SMTP_FROM: str = "noreply@alocredit.co"
    SERLEFIN_ATTACHMENT_EXCEPTION_RECIPIENTS: str = (
        "mdeulofeuth@alocredit.co,mdeulfoefeuth@alocredit.co"
    )

    # Conexiones para reportes extendidos (PostgreSQL)
    # Hosts y credenciales se cargan desde el .env (ver .env.example)
    REPORTS_EXT_PROD_HOST: str = ""
    REPORTS_EXT_PROD_USER: str = ""
    REPORTS_EXT_PROD_PASSWORD: str = ""
    REPORTS_EXT_PROD_DATABASE: str = "alocreditprod"
    REPORTS_EXT_PROD_PORT: int = 5432
    REPORTS_EXT_PROD_SCHEMA: str = "alocreditprod"

    REPORTS_EXT_IND_HOST: str = ""
    REPORTS_EXT_IND_USER: str = ""
    REPORTS_EXT_IND_PASSWORD: str = ""
    REPORTS_EXT_IND_DATABASE: str = "nexus_db"
    REPORTS_EXT_IND_PORT: int = 5432
    REPORTS_EXT_IND_SCHEMA: str = "alocreditindicators"

    # Twist 2.0: PostgreSQL CBS (core de credito, dpd) y PDS (datos del cliente).
    # Credenciales se cargan desde el .env (placeholders en .env.example).
    CBS_DB_HOST: str = ""
    CBS_DB_PORT: int = 5434
    CBS_DB_USER: str = ""
    CBS_DB_PASSWORD: str = ""
    CBS_DB_NAME: str = "cbs"
    CBS_DB_CONNECT_TIMEOUT: int = 15

    PDS_DB_HOST: str = ""
    PDS_DB_PORT: int = 5435
    PDS_DB_USER: str = ""
    PDS_DB_PASSWORD: str = ""
    PDS_DB_NAME: str = "PDS"
    PDS_DB_CONNECT_TIMEOUT: int = 15

    # Producto Twist 2.0 (su asignacion vive en tabla propia, no en contract_advisors)
    TWIST2_ENABLED: bool = True

    # Configuración dinámica de asignación (persistida con auditoría)
    DEFAULT_SERLEFIN_PERCENT: float = 60.0
    DEFAULT_COBYSER_PERCENT: float = 40.0
    DEFAULT_ASSIGNMENT_MIN_DAYS: int = 61
    DEFAULT_ASSIGNMENT_MAX_DAYS: int = 240

    # Actor por defecto para auditoría de configuración dinámica (fallback).
    ADMIN_DEFAULT_AUDIT_ACTOR: str = "system"

    # Base interna de configuración/auditoría.
    # Credenciales obligatorias por entorno (ver .env.example). Sin valor por defecto
    # para no exponer secretos en el código fuente.
    INTERNAL_CONFIG_DATABASE_URL: str = ""

    # Secreto HMAC de la API. Obligatorio por entorno (ver .env.example).
    # Sin valor por defecto para no exponer secretos en el código fuente.
    API_HMAC_SECRET: str = ""

    # CORS: orígenes permitidos separados por coma. "*" = cualquiera.
    # Por defecto se preserva el comportamiento actual de producción ("*" + credenciales);
    # para endurecer, definir orígenes explícitos y CORS_ALLOW_CREDENTIALS por entorno.
    CORS_ALLOWED_ORIGINS: str = "*"
    CORS_ALLOW_CREDENTIALS: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

    @property
    def mysql_url(self) -> str:
        """Genera la URL de conexión para MySQL"""
        return f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DATABASE}"

    @property
    def postgres_url(self) -> str:
        """Genera la URL de conexión para PostgreSQL"""
        return f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DATABASE}"

    @property
    def all_users(self) -> List[int]:
        """Retorna todos los usuarios de ambas casas de cobranza"""
        return self.COBYSER_USERS + self.SERLEFIN_USERS

    @property
    def pagare_excluded_status_id_list(self) -> List[int]:
        """IDs de estado de pagaré (endosos a afianzadora) que se excluyen de asignación."""
        if not self.PAGARE_EXCLUDE_ENABLED:
            return []
        out: List[int] = []
        for raw in str(self.PAGARE_EXCLUDED_STATUS_IDS or "").split(","):
            raw = raw.strip()
            if raw.isdigit() and int(raw) not in out:
                out.append(int(raw))
        return out

    @property
    def excluded_contract_status_id_list(self) -> List[int]:
        """Estados de contrato excluidos del proceso (anulado/fraude)."""
        out: List[int] = []
        for raw in str(self.EXCLUDED_CONTRACT_STATUS_IDS or "").split(","):
            raw = raw.strip()
            if raw.isdigit() and int(raw) not in out:
                out.append(int(raw))
        return out

    @property
    def redis_url(self) -> str:
        """URL de conexión a Redis (incluye password si está definido)."""
        auth = f":{self.REDIS_PASSWORD}@" if self.REDIS_PASSWORD else ""
        return f"redis://{auth}{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    @staticmethod
    def _parse_recipients(raw_value: str) -> List[str]:
        """Convierte una cadena CSV de correos en lista única y normalizada."""
        recipients = []
        for raw_recipient in str(raw_value or "").split(","):
            recipient = raw_recipient.strip().lower()
            if recipient and recipient not in recipients:
                recipients.append(recipient)
        return recipients

    @property
    def notification_recipient_list(self) -> List[str]:
        """
        Destinatarios que reciben notificación con ambas bases (Serlefin y Cobyser).
        """
        return self._parse_recipients(self.NOTIFICATION_RECIPIENTS)

    @property
    def cobyser_notification_recipient_list(self) -> List[str]:
        """
        Destinatarios que reciben notificación y base de Cobyser.
        """
        return self._parse_recipients(self.COBYSER_NOTIFICATION_RECIPIENTS)

    @property
    def serlefin_notification_recipient_list(self) -> List[str]:
        """
        Destinatarios que reciben solo notificación de Serlefin (sin adjunto).
        """
        return self._parse_recipients(self.SERLEFIN_NOTIFICATION_RECIPIENTS)

    @property
    def monthly_report_to_list(self) -> List[str]:
        """Destinatarios principales del informe mensual de finalización de ciclo."""
        return self._parse_recipients(self.MONTHLY_REPORT_TO)

    @property
    def monthly_report_cc_list(self) -> List[str]:
        """Destinatarios en copia (CC) del informe mensual."""
        return self._parse_recipients(self.MONTHLY_REPORT_CC)

    @staticmethod
    def _parse_weekdays(raw_value: str, fallback: List[int]) -> List[int]:
        """Convierte CSV de weekdays (0-6) a lista única y ordenada por aparición."""
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
        """Normaliza cédula/documento a solo dígitos."""
        cleaned = re.sub(r"\D+", "", str(raw_value or ""))
        return cleaned.strip()

    def _read_blacklisted_documents_from_file(self) -> List[str]:
        """Lee documentos bloqueados desde TXT (1 por línea o mezclado)."""
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
        """Retorna lista única de documentos bloqueados (CSV + archivo)."""
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
    def auto_assignment_weekday_list(self) -> List[int]:
        """
        Días de ejecución del scheduler en formato weekday de Python.
        """
        return self._parse_weekdays(
            self.AUTO_ASSIGNMENT_WEEKDAYS,
            [0, 1, 2, 3, 4, 5, 6],
        )

    @property
    def auto_notification_weekday_list(self) -> List[int]:
        """
        Días de envío de notificaciones en formato weekday de Python.
        """
        return self._parse_weekdays(
            self.AUTO_NOTIFICATION_WEEKDAYS,
            [1, 3],
        )

    @property
    def serlefin_attachment_exception_recipient_list(self) -> List[str]:
        """
        Destinatarios para excepción de adjunto de Serlefin.
        """
        recipients = []
        for raw_recipient in self.SERLEFIN_ATTACHMENT_EXCEPTION_RECIPIENTS.split(","):
            recipient = raw_recipient.strip().lower()
            if recipient and recipient not in recipients:
                recipients.append(recipient)
        return recipients

    @property
    def cors_allowed_origin_list(self) -> List[str]:
        """
        Lista de orígenes CORS permitidos. "*" se mantiene como comodín.
        """
        raw = str(self.CORS_ALLOWED_ORIGINS or "*").strip()
        if raw == "*" or not raw:
            return ["*"]
        origins = []
        for raw_origin in raw.split(","):
            origin = raw_origin.strip()
            if origin and origin not in origins:
                origins.append(origin)
        return origins or ["*"]

    @property
    def cors_credentials_enabled(self) -> bool:
        """
        Controla el envío de credenciales en CORS. Configurable por entorno.
        Recomendación: usar False con orígenes "*" (no conforme a la especificación
        CORS combinar ambos); definir orígenes explícitos para producción.
        """
        return bool(self.CORS_ALLOW_CREDENTIALS)


# Instancia global de configuración (Singleton)
settings = Settings()
