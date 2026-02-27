"""
ConfiguraciÃ³n centralizada de la aplicaciÃ³n.
Gestiona las credenciales de bases de datos y parÃ¡metros del sistema.
"""
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
    MAX_DAYS_THRESHOLD: int = 209  # DÃ­as de atraso mÃ¡ximos (casas de cobranza)
    
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

    # Scheduler automatico de asignacion (todos los dias 3:00 AM)
    AUTO_ASSIGNMENT_ENABLED: bool = True
    AUTO_ASSIGNMENT_HOUR: int = 3
    AUTO_ASSIGNMENT_MINUTE: int = 0
    AUTO_ASSIGNMENT_TIMEZONE: str = "America/Bogota"
    # Python weekday: lunes=0 ... domingo=6
    AUTO_ASSIGNMENT_WEEKDAYS: str = "0,1,2,3,4,5,6"

    # Correos de notificacion (separados por coma)
    NOTIFICATION_RECIPIENTS: str = "emduelofeuth@alocredit.co"

    # SMTP para envio de correos
    SMTP_SERVER: str = "smtp-relay.gmail.com"
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
    DEFAULT_ASSIGNMENT_MAX_DAYS: int = 209

    # Base interna de configuracion/auditoria
    INTERNAL_CONFIG_DATABASE_URL: str = "postgresql+psycopg2://internal_config_user:internal_config_pass@localhost:5559/internal_config_db"

    # Panel visual protegido por hash (acceso por URL secreta)
    ADMIN_PANEL_HASH: str = "3e63c8d8d94f44288b5f90d2c16fd101"
    ADMIN_DEFAULT_AUDIT_ACTOR: str = "mdeulofeuth@alocredit.co"
    
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

    @property
    def notification_recipients(self) -> List[str]:
        """Lista de correos para envio de notificaciones."""
        recipients = []
        for raw_recipient in self.NOTIFICATION_RECIPIENTS.split(","):
            recipient = raw_recipient.strip()
            if recipient and recipient not in recipients:
                recipients.append(recipient)
        return recipients

    @property
    def auto_assignment_weekdays(self) -> List[int]:
        """
        Dias de ejecucion del scheduler en formato weekday de Python.
        """
        weekdays = []
        for raw_day in self.AUTO_ASSIGNMENT_WEEKDAYS.split(","):
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
        return [0, 1, 2, 3, 4]

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

