"""
Configuración centralizada de la aplicación.
Gestiona las credenciales de bases de datos y parámetros del sistema.
"""
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
    
    # Configuración de negocio
    # Casas de Cobranza:
    # - COBYSER: usuarios 45, 46, 47, 48, 49, 50, 51
    # - SERLEFIN: usuarios 81, 82, 83, 84, 85, 86, 102, 103
    COBYSER_USERS: List[int] = [45, 46, 47, 48, 49, 50, 51]
    SERLEFIN_USERS: List[int] = [81, 82, 83, 84, 85, 86, 102, 103]
    
    # Para retrocompatibilidad (usuarios principales de cada casa)
    USER_IDS: List[int] = [45, 81]
    
    DAYS_THRESHOLD: int = 61  # Días de atraso mínimos
    FIXED_CONTRACT_EFFECT: str = "pago_total"
    
    # Configuración de reportes
    REPORTS_DIR: str = "reports"
    REPORT_FILE_USER_45: str = "asignacion_45.txt"
    REPORT_FILE_USER_81: str = "asignacion_81.txt"
    REPORT_EXCEL_FIXED: str = "reporte_fijos_efect.xlsx"
    
    # File Lock para singleton
    LOCK_FILE: str = "assignment_process.lock"
    LOCK_TIMEOUT: int = 300  # 5 minutos de timeout
    
    class Config:
        env_file = ".env"
        case_sensitive = True
    
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


# Instancia global de configuración (Singleton)
settings = Settings()
