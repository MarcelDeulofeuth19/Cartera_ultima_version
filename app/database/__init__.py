"""Database module - Conexiones y modelos (lazy imports)."""

from importlib import import_module

__all__ = [
    "DatabaseManager",
    "db_manager",
    "get_mysql_session",
    "get_postgres_session",
    "ContractAdvisor",
    "Management",
    "ContractAdvisorHistory",
    "Base",
]


def __getattr__(name: str):
    if name in {"DatabaseManager", "db_manager", "get_mysql_session", "get_postgres_session"}:
        module = import_module("app.database.connections")
        return getattr(module, name)
    if name in {"ContractAdvisor", "Management", "ContractAdvisorHistory", "Base"}:
        module = import_module("app.database.models")
        return getattr(module, name)
    raise AttributeError(f"module 'app.database' has no attribute '{name}'")
