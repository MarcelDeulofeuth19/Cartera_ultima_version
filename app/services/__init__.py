"""Services module - Servicios de negocio (lazy imports)."""

from importlib import import_module

__all__ = ["ContractService", "AssignmentService", "ReportService", "HistoryService"]


def __getattr__(name: str):
    mapping = {
        "ContractService": ("app.services.contract_service", "ContractService"),
        "AssignmentService": ("app.services.assignment_service", "AssignmentService"),
        "ReportService": ("app.services.report_service", "ReportService"),
        "HistoryService": ("app.services.history_service", "HistoryService"),
    }
    if name not in mapping:
        raise AttributeError(f"module 'app.services' has no attribute '{name}'")
    module_name, attr_name = mapping[name]
    module = import_module(module_name)
    return getattr(module, attr_name)
