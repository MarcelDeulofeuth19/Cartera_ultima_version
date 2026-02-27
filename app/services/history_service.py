"""
Servicio de gestion de historial de asignaciones.
Maneja INSERT y UPDATE en contract_advisors_history.
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.dpd import get_dpd_range
from app.database.models import ContractAdvisorHistory

logger = logging.getLogger(__name__)


class HistoryService:
    """
    Servicio para gestionar historial de asignaciones.

    Responsabilidades:
    - INSERT: registrar fecha inicial y datos DPD al asignar.
    - UPDATE: registrar fecha terminal, motivo y DPD al remover.
    """

    def __init__(self, postgres_session: Session):
        self.postgres_session = postgres_session

    @staticmethod
    def _to_int_or_none(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _resolve_initial_fields(
        self,
        contract_id: int,
        assignment_metadata: Optional[Dict[int, Dict[str, Any]]],
        default_tipo: str,
    ) -> Dict[str, Any]:
        metadata = (assignment_metadata or {}).get(contract_id, {})

        dias_atraso_inicial = self._to_int_or_none(
            metadata.get("dias_atraso_inicial", metadata.get("days_overdue"))
        )
        dpd_inicial = metadata.get("dpd_inicial") or get_dpd_range(dias_atraso_inicial)
        tipo = metadata.get("tipo") or default_tipo

        return {
            "tipo": tipo,
            "dpd_inicial": dpd_inicial,
            "dias_atraso_inicial": dias_atraso_inicial,
        }

    def _resolve_terminal_fields(
        self,
        contract_id: int,
        terminal_metadata: Optional[Dict[int, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        metadata = (terminal_metadata or {}).get(contract_id, {})

        dias_atraso_terminal = self._to_int_or_none(metadata.get("dias_atraso_terminal"))
        dpd_terminal = metadata.get("dpd_terminal") or get_dpd_range(dias_atraso_terminal)
        tipo = metadata.get("tipo") or "REMOVIDO"

        dias_atraso_inicial = self._to_int_or_none(metadata.get("dias_atraso_inicial"))
        dpd_inicial = metadata.get("dpd_inicial") or get_dpd_range(dias_atraso_inicial)

        return {
            "tipo": tipo,
            "dpd_terminal": dpd_terminal,
            "dias_atraso_terminal": dias_atraso_terminal,
            "dpd_inicial": dpd_inicial,
            "dias_atraso_inicial": dias_atraso_inicial,
        }

    def register_assignments(
        self,
        assignments: Dict[int, List[int]],
        assignment_metadata: Optional[Dict[int, Dict[str, Any]]] = None,
        default_tipo: str = "ASIGNACION",
    ) -> Dict[str, int]:
        """
        Registra nuevas asignaciones en historial con fecha inicial.

        Args:
            assignments: Diccionario {user_id: [contract_ids]}
            assignment_metadata: Metadatos por contrato para campos extra de historial
            default_tipo: Tipo por defecto para registros nuevos

        Returns:
            {'total_registered': X, 'cobyser': Y, 'serlefin': Z}
        """
        logger.info("Registrando nuevas asignaciones en historial...")

        stats = {"total_registered": 0, "cobyser": 0, "serlefin": 0}
        fecha_actual = datetime.now()

        try:
            all_pairs: List[Tuple[int, int]] = []
            for user_id, contract_ids in assignments.items():
                for contract_id in contract_ids:
                    all_pairs.append((int(contract_id), int(user_id)))

            if not all_pairs:
                logger.info("No hay asignaciones para registrar")
                return stats

            all_contract_ids = [pair[0] for pair in all_pairs]

            existing_active = self.postgres_session.query(
                ContractAdvisorHistory.contract_id,
                ContractAdvisorHistory.user_id,
            ).filter(
                and_(
                    ContractAdvisorHistory.contract_id.in_(all_contract_ids),
                    ContractAdvisorHistory.fecha_terminal.is_(None),
                )
            ).all()

            existing_pairs = set((int(row[0]), int(row[1])) for row in existing_active)

            new_history_records = []
            for contract_id, user_id in all_pairs:
                if (contract_id, user_id) in existing_pairs:
                    continue

                initial_fields = self._resolve_initial_fields(
                    contract_id,
                    assignment_metadata,
                    default_tipo,
                )

                new_history_records.append(
                    {
                        "user_id": user_id,
                        "contract_id": contract_id,
                        "fecha_inicial": fecha_actual,
                        "fecha_terminal": None,
                        "tipo": initial_fields["tipo"],
                        "dpd_inicial": initial_fields["dpd_inicial"],
                        "dpd_terminal": None,
                        "dias_atraso_inicial": initial_fields["dias_atraso_inicial"],
                        "dias_atraso_terminal": None,
                    }
                )

                stats["total_registered"] += 1
                if user_id in settings.COBYSER_USERS:
                    stats["cobyser"] += 1
                elif user_id in settings.SERLEFIN_USERS:
                    stats["serlefin"] += 1

            if new_history_records:
                logger.info(
                    f"Insertando {len(new_history_records)} nuevos registros en historial..."
                )
                self.postgres_session.bulk_insert_mappings(
                    ContractAdvisorHistory,
                    new_history_records,
                )

            self.postgres_session.commit()

            logger.info(
                "Historial registrado: "
                f"total={stats['total_registered']}, "
                f"cobyser={stats['cobyser']}, "
                f"serlefin={stats['serlefin']}"
            )

            return stats

        except Exception as e:
            logger.error(f"Error al registrar historial: {e}")
            self.postgres_session.rollback()
            raise

    def close_assignments(
        self,
        contracts_removed: Dict[int, List[int]],
        terminal_metadata: Optional[Dict[int, Dict[str, Any]]] = None,
    ) -> Dict[str, int]:
        """
        Cierra asignaciones activas en historial actualizando fecha terminal.

        Args:
            contracts_removed: {user_id: [contract_ids]} eliminados de contract_advisors
            terminal_metadata: metadatos por contrato para tipo y DPD terminal

        Returns:
            {'total_closed': X, 'updated': Y, 'inserted': Z, 'cobyser': A, 'serlefin': B}
        """
        logger.info("Cerrando asignaciones en historial...")

        stats = {
            "total_closed": 0,
            "updated": 0,
            "inserted": 0,
            "cobyser": 0,
            "serlefin": 0,
        }
        fecha_actual = datetime.now()

        all_pairs: List[Tuple[int, int]] = []
        for user_id, contract_ids in contracts_removed.items():
            for contract_id in contract_ids:
                all_pairs.append((int(contract_id), int(user_id)))

        if not all_pairs:
            logger.info("No hay contratos para cerrar en historial")
            return stats

        try:
            contract_ids = [pair[0] for pair in all_pairs]
            user_ids = list({pair[1] for pair in all_pairs})

            active_records = self.postgres_session.query(ContractAdvisorHistory).filter(
                and_(
                    ContractAdvisorHistory.contract_id.in_(contract_ids),
                    ContractAdvisorHistory.user_id.in_(user_ids),
                    ContractAdvisorHistory.fecha_terminal.is_(None),
                )
            ).all()

            active_map: Dict[Tuple[int, int], ContractAdvisorHistory] = {
                (int(record.contract_id), int(record.user_id)): record
                for record in active_records
            }

            for contract_id, user_id in all_pairs:
                terminal_fields = self._resolve_terminal_fields(
                    contract_id,
                    terminal_metadata,
                )

                record = active_map.get((contract_id, user_id))

                if record:
                    record.fecha_terminal = fecha_actual
                    record.tipo = terminal_fields["tipo"]
                    record.dpd_terminal = terminal_fields["dpd_terminal"]
                    record.dias_atraso_terminal = terminal_fields[
                        "dias_atraso_terminal"
                    ]

                    if record.dpd_inicial is None and terminal_fields["dpd_inicial"]:
                        record.dpd_inicial = terminal_fields["dpd_inicial"]
                    if (
                        record.dias_atraso_inicial is None
                        and terminal_fields["dias_atraso_inicial"] is not None
                    ):
                        record.dias_atraso_inicial = terminal_fields[
                            "dias_atraso_inicial"
                        ]

                    stats["updated"] += 1
                else:
                    # Fallback para contratos antiguos sin historial abierto.
                    dias_inicial = terminal_fields["dias_atraso_inicial"]
                    if dias_inicial is None:
                        dias_inicial = terminal_fields["dias_atraso_terminal"]

                    dpd_inicial = terminal_fields["dpd_inicial"]
                    if dpd_inicial is None:
                        dpd_inicial = get_dpd_range(dias_inicial)

                    new_history = ContractAdvisorHistory(
                        user_id=user_id,
                        contract_id=contract_id,
                        fecha_inicial=fecha_actual,
                        fecha_terminal=fecha_actual,
                        tipo=terminal_fields["tipo"],
                        dpd_inicial=dpd_inicial,
                        dpd_terminal=terminal_fields["dpd_terminal"],
                        dias_atraso_inicial=dias_inicial,
                        dias_atraso_terminal=terminal_fields["dias_atraso_terminal"],
                    )
                    self.postgres_session.add(new_history)
                    stats["inserted"] += 1

                stats["total_closed"] += 1
                if user_id in settings.COBYSER_USERS:
                    stats["cobyser"] += 1
                elif user_id in settings.SERLEFIN_USERS:
                    stats["serlefin"] += 1

            self.postgres_session.commit()

            logger.info(
                "Asignaciones cerradas en historial: "
                f"total={stats['total_closed']}, "
                f"updated={stats['updated']}, inserted={stats['inserted']}"
            )
            return stats

        except Exception as e:
            logger.error(f"Error al cerrar asignaciones en historial: {e}")
            self.postgres_session.rollback()
            raise

    def get_active_assignments(self, user_ids: List[int] = None) -> Dict[int, Set[int]]:
        """Obtiene asignaciones activas del historial."""
        logger.info("Consultando asignaciones activas del historial...")

        try:
            query = self.postgres_session.query(ContractAdvisorHistory).filter(
                ContractAdvisorHistory.fecha_terminal.is_(None)
            )

            if user_ids:
                query = query.filter(ContractAdvisorHistory.user_id.in_(user_ids))

            records = query.all()

            active_assignments: Dict[int, Set[int]] = {}
            for record in records:
                if record.user_id not in active_assignments:
                    active_assignments[record.user_id] = set()
                active_assignments[record.user_id].add(record.contract_id)

            total_active = sum(
                len(contracts) for contracts in active_assignments.values()
            )
            logger.info(f"Asignaciones activas encontradas: {total_active}")

            return active_assignments

        except Exception as e:
            logger.error(f"Error al consultar historial activo: {e}")
            raise

    def get_history_stats(self) -> Dict:
        """Obtiene estadisticas generales del historial."""
        try:
            total_records = self.postgres_session.query(ContractAdvisorHistory).count()

            active_records = self.postgres_session.query(ContractAdvisorHistory).filter(
                ContractAdvisorHistory.fecha_terminal.is_(None)
            ).count()

            closed_records = self.postgres_session.query(ContractAdvisorHistory).filter(
                ContractAdvisorHistory.fecha_terminal.isnot(None)
            ).count()

            stats = {
                "total_records": total_records,
                "active_assignments": active_records,
                "closed_assignments": closed_records,
            }

            logger.info(f"Estadisticas del historial: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Error al obtener estadisticas del historial: {e}")
            raise
