"""
Modelos de datos SQLAlchemy para las tablas de la base de datos.
Define la estructura de las tablas y las relaciones.
"""
from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class ContractAdvisor(Base):
    """
    Modelo para la tabla alocreditindicators.contract_advisors en PostgreSQL.
    Almacena las asignaciones de contratos a asesores.
    """
    __tablename__ = "contract_advisors"
    __table_args__ = {"schema": "alocreditindicators"}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    contract_id = Column(Integer, nullable=False, index=True)
    advisor_id = Column(Integer, nullable=False, index=True)
    assigned_date = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<ContractAdvisor(contract_id={self.contract_id}, advisor_id={self.advisor_id})>"


class Management(Base):
    """
    Modelo para la tabla alocreditindicators.managements en PostgreSQL.
    Contiene información de gestiones y efectos (como 'pago_total').
    """
    __tablename__ = "managements"
    __table_args__ = {"schema": "alocreditindicators"}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    contract_id = Column(Integer, nullable=False, index=True)
    advisor_id = Column(Integer, nullable=True)
    user_id = Column(Integer, nullable=True, index=True)  # Alias de advisor_id
    effect = Column(String(100), nullable=True, index=True)
    management_date = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=True)
    
    def __repr__(self):
        return f"<Management(contract_id={self.contract_id}, effect={self.effect})>"


class ContractAdvisorHistory(Base):
    """
    Modelo para la tabla alocreditindicators.contract_advisors_history en PostgreSQL.
    Almacena el historial completo de asignaciones con fechas inicial y terminal.
    
    Campos:
    - Fecha Inicial: Fecha en que se asignó el contrato
    - Fecha Terminal: Fecha en que se removió el contrato (null si aún activo)
    """
    __tablename__ = "contract_advisors_history"
    __table_args__ = {"schema": "alocreditindicators"}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, index=True)
    contract_id = Column(Integer, nullable=False, index=True)
    fecha_inicial = Column("Fecha Inicial", DateTime, nullable=False)
    fecha_terminal = Column("Fecha Terminal", DateTime, nullable=True)
    
    def __repr__(self):
        return f"<ContractAdvisorHistory(contract_id={self.contract_id}, user_id={self.user_id})>"


# Nota: Para MySQL (alocreditprod) usaremos queries raw SQL ya que
# solo necesitamos hacer SELECT y no modificaciones en esa base de datos.
# Las tablas contract_amortization y contract_status no necesitan modelos ORM.
