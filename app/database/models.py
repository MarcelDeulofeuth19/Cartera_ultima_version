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
    user_id = Column(Integer, nullable=False, index=True)
    
    def __repr__(self):
        return f"<ContractAdvisor(contract_id={self.contract_id}, user_id={self.user_id})>"


class Management(Base):
    """
    Modelo para la tabla alocreditindicators.managements en PostgreSQL.
    Contiene información de gestiones y efectos (como 'pago_total' y 'acuerdo_de_pago').
    """
    __tablename__ = "managements"
    __table_args__ = {"schema": "alocreditindicators"}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    contract_id = Column(Integer, nullable=False, index=True)
    user_id = Column(Integer, nullable=True, index=True)
    name = Column(String(255), nullable=True)
    last_name = Column(String(255), nullable=True)
    dni = Column(String(50), nullable=True)
    management_date = Column(DateTime, nullable=True, index=True)
    action = Column(String(100), nullable=True)
    result = Column(String(100), nullable=True)
    effect = Column(String(100), nullable=True, index=True)
    promise_date = Column(Date, nullable=True, index=True)
    contact_phone = Column(String(50), nullable=True)
    summary = Column(Text, nullable=True)
    is_bulk_import = Column(Integer, nullable=True)
    product_id = Column(Integer, nullable=True)
    
    def __repr__(self):
        return f"<Management(id={self.id}, contract_id={self.contract_id}, effect={self.effect})>"


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
