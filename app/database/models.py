"""
Modelos de datos SQLAlchemy para las tablas de la base de datos.
Define la estructura de las tablas y las relaciones.
"""
from sqlalchemy import Column, Integer, BigInteger, String, Date, DateTime, Numeric, Text
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
    estado_actual = Column(String(100), nullable=True, index=True)
    # Producto del contrato: PHONE (default historico) | TWIST1 | TWIST2.
    # Permite que un mismo contract_id conviva entre productos sin chocar.
    producto = Column(String(20), nullable=True, index=True)

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
    contract_id = Column(BigInteger, nullable=False, index=True)
    fecha_inicial = Column("Fecha Inicial", DateTime, nullable=False)
    fecha_terminal = Column("Fecha Terminal", DateTime, nullable=True)
    tipo = Column(String(50), nullable=True, index=True)
    dpd_inicial = Column(String(20), nullable=True)
    # La BD historica usa nombres distintos en estas columnas.
    dpd_terminal = Column("dpd_final", String(20), nullable=True)
    dias_atraso_inicial = Column("dias_atraso_incial", Integer, nullable=True)
    dias_atraso_terminal = Column(Integer, nullable=True)
    estado_actual = Column("estado_actual", String(100), nullable=True)
    dpd_actual = Column("dpd_actual", String(20), nullable=True)
    producto = Column(String(20), nullable=True)

    def __repr__(self):
        return f"<ContractAdvisorHistory(contract_id={self.contract_id}, user_id={self.user_id})>"


class ContractAdvisorTwist(Base):
    """
    Tabla existente alocreditindicators.contract_advisors_twist (producto Twist 1.0).
    Estructura minima preexistente: id, user_id, contract_id (bigint).
    """
    __tablename__ = "contract_advisors_twist"
    __table_args__ = {"schema": "alocreditindicators"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False, index=True)
    contract_id = Column(BigInteger, nullable=False, index=True)

    def __repr__(self):
        return f"<ContractAdvisorTwist(contract_id={self.contract_id}, user_id={self.user_id})>"


class Twist2Advisor(Base):
    """
    Asignaciones del producto Twist 2.0 (fuente PostgreSQL PDS/CBS).

    Tabla PROPIA (separada de contract_advisors) porque el identificador de la
    linea Twist 2.0 es un UUID (PDS) / bigint (CBS) y no cabe en el contract_id
    entero de contract_advisors. Reglas iguales a la imagen: 31-60 solo Cobyser
    cedula impar (Serlefin 0%), 61-240 reparto 40/60 Cobyser/Serlefin.
    """
    __tablename__ = "contract_advisors_twist2"
    __table_args__ = {"schema": "alocreditindicators"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    line_id = Column(String(64), nullable=False, index=True)   # PDS credit_lines.id (uuid) == CBS ext_id
    cbs_id = Column(BigInteger, nullable=True)                 # CBS credit_line.id
    user_id = Column(Integer, nullable=False, index=True)
    cedula = Column(String(50), nullable=True)
    days_overdue = Column(Integer, nullable=True)
    dpd = Column(String(20), nullable=True)
    tipo = Column(String(50), nullable=True)
    producto = Column(String(20), nullable=False, default="TWIST2")
    estado_actual = Column(String(100), nullable=True)
    fecha_inicial = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<Twist2Advisor(line_id={self.line_id}, user_id={self.user_id})>"


# Nota: Para MySQL (alocreditprod) usaremos queries raw SQL ya que
# solo necesitamos hacer SELECT y no modificaciones en esa base de datos.
# Las tablas contract_amortization y contract_status no necesitan modelos ORM.
