"""
Script de prueba para verificar la nueva lógica de bases fijas.
Muestra los contratos que serían considerados fijos según los nuevos filtros.
"""
import os
import sys
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

# Configuración de conexión PostgreSQL
required_env = {
    "POSTGRES_USER": os.getenv("POSTGRES_USER"),
    "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
    "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
    "POSTGRES_DATABASE": os.getenv("POSTGRES_DATABASE"),
}
missing_env = [key for key, value in required_env.items() if not value]
if missing_env:
    raise RuntimeError(
        "Faltan variables de entorno requeridas: " + ", ".join(missing_env)
    )

POSTGRES_URL = (
    "postgresql+psycopg2://"
    f"{required_env['POSTGRES_USER']}:"
    f"{required_env['POSTGRES_PASSWORD']}@"
    f"{required_env['POSTGRES_HOST']}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{required_env['POSTGRES_DATABASE']}"
)

def test_fixed_contracts_logic():
    """Prueba la lógica de contratos fijos sin modificar la base de datos"""
    
    engine = create_engine(POSTGRES_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    print("=" * 100)
    print("PRUEBA DE LÓGICA DE BASES FIJAS")
    print("=" * 100)
    print()
    
    today = datetime.now().date()
    validity_date = datetime.now() - timedelta(days=30)
    
    print(f"ðŸ“… Fecha actual: {today}")
    print(f"ðŸ“… Fecha límite pago_total: {validity_date.date()}")
    print()
    
    # Consulta para obtener registros con effect relevantes
    query = text("""
        SELECT 
            id,
            user_id,
            contract_id,
            effect,
            management_date,
            promise_date,
            CASE 
                WHEN effect = 'acuerdo_de_pago' AND promise_date >= CURRENT_DATE THEN 'VÁLIDO'
                WHEN effect = 'acuerdo_de_pago' AND promise_date < CURRENT_DATE THEN 'EXPIRADO'
                WHEN effect = 'pago_total' AND management_date >= (CURRENT_TIMESTAMP - INTERVAL '30 days') THEN 'VÁLIDO'
                WHEN effect = 'pago_total' AND management_date < (CURRENT_TIMESTAMP - INTERVAL '30 days') THEN 'EXPIRADO'
                ELSE 'N/A'
            END as status
        FROM alocreditindicators.managements
        WHERE effect IN ('acuerdo_de_pago', 'pago_total')
        ORDER BY effect, status, contract_id
    """)
    
    result = session.execute(query)
    rows = result.fetchall()
    
    # Estadísticas
    stats = {
        'acuerdo_pago_valid': 0,
        'acuerdo_pago_expired': 0,
        'pago_total_valid': 0,
        'pago_total_expired': 0
    }
    
    fixed_contracts_45 = set()
    fixed_contracts_81 = set()
    
    COBYSER_USERS = [45, 46, 47, 48, 49, 50, 51]
    SERLEFIN_USERS = [81, 82, 83, 84, 85, 86, 102, 103]
    
    print("📊 ANÁLISIS DE REGISTROS:")
    print("-" * 100)
    
    for row in rows:
        record_id = row[0]
        user_id = row[1]
        contract_id = row[2]
        effect = row[3]
        management_date = row[4]
        promise_date = row[5]
        status = row[6]
        
        # Contar estadísticas
        if effect == 'acuerdo_de_pago':
            if status == 'VÁLIDO':
                stats['acuerdo_pago_valid'] += 1
                is_fixed = True
            else:
                stats['acuerdo_pago_expired'] += 1
                is_fixed = False
        elif effect == 'pago_total':
            if status == 'VÁLIDO':
                stats['pago_total_valid'] += 1
                is_fixed = True
            else:
                stats['pago_total_expired'] += 1
                is_fixed = False
        else:
            is_fixed = False
        
        # Asignar a casa de cobranza
        if is_fixed:
            if user_id in COBYSER_USERS:
                fixed_contracts_45.add(contract_id)
            elif user_id in SERLEFIN_USERS:
                fixed_contracts_81.add(contract_id)
        
        # Mostrar primeros 20 registros como ejemplo
        if len([r for r in rows if rows.index(r) < 20]):
            print(f"ID: {record_id:6} | User: {user_id:3} | Contrato: {contract_id:8} | "
                  f"Effect: {effect:20} | Status: {status:10} | "
                  f"Mgmt Date: {str(management_date)[:10] if management_date else 'N/A':10} | "
                  f"Promise: {str(promise_date)[:10] if promise_date else 'N/A':10}")
    
    print()
    print("=" * 100)
    print("📈 RESUMEN DE RESULTADOS:")
    print("=" * 100)
    print()
    
    print("ðŸ”µ ACUERDO DE PAGO:")
    print(f"   ✅ Válidos (promise_date >= hoy):     {stats['acuerdo_pago_valid']:4}")
    print(f"   ❌ Expirados (promise_date < hoy):    {stats['acuerdo_pago_expired']:4}")
    print()
    
    print("ðŸŸ¢ PAGO TOTAL:")
    print(f"   ✅ Válidos (â‰¤ 30 días):               {stats['pago_total_valid']:4}")
    print(f"   ❌ Expirados (> 30 días):             {stats['pago_total_expired']:4}")
    print()
    
    print("ðŸ¢ CONTRATOS FIJOS POR CASA DE COBRANZA:")
    print(f"   ðŸ“Œ COBYSER (Usuario 45):              {len(fixed_contracts_45):4} contratos")
    print(f"   ðŸ“Œ SERLEFIN (Usuario 81):             {len(fixed_contracts_81):4} contratos")
    print(f"   ðŸ“Œ TOTAL:                             {len(fixed_contracts_45) + len(fixed_contracts_81):4} contratos")
    print()
    
    print("=" * 100)
    
    # Mostrar algunos contratos de ejemplo
    if fixed_contracts_45:
        print(f"\n🔍 Ejemplos de contratos fijos COBYSER (primeros 10):")
        for contract_id in list(fixed_contracts_45)[:10]:
            print(f"   - Contrato: {contract_id}")
    
    if fixed_contracts_81:
        print(f"\n🔍 Ejemplos de contratos fijos SERLEFIN (primeros 10):")
        for contract_id in list(fixed_contracts_81)[:10]:
            print(f"   - Contrato: {contract_id}")
    
    session.close()
    print("\n✅ Prueba completada exitosamente")

if __name__ == "__main__":
    try:
        test_fixed_contracts_logic()
    except Exception as e:
        print(f"\n❌ Error durante la prueba: {e}", file=sys.stderr)
        sys.exit(1)
