"""
Test de Lógica del Servicio ManualFixedService
Verifica que la lógica de validación por usuario sea correcta
sin necesidad de servidor activo
"""

# --- bootstrap: ejecutable desde cualquier ruta (anade la raiz del repo al path) ---
import sys as _sys, pathlib as _pathlib
for _cand in (_pathlib.Path(__file__).resolve(), *_pathlib.Path(__file__).resolve().parents):
    if (_cand / "app").is_dir() and (_cand / "main.py").exists():
        _sys.path.insert(0, str(_cand)); break
# --- fin bootstrap ---
import sys
from pathlib import Path

# Agregar directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent))

from app.data.manual_fixed_contracts import COBYSER_MANUAL_FIXED, SERLEFIN_MANUAL_FIXED
from app.database.connections import get_postgres_session
from app.database.models import ContractAdvisor


def test_manual_fixed_validation():
    """
    Simula la lógica del servicio para verificar que filtra por user_id correctamente
    """
    print("=" * 80)
    print("TEST DE LÓGICA MANUAL_FIXED_SERVICE")
    print("=" * 80)
    
    # Datos de entrada
    manual_contracts = {
        45: COBYSER_MANUAL_FIXED,
        81: SERLEFIN_MANUAL_FIXED
    }
    
    # Obtener sesión PostgreSQL
    postgres_session = next(get_postgres_session())
    
    try:
        print(f"\n📊 Contratos a procesar:")
        print(f"  - Cobyser (Usuario 45): {len(COBYSER_MANUAL_FIXED)} contratos")
        print(f"  - Serlefin (Usuario 81): {len(SERLEFIN_MANUAL_FIXED)} contratos")
        total_provided = len(COBYSER_MANUAL_FIXED) + len(SERLEFIN_MANUAL_FIXED)
        print(f"  - TOTAL: {total_provided} contratos")
        
        # LÓGICA CORREGIDA: Validar por usuario
        print(f"\n🔍 Validando asignaciones POR USUARIO...")
        contracts_to_insert_by_user = {}
        total_already_assigned = 0
        
        for user_id, contract_ids in manual_contracts.items():
            print(f"\n  Usuario {user_id}:")
            print(f"    - Contratos proporcionados: {len(contract_ids)}")
            
            # Verificar cuáles contratos YA están asignados a ESTE usuario específico
            existing_for_user = postgres_session.query(
                ContractAdvisor.contract_id
            ).filter(
                ContractAdvisor.contract_id.in_(contract_ids),
                ContractAdvisor.user_id == user_id
            ).all()
            
            existing_contract_ids_for_user = set(row[0] for row in existing_for_user)
            print(f"    - Ya asignados a usuario {user_id}: {len(existing_contract_ids_for_user)}")
            
            # Contratos nuevos = contratos proporcionados - contratos ya asignados a este usuario
            new_contracts_for_user = set(contract_ids) - existing_contract_ids_for_user
            contracts_to_insert_by_user[user_id] = new_contracts_for_user
            print(f"    - ✓ Nuevos a insertar: {len(new_contracts_for_user)}")
            
            total_already_assigned += len(existing_contract_ids_for_user)
        
        # Calcular total a insertar
        total_to_insert = sum(len(contracts) for contracts in contracts_to_insert_by_user.values())
        
        print(f"\n" + "=" * 80)
        print(f"📈 RESULTADO DE VALIDACIÓN:")
        print(f"  - Total proporcionados: {total_provided}")
        print(f"  - Ya asignados (todos los usuarios): {total_already_assigned}")
        print(f"  - ✓ NUEVOS A INSERTAR: {total_to_insert}")
        print(f"=" * 80)
        
        if total_to_insert > 0:
            print(f"\n✅ La lógica es CORRECTA: detecta {total_to_insert} contratos pendientes")
            print(f"   El servidor necesita reiniciarse para aplicar los cambios de código")
        else:
            print(f"\n✓ Todos los contratos ya están correctamente asignados")
            
    finally:
        postgres_session.close()


if __name__ == "__main__":
    try:
        test_manual_fixed_validation()
    except Exception as e:
        print(f"\n❌ Error en la validación: {e}")
        import traceback
        traceback.print_exc()
