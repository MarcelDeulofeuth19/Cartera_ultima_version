"""
Verificar si hay contratos duplicados entre las listas de Cobyser y Serlefin
"""
from app.data.manual_fixed_contracts import COBYSER_MANUAL_FIXED, SERLEFIN_MANUAL_FIXED

print("=" * 80)
print("VERIFICACIÓN DE CONTRATOS DUPLICADOS ENTRE LISTAS")
print("=" * 80)

cobyser_set = set(COBYSER_MANUAL_FIXED)
serlefin_set = set(SERLEFIN_MANUAL_FIXED)

# Encontrar duplicados
duplicates = cobyser_set.intersection(serlefin_set)

print(f"\n📊 ESTADÍSTICAS:")
print(f"  - Contratos Cobyser (Usuario 45): {len(cobyser_set)}")
print(f"  - Contratos Serlefin (Usuario 81): {len(serlefin_set)}")
print(f"  - Total contratos: {len(cobyser_set) + len(serlefin_set)}")
print(f"  - Contratos ÚNICOS: {len(cobyser_set.union(serlefin_set))}")

if duplicates:
    print(f"\n❌ DUPLICADOS ENCONTRADOS: {len(duplicates)} contratos")
    print(f"\n  Contratos que aparecen en AMBAS listas:")
    for contract_id in sorted(duplicates):
        print(f"    - {contract_id}")
    
    print(f"\n⚠️  PROBLEMA: La tabla 'contract_advisors' tiene un constraint UNIQUE")
    print(f"    en 'contract_id', lo que significa que un contrato solo puede")
    print(f"    asignarse a UN usuario. Si hay contratos en ambas listas,")
    print(f"    solo uno podrá insertarse (el primero que se procese).")
else:
    print(f"\n✅ No hay duplicados - cada contrato aparece solo en una lista")

# Verificar si 41985 está en alguna lista
print(f"\n\n🔍 ANÁLISIS DEL CONTRATO 41985:")
if 41985 in cobyser_set:
    print(f"  ✓ Está en Cobyser (Usuario 45)")
if 41985 in serlefin_set:
    print(f"  ✓ Está en Serlefin (Usuario 81)")
if 41985 not in cobyser_set and 41985 not in serlefin_set:
    print(f"  ✗ NO está en ninguna de las dos listas")
