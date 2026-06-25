"""
Test para verificar el balance equitativo de contratos entre asesores
"""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

print("=" * 80)
print("SIMULACIÓN DE BALANCE EQUITATIVO")
print("=" * 80)
print()

# Simular datos de ejemplo
usuarios = [4, 7, 36, 58, 60, 62, 71, 77, 89, 90, 91, 113, 114, 116]

# Simular contratos actuales (desiguales)
contratos_actuales = {
    4: 1000,
    7: 980,
    36: 1020,
    58: 990,
    60: 1010,
    62: 1000,
    71: 995,
    77: 1005,
    89: 985,
    90: 1015,
    91: 1025,
    113: 975,
    114: 1000,
    116: 990
}

# Simular 140 contratos nuevos a distribuir
contratos_nuevos = 140

print("📊 ESTADO INICIAL:")
print("-" * 80)
for user_id in usuarios:
    print(f"   Usuario {user_id:3d}: {contratos_actuales[user_id]:4d} contratos")
print(f"\n   Total actual: {sum(contratos_actuales.values())}")
print(f"   Contratos nuevos a distribuir: {contratos_nuevos}")
print()

# ALGORITMO DE BALANCE EQUITATIVO
# Asignar cada contrato al usuario que tiene MENOS contratos
current_counts = contratos_actuales.copy()

for i in range(contratos_nuevos):
    # Encontrar usuario con menor cantidad
    min_user = min(current_counts.keys(), key=lambda u: current_counts[u])
    
    # Asignar contrato
    current_counts[min_user] += 1

print("✅ DESPUÉS DEL BALANCE EQUITATIVO:")
print("-" * 80)

contratos_asignados = {}
for user_id in usuarios:
    nuevos = current_counts[user_id] - contratos_actuales[user_id]
    contratos_asignados[user_id] = nuevos
    print(f"   Usuario {user_id:3d}: {current_counts[user_id]:4d} contratos "
          f"(+{nuevos:2d} nuevos)")

print(f"\n   Total final: {sum(current_counts.values())}")
print()

# Verificar equidad
min_contratos = min(current_counts.values())
max_contratos = max(current_counts.values())
diferencia = max_contratos - min_contratos

print("📈 MÉTRICAS DE EQUIDAD:")
print("-" * 80)
print(f"   Mínimo de contratos: {min_contratos}")
print(f"   Máximo de contratos: {max_contratos}")
print(f"   Diferencia máxima: {diferencia}")
print()

if diferencia <= 1:
    print("   ✅ BALANCE PERFECTO: Diferencia máxima de 1 contrato")
else:
    print(f"   ⚠️ BALANCE IMPERFECTO: Diferencia de {diferencia} contratos")

print()
print("=" * 80)
print("✅ SIMULACIÓN COMPLETADA")
print("=" * 80)
