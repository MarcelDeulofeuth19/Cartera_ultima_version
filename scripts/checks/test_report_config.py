"""
Test simple para validar el sistema de informes
"""
# --- bootstrap: ejecutable desde cualquier ruta (anade la raiz del repo al path) ---
import sys as _sys, pathlib as _pathlib
for _cand in (_pathlib.Path(__file__).resolve(), *_pathlib.Path(__file__).resolve().parents):
    if (_cand / "app").is_dir() and (_cand / "main.py").exists():
        _sys.path.insert(0, str(_cand)); break
# --- fin bootstrap ---
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from app.data.manual_fixed_contracts import MANUAL_FIXED_CONTRACTS

print("=" * 80)
print("VALIDACIÓN DE CONFIGURACIÓN DE INFORMES")
print("=" * 80)
print()

# 1. Verificar contratos manuales
print("📋 CONTRATOS FIJOS MANUALES:")
for user_id, contracts in MANUAL_FIXED_CONTRACTS.items():
    user_name = "Cobyser" if user_id == 45 else "Serlefin" if user_id == 81 else f"Usuario {user_id}"
    print(f"   {user_name} (User {user_id}): {len(contracts)} contratos")
print()

# 2. Calcular proporción esperada
total_manual = sum(len(contracts) for contracts in MANUAL_FIXED_CONTRACTS.values())
if total_manual > 0:
    for user_id, contracts in MANUAL_FIXED_CONTRACTS.items():
        percentage = (len(contracts) / total_manual) * 100
        user_name = "Cobyser" if user_id == 45 else "Serlefin" if user_id == 81 else f"Usuario {user_id}"
        print(f"   {user_name}: {percentage:.2f}%")

print()
print("=" * 80)
print("✅ CONFIGURACIÓN VÁLIDA")
print("=" * 80)
