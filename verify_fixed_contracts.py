"""
Script de verificación de integridad de contratos fijos manuales.
Este script valida que las listas de contratos fijos no hayan sido modificadas.
"""

from app.data.manual_fixed_contracts import (
    COBYSER_MANUAL_FIXED,
    SERLEFIN_MANUAL_FIXED,
    MANUAL_FIXED_CONTRACTS
)

# Valores esperados
EXPECTED_COBYSER_COUNT = 79
EXPECTED_SERLEFIN_COUNT = 415
EXPECTED_TOTAL_COUNT = 494

def verify_integrity():
    """Verifica la integridad de las listas de contratos fijos."""
    
    print("=" * 80)
    print("VERIFICACIÓN DE INTEGRIDAD - CONTRATOS FIJOS MANUALES")
    print("=" * 80)
    
    errors = []
    warnings = []
    
    # Verificar cantidad de contratos Cobyser
    cobyser_count = len(COBYSER_MANUAL_FIXED)
    print(f"\n✓ Contratos Cobyser (Usuario 45): {cobyser_count}")
    if cobyser_count != EXPECTED_COBYSER_COUNT:
        errors.append(
            f"ERROR: Cobyser debe tener {EXPECTED_COBYSER_COUNT} contratos, "
            f"pero tiene {cobyser_count}"
        )
    
    # Verificar cantidad de contratos Serlefin
    serlefin_count = len(SERLEFIN_MANUAL_FIXED)
    print(f"✓ Contratos Serlefin (Usuario 81): {serlefin_count}")
    if serlefin_count != EXPECTED_SERLEFIN_COUNT:
        errors.append(
            f"ERROR: Serlefin debe tener {EXPECTED_SERLEFIN_COUNT} contratos, "
            f"pero tiene {serlefin_count}"
        )
    
    # Verificar total
    total_count = cobyser_count + serlefin_count
    print(f"✓ Total de contratos fijos: {total_count}")
    if total_count != EXPECTED_TOTAL_COUNT:
        errors.append(
            f"ERROR: Total debe ser {EXPECTED_TOTAL_COUNT} contratos, "
            f"pero es {total_count}"
        )
    
    # Verificar duplicados en Cobyser
    cobyser_duplicates = len(COBYSER_MANUAL_FIXED) - len(set(COBYSER_MANUAL_FIXED))
    if cobyser_duplicates > 0:
        errors.append(f"ERROR: Cobyser tiene {cobyser_duplicates} contratos duplicados")
    else:
        print("✓ Sin duplicados en Cobyser")
    
    # Verificar duplicados en Serlefin
    serlefin_duplicates = len(SERLEFIN_MANUAL_FIXED) - len(set(SERLEFIN_MANUAL_FIXED))
    if serlefin_duplicates > 0:
        errors.append(f"ERROR: Serlefin tiene {serlefin_duplicates} contratos duplicados")
    else:
        print("✓ Sin duplicados en Serlefin")
    
    # Verificar contratos cruzados
    cobyser_set = set(COBYSER_MANUAL_FIXED)
    serlefin_set = set(SERLEFIN_MANUAL_FIXED)
    shared_contracts = cobyser_set.intersection(serlefin_set)
    if shared_contracts:
        warnings.append(
            f"ADVERTENCIA: {len(shared_contracts)} contratos están en ambas listas: "
            f"{sorted(list(shared_contracts)[:5])}{'...' if len(shared_contracts) > 5 else ''}"
        )
    else:
        print("✓ Sin contratos compartidos entre Cobyser y Serlefin")
    
    # Verificar diccionario
    if 45 not in MANUAL_FIXED_CONTRACTS:
        errors.append("ERROR: Usuario 45 (Cobyser) no está en el diccionario")
    if 81 not in MANUAL_FIXED_CONTRACTS:
        errors.append("ERROR: Usuario 81 (Serlefin) no está en el diccionario")
    if len(MANUAL_FIXED_CONTRACTS) != 2:
        errors.append(f"ERROR: El diccionario debe tener 2 usuarios, tiene {len(MANUAL_FIXED_CONTRACTS)}")
    else:
        print("✓ Diccionario correctamente configurado")
    
    # Verificar que todos los contratos sean números positivos
    invalid_cobyser = [c for c in COBYSER_MANUAL_FIXED if not isinstance(c, int) or c <= 0]
    invalid_serlefin = [c for c in SERLEFIN_MANUAL_FIXED if not isinstance(c, int) or c <= 0]
    
    if invalid_cobyser:
        errors.append(f"ERROR: Cobyser tiene {len(invalid_cobyser)} contratos inválidos")
    else:
        print("✓ Todos los contratos de Cobyser son números válidos")
    
    if invalid_serlefin:
        errors.append(f"ERROR: Serlefin tiene {len(invalid_serlefin)} contratos inválidos")
    else:
        print("✓ Todos los contratos de Serlefin son números válidos")
    
    # Mostrar resultados
    print("\n" + "=" * 80)
    if errors:
        print("❌ VERIFICACIÓN FALLIDA - SE ENCONTRARON ERRORES:")
        print("=" * 80)
        for error in errors:
            print(f"  {error}")
        return False
    elif warnings:
        print("⚠️ VERIFICACIÓN COMPLETADA CON ADVERTENCIAS:")
        print("=" * 80)
        for warning in warnings:
            print(f"  {warning}")
        return True
    else:
        print("✅ VERIFICACIÓN EXITOSA - INTEGRIDAD CONFIRMADA")
        print("=" * 80)
        print("\nLas listas de contratos fijos están correctas y no han sido modificadas.")
        return True

if __name__ == "__main__":
    success = verify_integrity()
    exit(0 if success else 1)
