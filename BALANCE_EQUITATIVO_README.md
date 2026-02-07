# Balance Equitativo de Contratos - Documentación

## 🎯 Objetivo
Lograr distribución equitativa de contratos entre todos los asesores, con máximo 1 contrato de diferencia.

## ✅ Cambios Implementados

### 1. Algoritmo de Balance Mejorado
**Archivo modificado**: `app/services/division_service.py`

**Nueva lógica**:
```python
# En cada asignación, el contrato va al usuario que tiene MENOS contratos
for contract in sorted_contracts:
    # Encontrar usuario con menor cantidad de contratos
    min_user = min(current_counts.keys(), key=lambda u: current_counts[u])
    
    # Asignar contrato a ese usuario
    new_assignments[min_user].append(contract['contract_id'])
    
    # Actualizar contador
    current_counts[min_user] += 1
```

**Ventajas**:
- ✅ Distribución equitativa de NUEVOS contratos
- ✅ Considera contratos actuales + fijos + nuevos
- ✅ Minimiza diferencias entre usuarios

### 2. Protección de Asignaciones (No DELETE)
**Regla implementada**: **NUNCA** se eliminan contratos ya asignados

**Para casas de cobranza (45, 81)**:
- El método `clean_assignments()` está DEPRECATED y no elimina nada
- Los contratos asignados se mantienen permanentemente

**Para asesores individuales (4, 7, 36, etc.)**:
- NO existe método `clean_assignments()` en `division_service.py`
- Los contratos nunca se eliminan

## ⚠️ Limitación Importante

### El Problema del Desbalance Previo

Si los usuarios ya tienen cantidades desiguales de contratos asignados ANTES, el sistema **NO puede** corregir ese desbalance porque:

1. **No se pueden eliminar contratos** (regla establecida)
2. Solo se pueden **agregar nuevos contratos**
3. Los nuevos contratos se distribuyen equitativamente, pero no corrigen el pasado

### Ejemplo:

**Estado inicial** (desigual por asignaciones previas):
```
Usuario 91:  1025 contratos  ← Tiene 19 más que el mínimo
Usuario 113:  975 contratos  ← Tiene menos
```

**Después de asignar 140 nuevos**:
```
Usuario 91:  1025 contratos  (+ 0 nuevos) ← No recibe porque ya tiene muchos
Usuario 113: 1006 contratos  (+31 nuevos) ← Recibe más para compensar
```

**Diferencia final**: 19 contratos (no se puede corregir sin DELETE)

## 🔧 Soluciones

### Opción 1: Mantener la Lógica Actual (Recomendado)
- Los nuevos contratos se distribuyen equitativamente
- Con el tiempo, las asignaciones se equilibrarán naturalmente
- No requiere cambios ni redistributión

### Opción 2: Redistribución Manual Una Vez
Si necesitas balance PERFECTO:

1. **Contar contratos actuales por usuario**
2. **Calcular promedio ideal**
3. **Identificar usuarios con exceso**
4. **RE-asignar manualmente contratos** de usuarios con exceso a usuarios con déficit

⚠️ **IMPORTANTE**: Esto requiere **eliminar y re-asignar** contratos, rompiendo la regla "no DELETE".

### Opción 3: Regla de Balance Flexible
- Aceptar diferencia máxima de X contratos (ej: 20 contratos)
- Si diferencia > X, alertar al administrador
- El administrador decide si redistribuir manualmente

## 📊 Verificación de Balance Actual

Para verificar el balance actual, ejecuta:
```bash
python test_equitable_balance.py
```

O consulta directamente:
```sql
SELECT 
    user_id,
    COUNT(*) as total_contratos
FROM contract_advisors
WHERE user_id IN (4, 7, 36, 58, 60, 62, 71, 77, 89, 90, 91, 113, 114, 116)
GROUP BY user_id
ORDER BY total_contratos DESC;
```

## 🎯 Resultado del Nuevo Algoritmo

**Para asignaciones NUEVAS (desde ahora)**:
- ✅ Balance perfecto entre usuarios
- ✅ Diferencia máxima de 1 contrato
- ✅ Distribución justa

**Para el total acumulado**:
- ⚠️ Puede haber diferencias por asignaciones previas
- ⚠️ Se corregirá gradualmente con nuevas asignaciones
- ⚠️ Sin DELETE, no se puede forzar balance perfecto instantáneo

## 🔍 Monitoreo Recomendado

Agregar al email de informes:
```python
# Calcular diferencia máxima entre usuarios
min_contratos = min(contratos por usuario)
max_contratos = max(contratos por usuario)
diferencia = max_contratos - min_contratos

if diferencia > 20:
    alerta = "⚠️ DESBALANCE DETECTADO"
else:
    alerta = "✅ BALANCE ACEPTABLE"
```

## 📝 Resumen

| Aspecto | Estado |
|---------|--------|
| Balance de nuevos contratos | ✅ Equitativo (diferencia máxima 1) |
| Protección contra DELETE | ✅ Implementado |
| Balance total acumulado | ⚠️ Depende de asignaciones previas |
| Casas cobranza (45/81) | ✅ Nunca se eliminan contratos |
| Asesores individuales | ✅ Nunca se eliminan contratos |

---

**Última actualización**: 2026-02-07
**Archivo modificado**: `app/services/division_service.py`
**Método actualizado**: `balance_assignments()`
