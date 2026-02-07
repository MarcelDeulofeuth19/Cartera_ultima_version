# 🔒 Documentación: Lógica de Bases Fijas

## Descripción General

Los **contratos fijos** son contratos que se mantienen permanentemente asignados a un asesor y están protegidos contra la limpieza automática. Esta protección se basa en el campo `effect` de la tabla `managements` en PostgreSQL.

## Filtros para Determinar Contratos Fijos

Los filtros se aplican **en orden** para determinar qué contratos son considerados "fijos":

### ✅ FILTRO 0: `effect = 'acuerdo_de_pago'`

**Condición para mantenerlo como fijo:**
- El campo `promise_date` debe ser **>= fecha actual** (HOY)
- Si `promise_date < HOY` → El contrato **NO** es fijo

**Lógica:**
Si un cliente hizo un acuerdo de pago, ese contrato se mantiene fijo SOLO mientras la promesa de pago no haya expirado.

```sql
-- Ejemplo: Contrato FIJO
promise_date = '2026-03-01'  -- Fecha futura → ES FIJO
current_date = '2026-02-04'

-- Ejemplo: Contrato NO FIJO
promise_date = '2026-01-15'  -- Fecha pasada → NO ES FIJO
current_date = '2026-02-04'
```

### ✅ FILTRO 1: `effect = 'pago_total'`

**Condición para mantenerlo como fijo:**
- El campo `management_date` debe ser **máximo de 30 días** desde HOY
- Si `management_date < (HOY - 30 días)` → El contrato **NO** es fijo

**Lógica:**
Un pago total mantiene el contrato como fijo durante 30 días. Después de ese período, se considera que ya no requiere seguimiento especial.

```sql
-- Ejemplo: Contrato FIJO
management_date = '2026-01-20'  -- Hace 15 días → ES FIJO
current_date    = '2026-02-04'
dias_transcurridos = 15 (≤ 30)

-- Ejemplo: Contrato NO FIJO
management_date = '2025-12-20'  -- Hace 46 días → NO ES FIJO
current_date    = '2026-02-04'
dias_transcurridos = 46 (> 30)
```

## Orden de Ejecución

**⚠️ IMPORTANTE:** Los filtros se ejecutan en orden porque un contrato puede tener múltiples registros:

1. **Primero**: Se evalúa `acuerdo_de_pago`
2. **Después**: Se evalúa `pago_total`

**Ejemplo de caso:**
```
Contrato 12345:
  - 2026-01-10: acuerdo_de_pago (promise_date: 2026-01-25) ❌ Expirado
  - 2026-01-28: pago_total (management_date: 2026-01-28) ✅ Válido (7 días)

→ Resultado: ES FIJO (por el pago_total reciente)
```

## Asignación por Casa de Cobranza

Los contratos fijos se consolidan en usuarios principales:

### COBYSER → Usuario 45
- Usuarios origen: 45, 46, 47, 48, 49, 50, 51
- Todos los contratos fijos se asignan a: **Usuario 45**

### SERLEFIN → Usuario 81
- Usuarios origen: 81, 82, 83, 84, 85, 86, 102, 103
- Todos los contratos fijos se asignan a: **Usuario 81**

## Marcado de Registros No Fijos

Cuando un contrato deja de cumplir las condiciones, el sistema:

1. **Marca el registro** en `managements` con `is_fixed = 0`
2. **Actualización por lotes** para optimizar rendimiento
3. **NO elimina** el registro, solo lo marca como inactivo

```sql
-- Actualización por lotes (optimizado)
UPDATE alocreditindicators.managements 
SET is_fixed = 0 
WHERE id IN (123, 456, 789, ...);
```

## Protecciones de Contratos Fijos

### 1. ✅ Asignación Garantizada
Los contratos fijos se insertan automáticamente en `contract_advisors` si no están asignados.

### 2. ✅ Protección contra Limpieza
Los contratos fijos **NUNCA** se eliminan durante el proceso de limpieza (incluso si tienen 0-60 días de atraso).

### 3. ✅ Exclusión del Balanceo
Los contratos fijos **NO** participan en el balanceo par/impar. Solo los contratos nuevos se balancean.

## Configuración

Parámetros en [.env](.env.example):

```env
# Efectos que determinan contratos fijos
EFFECT_ACUERDO_PAGO=acuerdo_de_pago
EFFECT_PAGO_TOTAL=pago_total

# Período de validez para pago_total (días)
PAGO_TOTAL_VALIDITY_DAYS=30
```

## Flujo del Proceso

```
┌─────────────────────────────────────────────┐
│ 1. Consultar managements                    │
│    - effect IN ('acuerdo_de_pago',          │
│                 'pago_total')               │
└───────────────┬─────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────┐
│ 2. Aplicar FILTRO 0: acuerdo_de_pago       │
│    - Validar: promise_date >= HOY           │
│    - Si expiró → marcar is_fixed=0          │
└───────────────┬─────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────┐
│ 3. Aplicar FILTRO 1: pago_total            │
│    - Validar: management_date ≤ 30 días    │
│    - Si expiró → marcar is_fixed=0          │
└───────────────┬─────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────┐
│ 4. Actualizar is_fixed=0 (por lotes)       │
│    - UPDATE masivo optimizado               │
└───────────────┬─────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────┐
│ 5. Retornar contratos fijos válidos         │
│    - Usuario 45: [contratos COBYSER]       │
│    - Usuario 81: [contratos SERLEFIN]      │
└─────────────────────────────────────────────┘
```

## Estadísticas en Logs

El proceso muestra estadísticas detalladas:

```
✓ Análisis de contratos fijos completado:
  Acuerdo de Pago:
    - Válidos (promise_date >= hoy): 45
    - Expirados (promise_date < hoy): 12
  Pago Total:
    - Válidos (≤ 30 días): 78
    - Expirados (> 30 días): 23
  
  Contratos fijos activos:
    - COBYSER (Usuario 45): 65 contratos
    - SERLEFIN (Usuario 81): 58 contratos
    - Total: 123
```

## Migración de Base de Datos

Para agregar el campo `is_fixed` a la tabla `managements`:

```bash
psql -h 3.95.195.63 -U nexus_dev_84 -d nexus_db -f migrations/add_is_fixed_column.sql
```

O ejecutar manualmente:

```sql
ALTER TABLE alocreditindicators.managements 
ADD COLUMN IF NOT EXISTS is_fixed INTEGER DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_managements_is_fixed 
ON alocreditindicators.managements(is_fixed);
```

## Notas Importantes

1. ⚡ **Optimización**: Las actualizaciones se hacen por lotes para mejor rendimiento
2. 🔄 **No destructivo**: Los registros se marcan, no se eliminan
3. 📊 **Auditoría**: Todos los cambios generan logs detallados
4. 🔒 **Transaccionalidad**: El proceso es atómico con rollback automático en caso de error
