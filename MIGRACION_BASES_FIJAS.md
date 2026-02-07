# 🚀 Instrucciones de Migración: Nueva Lógica de Bases Fijas

## Resumen de Cambios

Se ha actualizado la lógica de **bases fijas** para aplicar dos filtros temporales:

1. **`acuerdo_de_pago`**: Mantener solo si `promise_date >= HOY`
2. **`pago_total`**: Mantener solo si `management_date` es máximo de 30 días

## 📋 Pasos de Migración

### Paso 1: Agregar columna `is_fixed` a la tabla `managements`

```bash
# Ejecutar script de migración en PostgreSQL
psql -h 3.95.195.63 -U nexus_dev_84 -d nexus_db -f migrations/add_is_fixed_column.sql
```

O manualmente:

```sql
-- Conectarse a PostgreSQL
psql -h 3.95.195.63 -U nexus_dev_84 -d nexus_db

-- Ejecutar comandos
ALTER TABLE alocreditindicators.managements 
ADD COLUMN IF NOT EXISTS is_fixed INTEGER DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_managements_is_fixed 
ON alocreditindicators.managements(is_fixed);

UPDATE alocreditindicators.managements 
SET is_fixed = 1 
WHERE is_fixed IS NULL;
```

### Paso 2: Actualizar variables de entorno

Editar archivo `.env` (o crear desde `.env.example`):

```env
# Configuración de negocio
DAYS_THRESHOLD=61
MAX_DAYS_THRESHOLD=210

# Efectos que determinan contratos fijos
EFFECT_ACUERDO_PAGO=acuerdo_de_pago
EFFECT_PAGO_TOTAL=pago_total

# Período de validez para pago_total (días)
PAGO_TOTAL_VALIDITY_DAYS=30

# Para retrocompatibilidad
FIXED_CONTRACT_EFFECT=pago_total
```

### Paso 3: Probar la nueva lógica (SIN modificar datos)

```bash
# Ejecutar script de prueba
python test_fixed_logic.py
```

Este script mostrará:
- ✅ Cuántos contratos son válidos para cada efecto
- ❌ Cuántos contratos han expirado
- 📊 Distribución por casa de cobranza (COBYSER/SERLEFIN)

**Ejemplo de salida:**
```
📈 RESUMEN DE RESULTADOS:
🔵 ACUERDO DE PAGO:
   ✅ Válidos (promise_date >= hoy):     45
   ❌ Expirados (promise_date < hoy):    12
🟢 PAGO TOTAL:
   ✅ Válidos (≤ 30 días):               78
   ❌ Expirados (> 30 días):             23
🏢 CONTRATOS FIJOS POR CASA DE COBRANZA:
   📌 COBYSER (Usuario 45):              65 contratos
   📌 SERLEFIN (Usuario 81):             58 contratos
   📌 TOTAL:                             123 contratos
```

### Paso 4: Reiniciar la aplicación

```bash
# Si usas Docker
docker-compose down
docker-compose up -d

# Si usas script local
# Windows
start.bat

# Linux/Mac
./start.sh
```

## 🧪 Verificación Post-Migración

### 1. Verificar que el campo `is_fixed` existe

```sql
SELECT column_name, data_type, column_default 
FROM information_schema.columns 
WHERE table_schema = 'alocreditindicators' 
  AND table_name = 'managements' 
  AND column_name = 'is_fixed';
```

### 2. Ejecutar proceso de asignación

```bash
# Llamar al endpoint de asignación
curl -X POST http://localhost:8000/api/v1/run-assignment
```

### 3. Revisar logs

Buscar en los logs estas líneas:

```
✓ Análisis de contratos fijos completado:
  Acuerdo de Pago:
    - Válidos (promise_date >= hoy): XX
    - Expirados (promise_date < hoy): XX
  Pago Total:
    - Válidos (≤ 30 días): XX
    - Expirados (> 30 días): XX
```

## 📊 Monitoreo

### Consulta para ver registros marcados como NO fijos

```sql
SELECT 
    effect,
    COUNT(*) as total,
    COUNT(CASE WHEN is_fixed = 1 THEN 1 END) as activos,
    COUNT(CASE WHEN is_fixed = 0 THEN 1 END) as inactivos
FROM alocreditindicators.managements
WHERE effect IN ('acuerdo_de_pago', 'pago_total')
GROUP BY effect;
```

### Consulta para ver contratos con acuerdos expirados

```sql
SELECT 
    id,
    user_id,
    contract_id,
    promise_date,
    CURRENT_DATE - promise_date as dias_expirado,
    is_fixed
FROM alocreditindicators.managements
WHERE effect = 'acuerdo_de_pago'
  AND promise_date < CURRENT_DATE
ORDER BY promise_date DESC
LIMIT 20;
```

### Consulta para ver pagos totales expirados

```sql
SELECT 
    id,
    user_id,
    contract_id,
    management_date,
    EXTRACT(DAY FROM CURRENT_TIMESTAMP - management_date) as dias_transcurridos,
    is_fixed
FROM alocreditindicators.managements
WHERE effect = 'pago_total'
  AND management_date < (CURRENT_TIMESTAMP - INTERVAL '30 days')
ORDER BY management_date DESC
LIMIT 20;
```

## 🔄 Rollback (si es necesario)

Si necesitas revertir los cambios:

```sql
-- Eliminar columna is_fixed
ALTER TABLE alocreditindicators.managements 
DROP COLUMN IF EXISTS is_fixed;

-- Eliminar índice
DROP INDEX IF EXISTS alocreditindicators.idx_managements_is_fixed;
```

Y revertir el código a la versión anterior usando Git:

```bash
git log --oneline  # Ver commits
git checkout <commit_anterior>
```

## 📝 Notas Importantes

1. ⚡ **Performance**: Las actualizaciones se hacen por lotes, optimizado para grandes volúmenes
2. 🔒 **No destructivo**: Los registros se marcan con `is_fixed=0`, no se eliminan
3. 📊 **Auditoría**: Todos los cambios se registran en logs detallados
4. 🔄 **Reversible**: La migración puede revertirse si es necesario

## ✅ Checklist de Migración

- [ ] Backup de base de datos PostgreSQL
- [ ] Ejecutar script de migración SQL
- [ ] Verificar que columna `is_fixed` existe
- [ ] Actualizar archivo `.env`
- [ ] Ejecutar `test_fixed_logic.py` para verificar
- [ ] Reiniciar aplicación
- [ ] Probar endpoint `/run-assignment`
- [ ] Verificar logs para confirmar nueva lógica
- [ ] Monitorear primeras ejecuciones

## 🆘 Soporte

Si encuentras problemas:

1. Revisa los logs de la aplicación: `logs/`
2. Ejecuta `test_fixed_logic.py` para diagnóstico
3. Consulta la documentación: [BASES_FIJAS.md](BASES_FIJAS.md)
4. Verifica las consultas SQL de monitoreo

## 📚 Documentación Relacionada

- [BASES_FIJAS.md](BASES_FIJAS.md) - Documentación completa de la lógica
- [GUIA_RAPIDA.md](GUIA_RAPIDA.md) - Guía de uso general
- [README.md](README.md) - Documentación principal del proyecto
