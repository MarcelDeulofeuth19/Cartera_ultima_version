# Proceso de División de Contratos y Contratos Fijos Manuales

## 📋 Resumen de Cambios

### 1. **División de Contratos (Día 1-60)** - 14 Usuarios
Se creó un sistema completo para dividir contratos entre 14 usuarios:
- **Usuarios**: 4, 7, 36, 58, 60, 62, 71, 77, 89, 90, 91, 114, 116, 113
- **Rango**: Contratos con 1 a 60 días de atraso
- **Distribución**: Equitativa usando round-robin
- **Validaciones**: Respeta contratos fijos y no duplica asignaciones

### 2. **Contratos Fijos Manuales para Casas de Cobranza**
Se implementó un sistema para agregar contratos fijos manuales:
- **Cobyser (Usuario 45)**: 79 contratos fijos manuales
- **Serlefin (Usuario 81)**: 712 contratos fijos manuales
- **Total**: 791 contratos fijos manuales
- **Validaciones por lotes**: Evita duplicados y verifica base de datos
- **Inserts optimizados**: Por lotes de 1000 contratos

---

## 🚀 Cómo Usar

### Opción 1: Usando la API (Recomendado)

#### A) Procesar Contratos Fijos Manuales de Cobyser y Serlefin
```bash
# Insertar contratos fijos manuales:
# - Cobyser (45): 79 contratos
# - Serlefin (81): 712 contratos
curl -X POST http://localhost:8000/api/v1/process-manual-fixed

# Respuesta incluye:
# - Total proporcionados: 791
# - Ya asignados: X (contratos que ya existían)
# - Insertados: Y (contratos nuevos)
# - Detalle por usuario (45 y 81)
```

#### B) Ejecutar División de Contratos (14 usuarios)
```bash
# Dividir contratos del día 1-60 entre 14 usuarios
curl -X POST http://localhost:8000/api/v1/run-division

# Genera automáticamente:
# - 14 archivos TXT (uno por usuario)
# - 1 Excel consolidado con todas las asignaciones
```

#### C) Ejecutar Asignación a Casas de Cobranza (Día 61-210)
```bash
# El proceso original sigue funcionando igual
curl -X POST http://localhost:8000/api/v1/run-assignment
```

### Opción 2: Usando Script Python

```bash
# Ejecutar división y generar Excel de asignaciones
python run_division.py

# El script:
# 1. Ejecuta el proceso de división
# 2. Genera reportes TXT para cada usuario
# 3. Genera Excel consolidado
# 4. Muestra resumen en consola
```

---

## 📊 Archivos Generados

### División de Contratos (14 usuarios):
```
reports/
├── division_contratos_4.txt
├── division_contratos_7.txt
├── division_contratos_36.txt
├── division_contratos_58.txt
├── division_contratos_60.txt
├── division_contratos_62.txt
├── division_contratos_71.txt
├── division_contratos_77.txt
├── division_contratos_89.txt
├── division_contratos_90.txt
├── division_contratos_91.txt
├── division_contratos_114.txt
├── division_contratos_116.txt
├── division_contratos_113.txt
└── reporte_division_contratos.xlsx  ← Excel con todas las asignaciones
```

### Excel de División incluye:
1. **Hoja "División Contratos"**: Todos los contratos asignados con detalles
2. **Hoja "Resumen por Usuario"**: Estadísticas de cada usuario
3. **Hoja "Metadata"**: Información del proceso y fechas

---

## 🔧 Estructura Técnica

### Nuevos Archivos Creados:
```
app/
├── data/
│   ├── __init__.py
│   └── manual_fixed_contracts.py  ← 791 contratos fijos (79 Cobyser + 712 Serlefin)
├── services/
│   ├── division_service.py        ← Servicio de división (14 usuarios)
│   └── manual_fixed_service.py    ← Servicio de contratos manuales
└── api/
    └── routes/
        └── assignment.py           ← Nuevos endpoints agregados

run_division.py                      ← Script para ejecutar división
DIVISION_CONTRATOS.md                ← Esta documentación
```

### Endpoints Disponibles:

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/api/v1/run-assignment` | POST | Asignación casas de cobranza (61-210 días) |
| `/api/v1/run-division` | POST | División 14 usuarios (1-60 días) |
| `/api/v1/process-manual-fixed` | POST | Procesar contratos fijos manuales (Cobyser + Serlefin) |
| `/api/v1/lock-status` | GET | Ver estado del proceso |
| `/api/v1/health` | GET | Health check de la API |

---

## ✅ Validaciones Implementadas

### División de Contratos:
1. ✅ Respeta contratos fijos de managements
2. ✅ No duplica contratos ya asignados
3. ✅ Distribución equitativa (round-robin)
4. ✅ Validaciones por lotes para performance
5. ✅ Registro en historial con fecha inicial

### Contratos Fijos Manuales:
1. ✅ Valida que no existan en `contract_advisors` (evita duplicados)
2. ✅ Valida contra `managements` (detecta fijos de base de datos)
3. ✅ Inserts por lotes de 1000 contratos
4. ✅ Registra en historial automáticamente
5. ✅ Retorna estadísticas detalladas por usuario (45 y 81)
6. ✅ Procesa Cobyser (79) y Serlefin (712) en una sola operación

---

## 📈 Ejemplo de Uso Completo

```bash
# 1. Iniciar el sistema
docker-compose up -d

# 2. Procesar contratos fijos manuales (Cobyser + Serlefin) - solo primera vez
curl -X POST http://localhost:8000/api/v1/process-manual-fixed

# 3. Ejecutar división de contratos (día 1-60)
curl -X POST http://localhost:8000/api/v1/run-division

# 4. Ver Excel generado
# Archivo: reports/reporte_division_contratos.xlsx

# 5. Ejecutar asignación de casas de cobranza (día 61-210)
curl -X POST http://localhost:8000/api/v1/run-assignment
```

---

## 🎯 Usuarios y Rangos

| Proceso | Usuarios | Rango de Días |
|---------|----------|---------------|
| División | 4, 7, 36, 58, 60, 62, 71, 77, 89, 90, 91, 114, 116, 113 | 1-60 días |
| Serlefin | 81 (+ 82-86, 102-103) | 61-210 días |
| Cobyser | 45 (+ 46-51) | 61-210 días |

---

## 📝 Notas Importantes

1. **No hay conflictos**: Los 3 procesos trabajan con rangos de días diferentes
2. **Sistema de locks**: Solo un proceso puede ejecutarse a la vez
3. **Transaccionalidad**: Todos los cambios tienen rollback en caso de error
4. **Performance**: Validaciones e inserts optimizados por lotes
5. **Historial**: Todas las asignaciones se registran con fecha inicial

---

## 🐛 Troubleshooting

### Si el proceso falla:
```bash
# 1. Verificar estado del lock
curl http://localhost:8000/api/v1/lock-status

# 2. Ver logs
tail -f assignment_process.log
tail -f division_process.log

# 3. Verificar conexiones
curl http://localhost:8000/api/v1/health
```

### Si hay duplicados:
- El sistema automáticamente los detecta y no los inserta
- Las estadísticas muestran cuántos se omitieron

---

## 📞 Soporte

Para cualquier problema o duda, revisar los logs:
- `assignment_process.log` - Log principal de la aplicación
- `division_process.log` - Log del script de división
- Logs de Docker: `docker-compose logs -f`
