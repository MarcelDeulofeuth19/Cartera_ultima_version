# Sistema de Informes y Envío Automático por Email

## 📊 Funcionalidades Implementadas

### 1. Generación Automática de Informes Excel
- **Archivo**: `app/services/report_service_extended.py`
- Genera informes detallados para Serlefin (Usuario 81) y Cobyser (Usuario 45)
- **Columnas incluidas**:
  - Todos los campos originales (NIT, Producto, Contrato, Cliente, etc.)
  - **NUEVO**: Columna `Contrato_Fijo` que indica si es "SI" o "NO"
  - Opciones de pago, descuentos, comisiones, etc.

### 2. Envío Automático por Email
- **Archivo**: `app/services/email_service.py`
- Envía los informes automáticamente al finalizar la asignación
- **Destinatario**: mdeulofeuth@alocredit.co
- **Archivos adjuntos**:
  - Informe Serlefin (Excel)
  - Informe Cobyser (Excel)
- **Contenido**: Métricas de distribución 60/40 en formato HTML

### 3. Validación de Proporción 60/40
- Valida si la distribución cumple:
  - Serlefin: 60% (±2% tolerancia = 58-62%)
  - Cobyser: 40% (±2% tolerancia = 38-42%)
- Genera alertas visuales en el email si NO cumple

### 4. Integración Automática
- **Archivo modificado**: `app/services/assignment_service.py`
- El método `execute_assignment_process()` ahora:
  1. Ejecuta la asignación normal
  2. Genera automáticamente los informes
  3. Envía los informes por email
  4. Todo sin intervención manual

## 🔧 Configuración de Email

```python
SMTP_SERVER = "smtp-relay.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "noreply@alocredit.co"
EMAIL_PASSWORD = "dzxivlyusuprwesu"
EMAIL_FROM = "noreply@alocredit.co"
DESTINATARIO = "mdeulofeuth@alocredit.co"
```

## 📝 Archivos Creados/Modificados

### Nuevos Archivos:
1. `app/services/email_service.py` - Servicio de envío de emails
2. `app/services/report_service_extended.py` - Generación de informes detallados
3. `generate_and_send_reports.py` - Script ejecutable independiente
4. `test_report_config.py` - Test de validación de configuración

### Archivos Modificados:
1. `app/services/assignment_service.py` - Integra envío automático de informes
2. `app/data/manual_fixed_contracts.py` - Actualizada lista de contratos fijos Serlefin (424 contratos)

## 🚀 Uso del Sistema

### Opción 1: Automático (Integrado en Asignación)
```python
# Al ejecutar el proceso de asignación normal, los informes se envían automáticamente
from app.services.assignment_service import AssignmentService

service = AssignmentService(mysql_session, postgres_session)
results = service.execute_assignment_process()
# Los informes se generan y envían automáticamente al finalizar
```

### Opción 2: Manual (Script Independiente)
```bash
# Genera y envía informes sin ejecutar nueva asignación
python generate_and_send_reports.py
```

### Opción 3: Test de Configuración
```bash
# Valida la configuración de contratos fijos
python test_report_config.py
```

## 📊 Estado Actual de Bases Fijas

**Contratos Fijos Manuales:**
- Serlefin (Usuario 81): 424 contratos (84.26%)
- Cobyser (Usuario 45): 79 contratos (15.74%)
- **Total**: 503 contratos fijos manuales

**⚠️ NOTA**: Las bases fijas NO cumplen actualmente el 60/40
- Meta: Serlefin 60% / Cobyser 40%
- Actual: Serlefin 84.26% / Cobyser 15.74%

## 🔍 Contenido del Email

El email enviado incluye:

```html
<h1>📊 Informes de Asignación de Cartera</h1>

Métricas de Distribución:
┌─────────────────────┬───────────┬────────────┬──────────┐
│ Casa de Cobranza    │ Contratos │ Porcentaje │ Fijos    │
├─────────────────────┼───────────┼────────────┼──────────┤
│ Serlefin (User 81)  │    XXX    │   XX.XX%   │   424    │
│ Cobyser (User 45)   │    XXX    │   XX.XX%   │    79    │
│ TOTAL               │    XXX    │  100.00%   │   503    │
└─────────────────────┴───────────┴────────────┴──────────┘

✅/⚠️ Cumplimiento 60/40: SÍ CUMPLE / NO CUMPLE
```

## 📋 Estructura de los Excel Generados

### Columnas Principales:
1. `NIT` - 901546410-9
2. `Llave` - PHONExxxxx
3. `Producto` - PHONE
4. `Contrato_x` - ID del contrato
5. **`Contrato_Fijo`** - ✨ **NUEVO**: "SI" o "NO"
6. `cliente` - Nombre completo
7. `telefono` - Teléfono
8. `correo` - Email
9. `cedula` - DNI
10. `ciudad` - Ciudad
11. `capital_pendiente` - Capital pendiente
12. `gastos_vencidos` - Gastos vencidos
13. `deuda_actual` - Deuda total
14. `%_Pago_capital` - Porcentaje de pago
15. `%_Descuento_gastos` - Porcentaje de descuento
16. `valor_opcion_1` - Opción de pago 1
17. ... (más opciones de pago)
18. `Comision` - Comisión aplicable
19. `Rango` - Rango de días

### Diferencias entre Archivos:
- **Serlefin**: Comisión variable según días de atraso (4%-15%)
- **Cobyser**: Comisión fija de 30%

## 🎯 Flujo Completo del Proceso

```
1. [Asignación de Contratos]
   ↓
2. [Proceso de Balanceo]
   ↓
3. [Guardar Asignaciones]
   ↓
4. [Generar Informes Excel]
   ├─ Serlefin (Usuario 81)
   └─ Cobyser (Usuario 45)
   ↓
5. [Calcular Métricas 60/40]
   ↓
6. [Enviar Email con Adjuntos]
   ↓
7. [✅ Proceso Completado]
```

## ⚙️ Dependencias Requeridas

Ya instaladas en `requirements.txt`:
- pandas==2.2.0
- openpyxl==3.1.2
- psycopg2-binary==2.9.9
- (SMTP integrado en Python estándar)

## 🐛 Troubleshooting

### Email no se envía
1. Verificar conexión a `smtp-relay.gmail.com`
2. Validar credenciales en `email_service.py`
3. Revisar logs del sistema

### Informes no se generan
1. Verificar que existan contratos asignados
2. Verificar conexión a base de datos PostgreSQL
3. Verificar conexión a base de datos MySQL (alocreditprod)

### Proporción 60/40 no se cumple
1. Ajustar cantidad de contratos fijos manuales en `manual_fixed_contracts.py`
2. Re-ejecutar el balance de asignaciones
3. El sistema reportará el estado actual automáticamente

## 📞 Contacto

Para modificaciones o soporte:
- Revisa los logs en `logs/`
- Consulta el archivo `progress.md` para seguimiento

---

**Última actualización**: 2026-02-07
**Versión**: 1.0.0
