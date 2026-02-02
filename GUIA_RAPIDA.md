# 📘 Guía de Uso Rápido - Sistema de Asignación de Contratos

## 🎯 ¿Qué hace este sistema?

Este aplicativo automatiza la asignación de contratos entre dos usuarios (45 y 81), siguiendo reglas específicas:

1. **Contratos Fijos**: Identifica contratos con `effect='pago_total'` que NUNCA se eliminan
2. **Limpieza Automática**: Elimina contratos con 0-60 días de atraso (excepto fijos)
3. **Asignación Balanceada**: Distribuye contratos con ≥61 días 50/50 entre usuarios
4. **Reportes Automáticos**: Genera archivos TXT y Excel con los resultados

---

## ⚡ Inicio Rápido (3 pasos)

### Paso 1: Instalar Dependencias

Doble clic en `start.bat` o ejecuta en terminal:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Paso 2: Verificar Conexiones (Opcional pero recomendado)

```bash
python test_connections.py
```

Esto verificará que puedas conectar a MySQL y PostgreSQL.

### Paso 3: Iniciar la API

```bash
python main.py
```

O simplemente ejecuta: `start.bat`

La API estará lista en: **http://localhost:8000**

---

## 🔥 Ejecutar el Proceso de Asignación

### Opción 1: Usando Swagger UI (Recomendado)

1. Abre tu navegador en: http://localhost:8000/docs
2. Busca el endpoint `POST /api/v1/run-assignment`
3. Clic en "Try it out"
4. Clic en "Execute"
5. ¡Listo! Verás los resultados en la respuesta

### Opción 2: Usando curl (Terminal)

```bash
curl -X POST http://localhost:8000/api/v1/run-assignment
```

### Opción 3: Usando PowerShell

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/api/v1/run-assignment"
```

---

## 📊 Entender los Resultados

### Respuesta del API (JSON)

```json
{
  "success": true,
  "message": "Proceso de asignación completado exitosamente",
  "execution_time": 12.45,  // ⏱️ Tiempo en segundos
  "results": {
    "fixed_contracts_count": {
      "user_45": 15,  // 🔒 Contratos fijos usuario 45
      "user_81": 18   // 🔒 Contratos fijos usuario 81
    },
    "contracts_processed": 250,  // 📝 Total contratos procesados
    "clean_stats": {
      "deleted_user_45": 30,     // 🗑️ Eliminados de usuario 45
      "deleted_user_81": 28,     // 🗑️ Eliminados de usuario 81
      "protected_fixed": 33      // 🛡️ Protegidos (fijos)
    },
    "balance_stats": {
      "45": 125,  // ⚖️ Asignados a usuario 45
      "81": 125   // ⚖️ Asignados a usuario 81
    },
    "insert_stats": {
      "inserted_user_45": 95,  // ➕ Nuevos contratos usuario 45
      "inserted_user_81": 97   // ➕ Nuevos contratos usuario 81
    }
  },
  "reports": {
    "user_45": "reports/asignacion_45.txt",
    "user_81": "reports/asignacion_81.txt",
    "excel_fixed": "reports/reporte_fijos_efect.xlsx"
  },
  "timestamp": "2025-02-02T10:30:00"
}
```

### Archivos Generados

Después de cada ejecución, encontrarás en la carpeta `reports/`:

#### 1. `asignacion_45.txt`
Lista simple de IDs de contratos asignados al usuario 45:
```
Asignación de Contratos - Usuario 45
Fecha: 2025-02-02 10:30:00
Total de contratos: 125
==================================================

1001
1002
1003
...
```

#### 2. `asignacion_81.txt`
Lista simple de IDs de contratos asignados al usuario 81 (mismo formato)

#### 3. `reporte_fijos_efect.xlsx`
Excel profesional con 3 hojas:
- **Contratos Fijos**: Detalle completo (ID, usuario, fecha, notas)
- **Resumen**: Totales por usuario
- **Metadata**: Información de generación

---

## 🔍 Monitoreo y Debug

### Ver estado del proceso

```bash
# Verificar si hay un proceso en ejecución
GET http://localhost:8000/api/v1/lock-status
```

### Health Check

```bash
# Verificar estado de bases de datos
GET http://localhost:8000/api/v1/health
```

### Ver logs en tiempo real

Los logs se muestran en consola y se guardan en `assignment_process.log`:

```bash
# Ver últimas 50 líneas del log
Get-Content assignment_process.log -Tail 50
```

---

## ⚙️ Configuración Personalizada

### Cambiar parámetros de negocio

Edita el archivo `.env`:

```env
# Cambiar días mínimos de atraso (por defecto 61)
DAYS_THRESHOLD=90

# Cambiar effect para contratos fijos
FIXED_CONTRACT_EFFECT=pago_total

# Habilitar modo debug (más logs)
DEBUG=True
```

### Cambiar usuarios de asignación

Edita `app/core/config.py` línea 28:

```python
USER_IDS: List[int] = [45, 81]  # Cambiar IDs aquí
```

---

## 🛠️ Solución de Problemas

### ❌ Error: "Process already running"

**Causa**: Otra instancia está en ejecución o quedó bloqueada.

**Solución**:
1. Verifica con: `GET /api/v1/lock-status`
2. Si está bloqueado, elimina el archivo: `del assignment_process.lock`

---

### ❌ Error de conexión a base de datos

**Causa**: Credenciales incorrectas o red no alcanzable.

**Solución**:
1. Verifica el health check: `GET /api/v1/health`
2. Revisa las credenciales en `.env`
3. Prueba la conectividad:
   ```bash
   # MySQL
   telnet 57.130.40.1 3306
   
   # PostgreSQL
   telnet 3.95.195.63 5432
   ```

---

### ❌ No se generan los reportes

**Causa**: Falta de permisos en el directorio `reports/`.

**Solución**:
1. Verifica que existe: `mkdir reports`
2. Asigna permisos de escritura

---

### ❌ Error: "Module not found"

**Causa**: Dependencias no instaladas.

**Solución**:
```bash
pip install -r requirements.txt
```

---

## 🔒 Seguridad y Mejores Prácticas

### ⚠️ IMPORTANTE en Producción

1. **Variables de entorno**: Usa variables de sistema en lugar de `.env`
2. **Secrets Management**: Implementa AWS Secrets Manager o Azure Key Vault
3. **Autenticación**: Agrega OAuth2 o JWT al API
4. **HTTPS**: Configura certificado SSL/TLS
5. **Rate Limiting**: Limita llamadas al endpoint
6. **Monitoring**: Integra con Prometheus/Grafana

### Ejemplo de autenticación (opcional)

```python
# En main.py
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer

security = HTTPBearer()

@app.post("/api/v1/run-assignment")
async def run_assignment(token: str = Depends(security)):
    # Validar token aquí
    ...
```

---

## 📞 Comandos Útiles

```bash
# Instalar dependencias
pip install -r requirements.txt

# Iniciar servidor modo development
python main.py

# Iniciar servidor modo production
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4

# Test de conexiones
python test_connections.py

# Ver logs en tiempo real (PowerShell)
Get-Content assignment_process.log -Wait

# Ejecutar proceso (curl)
curl -X POST http://localhost:8000/api/v1/run-assignment

# Ver estado del lock
curl http://localhost:8000/api/v1/lock-status

# Health check
curl http://localhost:8000/api/v1/health
```

---

## 🎓 Arquitectura (Resumen)

```
┌─────────────────────────────────────────────────┐
│              FastAPI Application                │
│         (Singleton con File Lock)               │
└─────────────────────────────────────────────────┘
                       │
        ┌──────────────┴──────────────┐
        ▼                             ▼
┌───────────────┐            ┌────────────────┐
│  MySQL (R/O)  │            │ PostgreSQL (RW)│
│ alocreditprod │            │   nexus_db     │
└───────────────┘            └────────────────┘
        │                             │
        │                             │
        ▼                             ▼
┌──────────────────┐        ┌─────────────────┐
│ contract_*       │        │ contract_advisors│
│ (Consultas)      │        │ managements     │
│ >= 61 días       │        │ (INSERT/DELETE) │
└──────────────────┘        └─────────────────┘
```

### Flujo de Datos

1. **Consulta MySQL**: Contratos con ≥61 días de atraso
2. **Consulta PostgreSQL**: Contratos fijos y asignaciones actuales
3. **Lógica de Negocio**: Limpieza + Balanceo 50/50
4. **Escritura PostgreSQL**: INSERT nuevas asignaciones
5. **Reportes**: TXT + Excel

---

## 📖 Recursos Adicionales

- **Documentación Interactiva**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **README Completo**: Ver `README.md`
- **Logs**: `assignment_process.log`

---

## ✅ Checklist de Despliegue

Antes de ejecutar en producción:

- [ ] Instaladas todas las dependencias (`pip install -r requirements.txt`)
- [ ] Verificadas conexiones (`python test_connections.py`)
- [ ] Configuradas variables de entorno (`.env`)
- [ ] Probado el endpoint en desarrollo
- [ ] Revisados los reportes generados
- [ ] Configurada rotación de logs
- [ ] Implementada autenticación (si es necesario)
- [ ] Configurado monitoreo y alertas
- [ ] Documentación actualizada para el equipo

---

**¿Listo para empezar?** 🚀

```bash
# Paso 1: Activa el entorno
venv\Scripts\activate

# Paso 2: Inicia el servidor
python main.py

# Paso 3: Abre el navegador
start http://localhost:8000/docs
```

---

**Versión**: 1.0.0 | **Última actualización**: Febrero 2025
