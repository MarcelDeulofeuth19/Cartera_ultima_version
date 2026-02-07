# Sistema de Asignación de Contratos - FastAPI

API profesional para la asignación automática de contratos entre asesores, implementando lógica de contratos fijos, limpieza y balanceo 50/50.

## 🚀 Características

- **Arquitectura Monolito Modular**: Estructura profesional con separación de responsabilidades
- **Singleton Pattern**: File lock para garantizar una única instancia en ejecución
- **Dual Database**: Integración con MySQL (consultas) y PostgreSQL (escrituras)
- **Contratos Fijos**: Priorización de contratos con effect='pago_total'
- **Balanceo Inteligente**: Distribución 50/50 con manejo de números impares
- **Transaccionalidad**: Commit/Rollback automático en todas las operaciones
- **Reportes Automáticos**: Generación de archivos TXT y Excel
- **Health Checks**: Monitoreo del estado de la aplicación y bases de datos

## 📁 Estructura del Proyecto

```
.
├── app/
│   ├── core/                  # Configuración central
│   │   ├── config.py          # Settings y credenciales
│   │   └── file_lock.py       # Singleton pattern
│   ├── database/              # Gestión de bases de datos
│   │   ├── connections.py     # SQLAlchemy engines
│   │   └── models.py          # Modelos ORM
│   ├── services/              # Lógica de negocio
│   │   ├── contract_service.py       # Consultas de contratos
│   │   ├── assignment_service.py     # Lógica de asignación
│   │   └── report_service.py         # Generación de reportes
│   └── api/
│       └── routes/
│           └── assignment.py  # Endpoints FastAPI
├── reports/                   # Directorio de reportes generados
├── main.py                    # Punto de entrada de la aplicación
├── requirements.txt           # Dependencias
├── .env.example              # Plantilla de variables de entorno
└── README.md                 # Este archivo
```

## 🛠️ Instalación

### 1. Clonar o descargar el proyecto

```bash
cd "C:\Users\Alo User\Desktop\CODIGOS_ALO\Nuevo aplicativo Cartera"
```

### 2. Crear entorno virtual (recomendado)

```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar variables de entorno

Copia `.env.example` a `.env` y ajusta las credenciales si es necesario:

```bash
copy .env.example .env
```

Las credenciales ya están preconfiguradas para:
- **MySQL**: alocreditprod en 57.130.40.1
- **PostgreSQL**: nexus_db en 3.95.195.63

## 🚀 Ejecución

### Modo Development (con auto-reload)

```bash
python main.py
```

### Modo Production (con Uvicorn)

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

La API estará disponible en: **http://localhost:8000**

## 📚 Documentación de la API

Una vez iniciada la aplicación, accede a:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## 🔌 Endpoints Disponibles

### 1. Ejecutar Proceso de Asignación

**POST** `/api/v1/run-assignment`

Ejecuta el proceso completo de asignación de contratos.

**Response Example:**
```json
{
  "success": true,
  "message": "Proceso de asignación completado exitosamente",
  "execution_time": 12.45,
  "results": {
    "fixed_contracts_count": {
      "user_45": 15,
      "user_81": 18
    },
    "contracts_processed": 250,
    "clean_stats": {
      "deleted_user_45": 30,
      "deleted_user_81": 28,
      "protected_fixed": 33
    },
    "balance_stats": {
      "45": 125,
      "81": 125
    }
  },
  "reports": {
    "user_45": "reports/asignacion_45.txt",
    "user_81": "reports/asignacion_81.txt",
    "excel_fixed": "reports/reporte_fijos_efect.xlsx"
  }
}
```

### 2. Verificar Estado del Lock

**GET** `/api/v1/lock-status`

Consulta si hay una instancia del proceso en ejecución.

### 3. Health Check

**GET** `/api/v1/health`

Verifica el estado de la API y las conexiones de bases de datos.

## 📊 Reportes Generados

Cada ejecución genera 3 archivos en el directorio `reports/`:

1. **asignacion_45.txt**: IDs de contratos asignados al usuario 45
2. **asignacion_81.txt**: IDs de contratos asignados al usuario 81
3. **reporte_fijos_efect.xlsx**: Excel detallado con contratos fijos
   - Hoja "Contratos Fijos": Detalle completo
   - Hoja "Resumen": Totales por usuario
   - Hoja "Metadata": Información de generación

## 🔒 Lógica de Negocio

### Contratos Fijos

Los contratos son considerados FIJOS si cumplen:
- `effect = 'pago_total'` en la tabla `managements`
- Asignados a usuarios 45 o 81

**Reglas:**
- Los contratos fijos **NUNCA** se eliminan
- Si un contrato fijo no está asignado, se asigna prioritariamente

### Limpieza de Asignaciones

Se eliminan de `contract_advisors`:
- Contratos con **0-60 días** de atraso
- De usuarios 45 y 81
- **EXCEPTO** los contratos fijos

### Asignación y Balanceo

Se asignan contratos con **>= 61 días** de atraso:

1. **Prioridad alta**: Contratos fijos no asignados
2. **Balanceo 50/50**: Distribución equitativa
3. **Números impares**: Alternancia para mantener equilibrio

## 🔄 Flujo de Ejecución

```
1. Adquirir File Lock (Singleton)
   ↓
2. Conectar a MySQL y PostgreSQL
   ↓
3. Consultar contratos fijos (managements)
   ↓
4. Consultar asignaciones actuales (contract_advisors)
   ↓
5. Obtener contratos con >= 61 días (MySQL)
   ↓
6. Limpieza: DELETE contratos 0-60 días (excepto fijos)
   ↓
7. Balanceo: Asignar contratos 50/50
   ↓
8. INSERT nuevas asignaciones (contract_advisors)
   ↓
9. Generar reportes TXT y Excel
   ↓
10. Liberar Lock
```

## ⚙️ Configuración Avanzada

### Modificar Parámetros de Negocio

Edita el archivo `.env`:

```env
# Cambiar días mínimos de atraso
DAYS_THRESHOLD=61

# Cambiar effect para contratos fijos
FIXED_CONTRACT_EFFECT=pago_total

# Cambiar directorio de reportes
REPORTS_DIR=reports
```

### Modificar Usuarios

Edita `app/core/config.py`:

```python
USER_IDS: List[int] = [45, 81]  # Cambiar IDs de usuarios
```

## 🐛 Troubleshooting

### Error: "Process already running"

Otra instancia está en ejecución. Verifica con:

```bash
GET /api/v1/lock-status
```

Si el proceso está bloqueado, elimina manualmente:

```bash
del assignment_process.lock
```

### Error de conexión a bases de datos

Verifica las credenciales en `.env` y la conectividad de red:

```bash
GET /api/v1/health
```

### Logs de ejecución

Los logs se guardan en:
- **Console**: STDOUT
- **Archivo**: `assignment_process.log`

## 🧪 Testing Manual

Usa curl o Postman:

```bash
# Ejecutar asignación
curl -X POST http://localhost:8000/api/v1/run-assignment

# Ver estado del lock
curl http://localhost:8000/api/v1/lock-status

# Health check
curl http://localhost:8000/api/v1/health
```

## 📝 Notas Técnicas

- **SQLAlchemy**: ORM para modelos de PostgreSQL
- **Raw SQL**: Queries directas para MySQL (solo lectura)
- **File Lock**: `filelock` library para garantizar singleton
- **Pandas + OpenPyXL**: Generación de reportes Excel
- **Context Managers**: Gestión automática de sesiones y transacciones

## 👨‍💻 Desarrollo

Para activar modo debug, edita `.env`:

```env
DEBUG=True
```

Esto habilitará:
- Auto-reload en cambios de código
- Logs detallados de queries SQL
- Stack traces completos

## 📦 Dependencias Principales

- **FastAPI**: Framework web moderno
- **SQLAlchemy**: ORM y gestión de bases de datos
- **Pydantic**: Validación de datos
- **Pandas**: Generación de reportes
- **Filelock**: Singleton pattern

## 🔐 Seguridad

⚠️ **IMPORTANTE**: Este código contiene credenciales de bases de datos. En producción:

1. Usa variables de entorno del sistema
2. Implementa secrets management (AWS Secrets Manager, Azure Key Vault)
3. No commitees el archivo `.env` al repositorio
4. Restringe acceso a los endpoints con autenticación

## 📞 Soporte

Para preguntas o problemas, consulta los logs en:
- `assignment_process.log`
- Console output

---

**Versión**: 1.0.0  
**Autor**: Senior Backend Developer  
**Stack**: Python 3.11+ | FastAPI | SQLAlchemy | MySQL | PostgreSQL#   C a s a _ C o b r a n z a  
 #   C a s a _ C o b r a n z a  
 