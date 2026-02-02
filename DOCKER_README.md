# 🐳 Docker - Sistema de Asignación de Contratos

## 📦 Contenido Docker

Este proyecto incluye:
- **Dockerfile**: Imagen multi-stage optimizada para producción
- **docker-compose.yml**: Orquestación con puerto único (8000)
- **test_api.py**: Suite completa de tests
- **docker-test.bat**: Script automatizado de build + test

---

## 🚀 Inicio Rápido

### Opción 1: Script Automatizado (Recomendado)

```bash
docker-test.bat
```

Este script hace TODO automáticamente:
1. ✅ Limpia contenedores previos
2. ✅ Build de la imagen Docker
3. ✅ Inicia el contenedor
4. ✅ Ejecuta todos los tests
5. ✅ Muestra resultados

### Opción 2: Comandos Manuales

```bash
# Build
docker-compose build

# Iniciar
docker-compose up -d

# Ver logs
docker-compose logs -f

# Tests
python test_api.py

# Detener
docker-compose down
```

---

## 🌐 Accesos

Una vez iniciado el contenedor:

- **Swagger UI**: http://localhost:8000/docs
- **API Root**: http://localhost:8000
- **Health Check**: http://localhost:8000/api/v1/health
- **ReDoc**: http://localhost:8000/redoc

---

## 🧪 Tests Incluidos

El archivo `test_api.py` ejecuta 6 tests:

1. ✅ **API Root**: Verifica endpoint principal
2. ✅ **Health Check**: Valida conexiones a MySQL y PostgreSQL
3. ✅ **Lock Status**: Verifica sistema singleton
4. ✅ **Swagger Docs**: Comprueba documentación
5. ✅ **Assignment Process**: Ejecuta proceso REAL completo
6. ✅ **Singleton Protection**: Valida no concurrencia

---

## 📊 Características Docker

### Multi-Stage Build

```dockerfile
Stage 1 (Builder): Instala dependencias
Stage 2 (Production): Copia solo necesario
Resultado: Imagen optimizada y ligera
```

### Seguridad

- ✅ Usuario no-root (appuser)
- ✅ Permisos mínimos necesarios
- ✅ Variables de entorno externalizadas
- ✅ Health checks configurados

### Volúmenes Persistentes

```yaml
volumes:
  - ./reports:/app/reports  # Reportes generados
  - ./logs:/app/logs        # Logs de aplicación
```

---

## 🔧 Comandos Docker Útiles

```bash
# Ver estado
docker-compose ps

# Ver logs en tiempo real
docker-compose logs -f fastapi-app

# Reiniciar
docker-compose restart

# Reconstruir imagen
docker-compose build --no-cache

# Detener y limpiar
docker-compose down -v

# Ejecutar comando dentro del contenedor
docker-compose exec fastapi-app bash

# Ver uso de recursos
docker stats asignacion-contratos-api
```

---

## 🛠️ Troubleshooting Docker

### Error: Puerto 8000 en uso

```bash
# Windows
netstat -ano | findstr :8000
taskkill /PID [PID] /F

# Cambiar puerto en docker-compose.yml
ports:
  - "8001:8000"  # Mapear a puerto 8001
```

### Error: No se puede conectar a bases de datos

Verifica conectividad desde el contenedor:

```bash
docker-compose exec fastapi-app curl http://localhost:8000/api/v1/health
```

### Error: Build falla

Limpiar cache y rebuildar:

```bash
docker system prune -a
docker-compose build --no-cache
```

### Ver logs detallados

```bash
# Todos los logs
docker-compose logs

# Últimas 100 líneas
docker-compose logs --tail=100

# Logs en tiempo real
docker-compose logs -f
```

---

## 📋 Variables de Entorno

Configuradas en `docker-compose.yml`:

```yaml
environment:
  - MYSQL_HOST=57.130.40.1
  - MYSQL_USER=alo_estadisticas
  - POSTGRES_HOST=3.95.195.63
  - DAYS_THRESHOLD=61
  # etc...
```

Para cambiar en producción, usa archivo `.env`:

```bash
# Crear .env
MYSQL_HOST=tu-servidor-mysql.com
POSTGRES_HOST=tu-servidor-postgres.com
```

---

## 🚀 Deployment Producción

### Con Docker Compose

```bash
# Producción con replicas
docker-compose up -d --scale fastapi-app=3
```

### Con Docker Swarm

```bash
docker stack deploy -c docker-compose.yml asignacion-stack
```

### Con Kubernetes

Genera manifiesto:

```bash
kompose convert -f docker-compose.yml
```

---

## 📈 Monitoreo

### Health Check Automático

Docker verifica salud cada 30s:

```bash
# Ver estado de salud
docker inspect asignacion-contratos-api | grep -A 5 Health
```

### Prometheus + Grafana (Opcional)

Agrega a `docker-compose.yml`:

```yaml
prometheus:
  image: prom/prometheus
  ports:
    - "9090:9090"

grafana:
  image: grafana/grafana
  ports:
    - "3000:3000"
```

---

## 🔐 Seguridad en Producción

1. **Usar secrets en lugar de variables**:
   ```yaml
   secrets:
     - db_password
   ```

2. **Escanear imagen por vulnerabilidades**:
   ```bash
   docker scan asignacion-contratos-api
   ```

3. **Actualizar base image regularmente**:
   ```bash
   docker pull python:3.11-slim
   docker-compose build --no-cache
   ```

---

## 📦 Tamaño de Imagen

```bash
# Ver tamaño
docker images | grep asignacion

# Optimizar limpiando layers
docker image prune -a
```

Imagen optimizada: ~300-400 MB

---

## ✅ Checklist Pre-Producción

- [ ] Tests pasando (ejecutar `docker-test.bat`)
- [ ] Health checks configurados
- [ ] Variables de entorno externalizadas
- [ ] Volúmenes para persistencia
- [ ] Logs configurados
- [ ] Resources limits definidos
- [ ] Backup strategy definida
- [ ] Monitoring configurado
- [ ] Documentación actualizada

---

**Puerto Único**: 8000 (Swagger UI + API)
**Docker Image**: Python 3.11-slim
**Architecture**: Multi-stage optimized
