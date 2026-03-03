# ðŸ³ Docker - Sistema de AsignaciÃ³n de Contratos

## ðŸ“¦ Contenido Docker

Este proyecto incluye:
- **Dockerfile**: Imagen multi-stage optimizada para producciÃ³n
- **docker-compose.yml**: OrquestaciÃ³n con puerto Ãºnico (8000)
- **test_api.py**: Suite completa de tests
- **docker-test.bat**: Script automatizado de build + test

---

## ðŸš€ Inicio RÃ¡pido

### OpciÃ³n 1: Script Automatizado (Recomendado)

```bash
docker-test.bat
```

Este script hace TODO automÃ¡ticamente:
1. âœ… Limpia contenedores previos
2. âœ… Build de la imagen Docker
3. âœ… Inicia el contenedor
4. âœ… Ejecuta todos los tests
5. âœ… Muestra resultados

### OpciÃ³n 2: Comandos Manuales

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

## ðŸŒ Accesos

Una vez iniciado el contenedor:

- **Swagger UI**: http://localhost:8000/docs
- **API Root**: http://localhost:8000
- **Health Check**: http://localhost:8000/api/v1/health
- **ReDoc**: http://localhost:8000/redoc

---

## ðŸ§ª Tests Incluidos

El archivo `test_api.py` ejecuta 6 tests:

1. âœ… **API Root**: Verifica endpoint principal
2. âœ… **Health Check**: Valida conexiones a MySQL y PostgreSQL
3. âœ… **Lock Status**: Verifica sistema singleton
4. âœ… **Swagger Docs**: Comprueba documentaciÃ³n
5. âœ… **Assignment Process**: Ejecuta proceso REAL completo
6. âœ… **Singleton Protection**: Valida no concurrencia

---

## ðŸ“Š CaracterÃ­sticas Docker

### Multi-Stage Build

```dockerfile
Stage 1 (Builder): Instala dependencias
Stage 2 (Production): Copia solo necesario
Resultado: Imagen optimizada y ligera
```

### Seguridad

- âœ… Usuario no-root (appuser)
- âœ… Permisos mÃ­nimos necesarios
- âœ… Variables de entorno externalizadas
- âœ… Health checks configurados

### VolÃºmenes Persistentes

```yaml
volumes:
  - ./reports:/app/reports  # Reportes generados
  - ./logs:/app/logs        # Logs de aplicaciÃ³n
  - ./docker-data/internal-config-db:/var/lib/postgresql/data  # DB interna persistente
```

### Persistencia de Base Interna (IMPORTANTE)

La base interna `internal-config-db` usa bind mount en `./docker-data/internal-config-db`.

Esto permite que los datos de configuraciÃ³n/auditorÃ­a/login del panel sobrevivan:
- `docker compose down`
- `docker system prune`
- rebuild de imÃ¡genes

Solo se pierden si borras manualmente `./docker-data/internal-config-db`.

---

## ðŸ”§ Comandos Docker Ãštiles

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

Nota: evita `docker system prune --volumes` si usas volÃºmenes Docker tradicionales.
En este proyecto la DB interna persiste en carpeta host (`./docker-data/...`) para mitigar ese riesgo.

---

## ðŸ› ï¸ Troubleshooting Docker

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

# Ãšltimas 100 lÃ­neas
docker-compose logs --tail=100

# Logs en tiempo real
docker-compose logs -f
```

---

## ðŸ“‹ Variables de Entorno

Configuradas en `docker-compose.yml`:

```yaml
environment:
  - MYSQL_HOST=57.130.40.1
  - MYSQL_USER=alo_estadisticas
  - POSTGRES_HOST=3.95.195.63
  - DAYS_THRESHOLD=61
  # etc...
```

Para cambiar en producciÃ³n, usa archivo `.env`:

```bash
# Crear .env
MYSQL_HOST=tu-servidor-mysql.com
POSTGRES_HOST=tu-servidor-postgres.com
```

---

## ðŸš€ Deployment ProducciÃ³n

### Con Docker Compose

```bash
# ProducciÃ³n con replicas
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

## ðŸ“ˆ Monitoreo

### Health Check AutomÃ¡tico

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

## ðŸ” Seguridad en ProducciÃ³n

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

## ðŸ“¦ TamaÃ±o de Imagen

```bash
# Ver tamaÃ±o
docker images | grep asignacion

# Optimizar limpiando layers
docker image prune -a
```

Imagen optimizada: ~300-400 MB

---

## âœ… Checklist Pre-ProducciÃ³n

- [ ] Tests pasando (ejecutar `docker-test.bat`)
- [ ] Health checks configurados
- [ ] Variables de entorno externalizadas
- [ ] VolÃºmenes para persistencia
- [ ] Logs configurados
- [ ] Resources limits definidos
- [ ] Backup strategy definida
- [ ] Monitoring configurado
- [ ] DocumentaciÃ³n actualizada

---

**Puerto Ãšnico**: 8000 (Swagger UI + API)
**Docker Image**: Python 3.11-slim
**Architecture**: Multi-stage optimized
