# Dockerfile Multi-Stage para Sistema de AsignaciÃ³n de Contratos
# Optimizado para producciÃ³n

# ================================
# Stage 1: Builder
# ================================
FROM python:3.11-slim as builder

WORKDIR /app

# Instalar dependencias del sistema necesarias para compilaciÃ³n
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar solo requirements primero (cache de Docker)
COPY requirements.txt .

# Crear virtual environment e instalar dependencias
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ================================
# Stage 2: Production
# ================================
FROM python:3.11-slim

WORKDIR /app

# Instalar solo librerÃ­as runtime necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar virtual environment desde builder
COPY --from=builder /opt/venv /opt/venv

# Configurar PATH para usar el venv
ENV PATH="/opt/venv/bin:$PATH"

# Variables de entorno para Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Copiar cÃ³digo de la aplicaciÃ³n
COPY ./app /app/app
COPY ./main.py /app/
COPY ./admin_panel.py /app/
COPY ./.env.example /app/.env

# Crear directorio para reportes
RUN mkdir -p /app/reports && \
    chmod 777 /app/reports

# Crear usuario no-root para seguridad
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# Exponer puerto para FastAPI (Swagger)
EXPOSE 8000
EXPOSE 9007

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

# Comando para iniciar la aplicaciÃ³n
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

