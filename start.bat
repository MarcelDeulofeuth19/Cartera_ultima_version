@echo off
echo ========================================
echo Sistema de Asignacion de Contratos
echo FastAPI - Inicio Rapido
echo ========================================
echo.

REM Verificar si existe el entorno virtual
if not exist "venv\" (
    echo [1/4] Creando entorno virtual...
    python -m venv venv
    echo       OK - Entorno virtual creado
) else (
    echo [1/4] Entorno virtual ya existe
)

echo.
echo [2/4] Activando entorno virtual...
call venv\Scripts\activate.bat

echo.
echo [3/4] Instalando/Actualizando dependencias...
pip install -r requirements.txt --quiet

echo.
echo [4/4] Verificando archivo .env...
if not exist ".env" (
    echo       ADVERTENCIA: No existe .env, copiando desde .env.example
    copy .env.example .env
) else (
    echo       OK - Archivo .env encontrado
)

echo.
echo ========================================
echo Todo listo! Iniciando servidor...
echo ========================================
echo.
echo Documentacion: http://localhost:8000/docs
echo Health Check:  http://localhost:8000/api/v1/health
echo.
echo Presiona Ctrl+C para detener el servidor
echo ========================================
echo.

REM Iniciar el servidor
python main.py
