@echo off
REM ========================================
REM Script de Build y Test - Docker
REM Sistema de Asignacion de Contratos
REM ========================================

echo.
echo ================================================================================
echo   DOCKERIZACION Y TEST - SISTEMA DE ASIGNACION DE CONTRATOS
echo ================================================================================
echo.

REM Paso 1: Limpiar contenedores previos
echo [1/5] Limpiando contenedores previos...
docker-compose down 2>nul
echo       OK - Limpieza completada
echo.

REM Paso 2: Build de la imagen Docker
echo [2/5] Construyendo imagen Docker...
echo       Esto puede tardar unos minutos la primera vez...
docker-compose build
if errorlevel 1 (
    echo       ERROR - Fallo en el build de Docker
    pause
    exit /b 1
)
echo       OK - Imagen construida exitosamente
echo.

REM Paso 3: Iniciar contenedores
echo [3/5] Iniciando contenedores...
docker-compose up -d
if errorlevel 1 (
    echo       ERROR - Fallo al iniciar contenedores
    pause
    exit /b 1
)
echo       OK - Contenedor iniciado
echo.

REM Paso 4: Esperar a que la API este lista
echo [4/5] Esperando a que la API este lista...
timeout /t 10 /nobreak >nul
echo       OK - API deberia estar lista
echo.

REM Paso 5: Ejecutar tests
echo [5/5] Ejecutando suite de tests...
echo.
echo ================================================================================
python test_api.py
set TEST_RESULT=%errorlevel%
echo ================================================================================
echo.

REM Mostrar logs si hay error
if not %TEST_RESULT%==0 (
    echo.
    echo LOGS DEL CONTENEDOR:
    echo ================================================================================
    docker-compose logs --tail=50
    echo ================================================================================
)

REM Resumen final
echo.
echo ================================================================================
echo   RESUMEN FINAL
echo ================================================================================
echo.
docker-compose ps
echo.

if %TEST_RESULT%==0 (
    echo [OK] Todos los tests pasaron exitosamente!
    echo.
    echo Accesos:
    echo   - API:        http://localhost:8000
    echo   - Swagger UI: http://localhost:8000/docs
    echo   - Health:     http://localhost:8000/api/v1/health
    echo.
    echo Comandos utiles:
    echo   - Ver logs:        docker-compose logs -f
    echo   - Detener:         docker-compose down
    echo   - Reiniciar:       docker-compose restart
    echo.
) else (
    echo [ERROR] Algunos tests fallaron. Revisa los logs arriba.
    echo.
    echo Para ver logs completos: docker-compose logs
    echo.
)

echo ================================================================================
echo.
echo Presiona cualquier tecla para salir...
pause >nul
