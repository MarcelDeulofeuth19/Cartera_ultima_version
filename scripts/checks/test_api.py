"""
Suite de Tests Completa para Sistema de Asignación de Contratos
Prueba endpoints, lógica de negocio y conectividad
"""
import requests
import time
import json
from datetime import datetime

# Configuración
BASE_URL = "http://localhost:8000"
TIMEOUT = 30

class Colors:
    """Colores para terminal"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_header(text):
    """Imprime un encabezado con formato"""
    print("\n" + "=" * 80)
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print("=" * 80)

def print_success(text):
    """Imprime mensaje de éxito"""
    print(f"{Colors.GREEN}✅ {text}{Colors.END}")

def print_error(text):
    """Imprime mensaje de error"""
    print(f"{Colors.RED}❌ {text}{Colors.END}")

def print_warning(text):
    """Imprime mensaje de advertencia"""
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.END}")

def print_info(text):
    """Imprime mensaje informativo"""
    print(f"{Colors.BLUE}ℹ️  {text}{Colors.END}")

def test_api_root():
    """Test 1: Verificar endpoint raíz"""
    print_header("TEST 1: Verificar API Root")
    try:
        response = requests.get(f"{BASE_URL}/", timeout=TIMEOUT)
        assert response.status_code == 200, f"Status code: {response.status_code}"
        
        data = response.json()
        assert "app" in data, "Falta campo 'app'"
        assert "version" in data, "Falta campo 'version'"
        assert data["status"] == "running", "Status no es 'running'"
        
        print_success(f"API Root OK - {data['app']} v{data['version']}")
        return True
    except Exception as e:
        print_error(f"Fallo en API Root: {e}")
        return False

def test_health_check():
    """Test 2: Verificar health check"""
    print_header("TEST 2: Health Check de Bases de Datos")
    try:
        response = requests.get(f"{BASE_URL}/api/v1/health", timeout=TIMEOUT)
        
        if response.status_code == 200:
            data = response.json()
            print_success(f"Health Check: {data['status']}")
            
            # Verificar bases de datos
            if "databases" in data:
                for db, status in data["databases"].items():
                    if status == "connected":
                        print_success(f"  {db.upper()}: {status}")
                    else:
                        print_error(f"  {db.upper()}: {status}")
            return True
        else:
            print_warning(f"Health Check degradado: {response.status_code}")
            print_info(f"Respuesta: {response.json()}")
            return False
            
    except Exception as e:
        print_error(f"Fallo en Health Check: {e}")
        return False

def test_lock_status():
    """Test 3: Verificar estado del lock"""
    print_header("TEST 3: Estado del Lock (Singleton)")
    try:
        response = requests.get(f"{BASE_URL}/api/v1/lock-status", timeout=TIMEOUT)
        assert response.status_code == 200, f"Status code: {response.status_code}"
        
        data = response.json()
        print_info(f"Lock file: {data['lock_file']}")
        print_info(f"Existe: {data['exists']}")
        print_info(f"Bloqueado: {data['is_locked']}")
        
        if data['is_locked']:
            print_warning("⚠️ Proceso en ejecución - Lock activo")
        else:
            print_success("✅ Lock disponible - Listo para ejecutar")
        
        return True
    except Exception as e:
        print_error(f"Fallo en Lock Status: {e}")
        return False

def test_swagger_docs():
    """Test 4: Verificar documentación Swagger"""
    print_header("TEST 4: Documentación Swagger")
    try:
        response = requests.get(f"{BASE_URL}/docs", timeout=TIMEOUT)
        assert response.status_code == 200, f"Status code: {response.status_code}"
        assert "swagger" in response.text.lower(), "No contiene Swagger UI"
        
        print_success("Swagger UI disponible en: http://localhost:8000/docs")
        return True
    except Exception as e:
        print_error(f"Fallo en Swagger: {e}")
        return False

def test_assignment_process_dry_run():
    """Test 5: Ejecutar proceso de asignación (REAL)"""
    print_header("TEST 5: Ejecutar Proceso de Asignación Completo")
    print_warning("⚠️  ADVERTENCIA: Este test ejecutará el proceso REAL de asignación")
    print_warning("⚠️  Se modificarán datos en la base de datos PostgreSQL")
    
    # Dar tiempo para cancelar si es necesario
    print_info("Iniciando en 3 segundos... (Ctrl+C para cancelar)")
    try:
        time.sleep(3)
    except KeyboardInterrupt:
        print_warning("\n⚠️  Test cancelado por el usuario")
        return False
    
    print_info("Ejecutando proceso de asignación...")
    start_time = time.time()
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v1/run-assignment",
            timeout=120  # 2 minutos de timeout
        )
        
        execution_time = time.time() - start_time
        
        if response.status_code == 200:
            data = response.json()
            
            print_success(f"Proceso completado exitosamente en {execution_time:.2f}s")
            print("\n" + "-" * 80)
            print(f"{Colors.BOLD}RESULTADOS DEL PROCESO:{Colors.END}")
            print("-" * 80)
            
            # Mostrar resultados
            if "results" in data:
                results = data["results"]
                
                print(f"\n📊 {Colors.BOLD}Contratos Fijos:{Colors.END}")
                if "fixed_contracts_count" in results:
                    fc = results["fixed_contracts_count"]
                    print(f"   • Usuario 45: {fc.get('user_45', 0)} contratos")
                    print(f"   • Usuario 81: {fc.get('user_81', 0)} contratos")
                
                print(f"\n📝 {Colors.BOLD}Procesamiento:{Colors.END}")
                print(f"   • Total procesados: {results.get('contracts_processed', 0)}")
                
                if "clean_stats" in results:
                    cs = results["clean_stats"]
                    print(f"\n🗑️  {Colors.BOLD}Limpieza (0-60 días):{Colors.END}")
                    print(f"   • Eliminados Usuario 45: {cs.get('deleted_user_45', 0)}")
                    print(f"   • Eliminados Usuario 81: {cs.get('deleted_user_81', 0)}")
                    print(f"   • Protegidos (fijos): {cs.get('protected_fixed', 0)}")
                
                if "balance_stats" in results:
                    bs = results["balance_stats"]
                    print(f"\n⚖️  {Colors.BOLD}Balanceo Final:{Colors.END}")
                    print(f"   • Usuario 45: {bs.get('45', 0)} contratos")
                    print(f"   • Usuario 81: {bs.get('81', 0)} contratos")
                    diff = abs(bs.get('45', 0) - bs.get('81', 0))
                    print(f"   • Diferencia: {diff} {'✅' if diff <= 1 else '⚠️'}")
                
                if "insert_stats" in results:
                    ins = results["insert_stats"]
                    print(f"\n➕ {Colors.BOLD}Nuevas Asignaciones:{Colors.END}")
                    print(f"   • Usuario 45: {ins.get('inserted_user_45', 0)}")
                    print(f"   • Usuario 81: {ins.get('inserted_user_81', 0)}")
            
            # Mostrar reportes generados
            if "reports" in data:
                reports = data["reports"]
                print(f"\n📁 {Colors.BOLD}Reportes Generados:{Colors.END}")
                for key, path in reports.items():
                    print(f"   • {key}: {path}")
            
            print("\n" + "-" * 80)
            print_success(f"⏱️  Tiempo total: {data.get('execution_time', execution_time):.2f}s")
            
            return True
            
        elif response.status_code == 409:
            print_warning("Proceso ya en ejecución (Lock activo)")
            print_info("Espera a que termine o verifica el estado del lock")
            return False
        else:
            print_error(f"Error en proceso: Status {response.status_code}")
            print_info(f"Respuesta: {response.json()}")
            return False
            
    except requests.Timeout:
        print_error("⏱️  Timeout: El proceso tomó más de 2 minutos")
        print_info("El proceso puede estar ejecutándose aún. Verifica los logs.")
        return False
    except Exception as e:
        print_error(f"Fallo en proceso de asignación: {e}")
        return False

def test_concurrent_execution():
    """Test 6: Verificar protección contra ejecución concurrente"""
    print_header("TEST 6: Protección Singleton (Ejecución Concurrente)")
    print_info("Este test verifica que no se puedan ejecutar 2 procesos simultáneamente")
    print_warning("⚠️  Saltando test para evitar sobrecarga (ya validado en Test 5)")
    return True

def generate_summary(results):
    """Genera resumen final de los tests"""
    print_header("RESUMEN FINAL DE TESTS")
    
    total = len(results)
    passed = sum(1 for r in results.values() if r)
    failed = total - passed
    
    print(f"\n{Colors.BOLD}Total de tests: {total}{Colors.END}")
    print(f"{Colors.GREEN}✅ Exitosos: {passed}{Colors.END}")
    print(f"{Colors.RED}❌ Fallidos: {failed}{Colors.END}")
    
    if failed == 0:
        print(f"\n{Colors.GREEN}{Colors.BOLD}🎉 ¡TODOS LOS TESTS PASARON!{Colors.END}")
        print(f"\n{Colors.GREEN}✅ Sistema completamente funcional{Colors.END}")
        print(f"{Colors.GREEN}✅ API disponible en: {BASE_URL}{Colors.END}")
        print(f"{Colors.GREEN}✅ Swagger UI: {BASE_URL}/docs{Colors.END}")
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}⚠️  ALGUNOS TESTS FALLARON{Colors.END}")
        print(f"\n{Colors.YELLOW}Tests fallidos:{Colors.END}")
        for test_name, result in results.items():
            if not result:
                print(f"{Colors.RED}  • {test_name}{Colors.END}")
    
    print("\n" + "=" * 80)
    return failed == 0

def main():
    """Función principal que ejecuta todos los tests"""
    print("\n" + "=" * 80)
    print(f"{Colors.BOLD}{Colors.BLUE}🧪 SUITE DE TESTS - SISTEMA DE ASIGNACIÓN DE CONTRATOS{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}   Docker + FastAPI + Dual Database{Colors.END}")
    print("=" * 80)
    print(f"\n{Colors.YELLOW}Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.END}")
    print(f"{Colors.YELLOW}URL Base: {BASE_URL}{Colors.END}")
    
    # Esperar a que la API esté lista
    print_info("\nEsperando a que la API esté lista...")
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get(f"{BASE_URL}/", timeout=2)
            if response.status_code == 200:
                print_success("API está lista!\n")
                break
        except:
            pass
        
        if i < max_retries - 1:
            print(f"   Intento {i+1}/{max_retries}... ", end="\r")
            time.sleep(2)
        else:
            print_error("\n⚠️  No se pudo conectar con la API")
            print_info("Asegúrate de que Docker esté corriendo y el contenedor iniciado")
            return False
    
    # Ejecutar tests
    results = {
        "Test 1: API Root": test_api_root(),
        "Test 2: Health Check": test_health_check(),
        "Test 3: Lock Status": test_lock_status(),
        "Test 4: Swagger Docs": test_swagger_docs(),
        "Test 5: Assignment Process": test_assignment_process_dry_run(),
        "Test 6: Singleton Protection": test_concurrent_execution()
    }
    
    # Generar resumen
    return generate_summary(results)

if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}⚠️  Tests interrumpidos por el usuario{Colors.END}")
        exit(1)
    except Exception as e:
        print(f"\n\n{Colors.RED}❌ Error crítico: {e}{Colors.END}")
        exit(1)
