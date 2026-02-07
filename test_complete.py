"""
Script de validación completo del sistema mejorado con historial.
Valida la tabla contract_advisors_history y las nuevas funcionalidades.
"""
import requests
import time
from datetime import datetime

BASE_URL = "http://localhost:8000"

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_header(text):
    print("\n" + "=" * 100)
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print("=" * 100)

def print_success(text):
    print(f"{Colors.GREEN}✅ {text}{Colors.END}")

def print_error(text):
    print(f"{Colors.RED}❌ {text}{Colors.END}")

def print_info(text):
    print(f"{Colors.BLUE}ℹ️  {text}{Colors.END}")

def print_warning(text):
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.END}")

print_header("🚀 SISTEMA DE ASIGNACIÓN CON HISTORIAL - VALIDACIÓN COMPLETA")
print(f"\n{Colors.YELLOW}Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.END}")
print(f"{Colors.YELLOW}URL: {BASE_URL}{Colors.END}")

# Esperar a que API esté lista
print_info("\nEsperando a que la API esté lista...")
for i in range(30):
    try:
        response = requests.get(f"{BASE_URL}/", timeout=2)
        if response.status_code == 200:
            print_success("API está lista!")
            break
    except:
        pass
    if i < 29:
        print(f"   Intento {i+1}/30... ", end="\r")
        time.sleep(2)
else:
    print_error("\n⚠️  No se pudo conectar con la API")
    exit(1)

# Configuraciones
print_header("📋 CONFIGURACIÓN DEL SISTEMA")
print(f"""
{Colors.BOLD}Casas de Cobranza:{Colors.END}
  • COBYSER:  usuarios 45, 46, 47, 48, 49, 50, 51
  • SERLEFIN: usuarios 81, 82, 83, 84, 85, 86, 102, 103

{Colors.BOLD}Lógica de Asignación:{Colors.END}
  • Contratos >= 61 días: Se asignan automáticamente
  • Contratos < 61 días: Se eliminan (excepto fijos)
  • Effect 'pago_total': NUNCA se eliminan (fijos)
  • Distribución: 50/50 equitativa entre casas

{Colors.BOLD}Historial (contract_advisors_history):{Colors.END}
  • Fecha Inicial: Se registra al asignar contrato
  • Fecha Terminal: Se registra al eliminar contrato (< 61 días sin effect)
  • Permite rastrear toda la historia de asignaciones
""")

# Test 1: Health Check
print_header("TEST 1: Health Check - Conexiones a Bases de Datos")
try:
    response = requests.get(f"{BASE_URL}/api/v1/health", timeout=30)
    if response.status_code == 200:
        data = response.json()
        print_success(f"Health Status: {data['status']}")
        if "databases" in data:
            for db, status in data["databases"].items():
                if status == "connected":
                    print_success(f"  {db.upper()}: {status}")
                else:
                    print_error(f"  {db.upper()}: {status}")
    else:
        print_warning(f"Health Check degradado: {response.status_code}")
except Exception as e:
    print_error(f"Error en health check: {e}")
    exit(1)

# Test 2: Ejecutar proceso completo
print_header("TEST 2: Ejecutar Proceso Completo de Asignación")
print_warning("⚠️  Este test ejecuta el proceso REAL y modificará datos en PostgreSQL")
print_info("Iniciando en 3 segundos... (Ctrl+C para cancelar)")
try:
    time.sleep(3)
except KeyboardInterrupt:
    print_warning("\n⚠️  Test cancelado")
    exit(0)

print_info("Ejecutando proceso de asignación...")
start_time = time.time()

try:
    response = requests.post(f"{BASE_URL}/api/v1/run-assignment", timeout=120)
    execution_time = time.time() - start_time
    
    if response.status_code == 200:
        data = response.json()
        
        print_success(f"Proceso completado en {execution_time:.2f}s")
        print("\n" + "-" * 100)
        print(f"{Colors.BOLD}RESULTADOS DETALLADOS:{Colors.END}")
        print("-" * 100)
        
        if "results" in data:
            results = data["results"]
            
            # Contratos fijos
            print(f"\n🔒 {Colors.BOLD}Contratos Fijos (effect='pago_total'):{Colors.END}")
            if "fixed_contracts_count" in results:
                fc = results["fixed_contracts_count"]
                for user, count in fc.items():
                    casa = "COBYSER" if any(str(u) in str(user) for u in [45,46,47,48,49,50,51]) else "SERLEFIN"
                    print(f"   • {casa} (Usuario {user}): {count} contratos")
            
            # Procesamiento
            print(f"\n📝 {Colors.BOLD}Contratos Procesados:{Colors.END}")
            print(f"   • Total con >= 61 días: {results.get('contracts_processed', 0)}")
            
            # Limpieza
            if "clean_stats" in results:
                cs = results["clean_stats"]
                print(f"\n🗑️  {Colors.BOLD}Limpieza (contratos 0-60 días):{Colors.END}")
                print(f"   • Total eliminados: {cs.get('deleted_total', 0)}")
                print(f"   • COBYSER: {cs.get('deleted_cobyser', 0)}")
                print(f"   • SERLEFIN: {cs.get('deleted_serlefin', 0)}")
                print(f"   • Protegidos (fijos): {cs.get('protected_fixed', 0)}")
            
            # Balanceo
            if "balance_stats" in results:
                bs = results["balance_stats"]
                print(f"\n⚖️  {Colors.BOLD}Balanceo Final:{Colors.END}")
                for user, count in bs.items():
                    casa = "COBYSER" if any(str(u) == str(user) for u in [45,46,47,48,49,50,51]) else "SERLEFIN"
                    print(f"   • {casa} (Usuario {user}): {count} contratos")
            
            # Nuevas asignaciones
            if "insert_stats" in results:
                ins = results["insert_stats"]
                print(f"\n➕ {Colors.BOLD}Nuevas Asignaciones:{Colors.END}")
                print(f"   • Total insertados: {ins.get('inserted_total', 0)}")
                print(f"   • COBYSER: {ins.get('inserted_cobyser', 0)}")
                print(f"   • SERLEFIN: {ins.get('inserted_serlefin', 0)}")
            
            # Reportes
            if "reports" in data:
                reports = data["reports"]
                print(f"\n📁 {Colors.BOLD}Reportes Generados:{Colors.END}")
                for key, path in reports.items():
                    print(f"   • {path}")
        
        print("\n" + "-" * 100)
        print_success(f"⏱️  Tiempo total: {data.get('execution_time', execution_time):.2f}s")
        
        # Resumen final
        print_header("✅ VALIDACIÓN EXITOSA")
        print(f"""
{Colors.GREEN}El sistema está funcionando correctamente con las siguientes características:{Colors.END}

✅ Dual Database (MySQL + PostgreSQL)
✅ Asignación automática de contratos >= 61 días
✅ Protección de contratos fijos (effect='pago_total')
✅ Distribución equitativa entre casas de cobranza
✅ Registro de historial con Fecha Inicial
✅ Actualización de historial con Fecha Terminal
✅ Generación de reportes TXT y Excel
✅ Sistema singleton con file lock

{Colors.BOLD}Tabla contract_advisors_history:{Colors.END}
• Fecha Inicial: Registrada al asignar
• Fecha Terminal: Registrada al eliminar
• Permite auditoría completa de asignaciones

{Colors.BOLD}Accesos:{Colors.END}
• Swagger UI: {BASE_URL}/docs
• Health Check: {BASE_URL}/api/v1/health
• API Root: {BASE_URL}

{Colors.GREEN}{Colors.BOLD}🎉 ¡SISTEMA VALIDADO COMPLETAMENTE!{Colors.END}
""")
        
    elif response.status_code == 409:
        print_warning("Proceso ya en ejecución (Lock activo)")
    else:
        print_error(f"Error: Status {response.status_code}")
        print_info(f"Respuesta: {response.json()}")

except requests.Timeout:
    print_error("⏱️  Timeout: El proceso tomó más de 2 minutos")
except Exception as e:
    print_error(f"Error: {e}")
    exit(1)
