"""
Script para insertar el 100% de los contratos fijos manuales.
Ejecuta el endpoint que procesa e inserta todos los contratos definidos:
- Cobyser: 79 contratos
- Serlefin: 415 contratos
- Total: 494 contratos fijos

El endpoint valida automáticamente contra BD y evita duplicados.
"""

import requests
import json
from datetime import datetime

# Configuración
API_URL = "http://localhost:8000/api/v1/process-manual-fixed"

def insert_manual_fixed_contracts():
    """Ejecuta el endpoint para insertar contratos fijos manuales."""
    
    print("=" * 100)
    print("INSERTANDO 100% DE CONTRATOS FIJOS MANUALES")
    print("=" * 100)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Endpoint: {API_URL}")
    print("\nContratos a procesar:")
    print("  - Cobyser (Usuario 45): 79 contratos")
    print("  - Serlefin (Usuario 81): 415 contratos")
    print("  - TOTAL: 494 contratos fijos")
    print("\n" + "=" * 100)
    
    try:
        print("\n⏳ Enviando solicitud al endpoint...")
        print("   (Este proceso puede tomar varios segundos dependiendo de la cantidad de contratos)")
        
        response = requests.post(API_URL, timeout=300)  # 5 minutos de timeout
        
        if response.status_code == 200:
            data = response.json()
            
            print("\n" + "=" * 100)
            print("✅ PROCESAMIENTO EXITOSO")
            print("=" * 100)
            
            print(f"\n📊 RESULTADOS:")
            print(f"  Tiempo de ejecución: {data.get('execution_time', 0):.2f} segundos")
            print(f"  Mensaje: {data.get('message', '')}")
            
            if 'results' in data:
                results = data['results']
                print(f"\n📈 ESTADÍSTICAS:")
                print(f"  Total proporcionados: {results.get('total_provided', 0)}")
                print(f"  Ya asignados (omitidos): {results.get('already_assigned', 0)}")
                print(f"  En managements: {results.get('in_managements', 0)}")
                print(f"  ✓ INSERTADOS: {results.get('inserted', 0)}")
                
                if 'by_user' in results:
                    print(f"\n👥 POR USUARIO:")
                    for user_id, user_stats in results['by_user'].items():
                        print(f"  Usuario {user_id}:")
                        print(f"    - Proporcionados: {user_stats.get('provided', 0)}")
                        print(f"    - Insertados: {user_stats.get('inserted', 0)}")
                        print(f"    - Omitidos: {user_stats.get('skipped', 0)}")
            
            print("\n" + "=" * 100)
            
            # Guardar resultado en archivo
            report_file = f"reports/insert_fixed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            try:
                with open(report_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"📄 Reporte guardado en: {report_file}")
            except Exception as e:
                print(f"⚠️  No se pudo guardar el reporte: {e}")
            
            return True
        
        elif response.status_code == 409:
            print("\n" + "=" * 100)
            print("⚠️  PROCESO YA EN EJECUCIÓN")
            print("=" * 100)
            print("\nOtra instancia del proceso está en ejecución.")
            print("Por favor, espera a que termine antes de ejecutar nuevamente.")
            return False
        
        else:
            print("\n" + "=" * 100)
            print(f"❌ ERROR HTTP {response.status_code}")
            print("=" * 100)
            print(f"Respuesta: {response.text}")
            return False
    
    except requests.exceptions.Timeout:
        print("\n" + "=" * 100)
        print("❌ TIMEOUT")
        print("=" * 100)
        print("La solicitud excedió el tiempo de espera (5 minutos).")
        print("El proceso puede estar tardando más de lo esperado.")
        print("Verifica el estado de la API y los logs del servidor.")
        return False
    
    except requests.exceptions.ConnectionError:
        print("\n" + "=" * 100)
        print("❌ ERROR DE CONEXIÓN")
        print("=" * 100)
        print(f"No se pudo conectar a la API en {API_URL}")
        print("\nVerifica que:")
        print("  1. El servidor FastAPI esté ejecutándose")
        print("  2. El puerto 8000 esté disponible")
        print("  3. Docker Compose esté activo (si aplica)")
        print("\nComandos útiles:")
        print("  - Docker: docker compose up -d")
        print("  - Local: python main.py")
        return False
    
    except Exception as e:
        print("\n" + "=" * 100)
        print("❌ ERROR INESPERADO")
        print("=" * 100)
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = insert_manual_fixed_contracts()
    exit(0 if success else 1)
