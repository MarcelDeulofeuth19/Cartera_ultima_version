import os
import hmac
import hashlib
import requests
import json

# ==========================================
# EJEMPLO DE CLIENTE PARA CONSUMIR LA API
# CON SEGURIDAD HMAC-SHA256
# ==========================================

# Tu secreto de HMAC (debe coincidir con API_HMAC_SECRET en el .env del servidor)
SECRET_KEY = os.environ.get("API_HMAC_SECRET", "CHANGE_ME")

# URL base del aplicativo
BASE_URL = "http://localhost:8000"

def generate_hmac_signature(method: str, path: str, body: bytes = b"") -> str:
    """
    Genera la firma HMAC SHA256 requerida por el servidor.
    Se concatena: METHOD + PATH + BODY
    """
    payload = method.encode('utf-8') + path.encode('utf-8') + body
    
    signature = hmac.new(
        SECRET_KEY.encode('utf-8'),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return signature

def download_report(house_key: str):
    """
    Ejemplo de peticion GET para descargar un reporte Excel.
    """
    path = f"/api/v1/reports/download/{house_key}"
    url = f"{BASE_URL}{path}"
    
    # 1. Generamos la firma para GET (sin body)
    signature = generate_hmac_signature("GET", path)
    
    # 2. Agregamos la firma al header
    headers = {
        "X-Signature": signature
    }
    
    print(f"Descargando reporte de {house_key}...")
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        filename = f"reporte_descargado_{house_key}.xlsx"
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"¡Exito! Archivo guardado como: {filename}")
    else:
        print(f"Error {response.status_code}: {response.text}")

def get_current_assignments():
    """
    Ejemplo de peticion GET para obtener el JSON cacheado de asignaciones.
    """
    path = "/api/v1/reports/assignments/current"
    url = f"{BASE_URL}{path}"
    
    signature = generate_hmac_signature("GET", path)
    headers = {
        "X-Signature": signature
    }
    
    print("Obteniendo asignaciones actuales...")
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        print("¡Exito! Datos obtenidos:")
        print(json.dumps(data, indent=2)[:500] + "\n... (truncado)")
    else:
        print(f"Error {response.status_code}: {response.text}")

if __name__ == "__main__":
    # Puedes probar ambos endpoints:
    get_current_assignments()
    
    print("\n" + "-"*50 + "\n")
    
    # download_report("serlefin")
