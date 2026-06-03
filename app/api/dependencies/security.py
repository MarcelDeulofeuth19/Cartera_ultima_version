import hmac
import hashlib
from fastapi import Request, HTTPException, status
from app.core.config import settings

async def verify_hmac_signature(request: Request):
    """
    Verifica la firma HMAC de la petición usando API_HMAC_SECRET.
    Se espera que el cliente envíe el header 'X-Signature'.
    """
    signature_header = request.headers.get("X-Signature")
    if not signature_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Signature header"
        )
    
    # Obtener el body de la peticion para firmarlo (si hay)
    body = await request.body()
    
    # Generar la firma esperada usando el body (o un string vacio si es GET)
    # y el method + path como parte del payload
    payload = request.method.encode() + request.url.path.encode() + body
    
    secret = settings.API_HMAC_SECRET.encode()
    
    expected_signature = hmac.new(
        secret,
        payload,
        hashlib.sha256
    ).hexdigest()
    
    if not hmac.compare_digest(signature_header, expected_signature):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid signature"
        )
