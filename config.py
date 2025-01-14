# config.py
import json
from fastapi_cache.decorator import cache
import requests
from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Header, HTTPException

PASSWORD_FILE = "password.json"

SECRET_KEY = "XwKDmSfchJTSo2ulEJAVGFpgk1rGjXTCR6Duu8xdUCjEzwFjz7shVsBVAm9Sxu83"  # Clave secreta para firmar los tokens (¡NUNCA la expongas en tu código!)
ALGORITHM = "HS256"  # Algoritmo de encriptación
ACCESS_TOKEN_EXPIRE_MINUTES = 20160  # Duración del token en minutos

def load_password():
    """Cargar la contraseña desde un archivo JSON."""
    try:
        with open(PASSWORD_FILE, 'r') as f:
            data = json.load(f)
            return data.get("password")
    except FileNotFoundError:
        return None


def update_password(new_password):
    """Actualizar la contraseña en el archivo JSON."""
    with open(PASSWORD_FILE, 'w') as f:
        json.dump({"password": new_password}, f)


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Función para verificar una contraseña
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

# Función para hashear una contraseña
def get_password_hash(password):
    return pwd_context.hash(password)

# Función para generar un token JWT
def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return token

# Función para decodificar y validar un token
def decode_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


# Dependencia para verificar la secret key en el header
async def verify_secret_key(x_secret_key: str = Header(None)):
    if x_secret_key != SECRET_KEY:
        raise HTTPException(
            status_code=401,
            detail="No está autorizado.",
        )


# Función para actualizar y obtener el token desde la API de GECROS
@cache(expire=1296000)  # Establece el tiempo de expiración solo aquí
async def actualizar_token_gecros():
    url = "https://appmobile.nobissalud.com.ar/connect/token"
    payload = {
        'userName': '2|45899788',
        'password': 'widgetapi',
        'grant_type': 'password',
        'client_id': 'gecrosAppAfiliado'
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()  # Verifica si la solicitud fue exitosa
        response_json = response.json()
        
        token = response_json.get('access_token')
        if token:
            print(f"Nuevo token obtenido y guardado: {token}")
            return token
        else:
            raise Exception("No se encontró el token en la respuesta.")
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener el token: {e}")
        return None