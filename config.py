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


def load_password_admin():
    try:
        with open(PASSWORD_FILE, 'r') as f:
            data = json.load(f)
            return data.get("admin")
    except FileNotFoundError:
        return None


def update_password(new_password):
    """Actualizar solo la clave 'password' en el archivo JSON."""
    data = {}
    with open(PASSWORD_FILE, 'r') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            pass  # si el archivo está vacío o mal formado, lo reiniciamos

    data["password"] = new_password

    with open(PASSWORD_FILE, 'w') as f:
        json.dump(data, f, indent=4)


def update_password_admin(new_password):
    data = {}
    with open(PASSWORD_FILE, 'r') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            pass

    data["admin"] = new_password

    with open(PASSWORD_FILE, 'w') as f:
        json.dump(data, f, indent=4)


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
    

# Función para actualizar y obtener el token desde la API de GECROS
@cache(expire=3000)  # Establece el tiempo de expiración solo aquí
async def actualizar_token_wise():
    """Obtiene un nuevo token desde la API y lo guarda."""
    url_token = 'https://api.wcx.cloud/core/v1/authenticate'
    query_params = {'user': 'apinobis'}
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': 'be9dd08a9cd8422a9af1372a445ec8e4',
    }
    try:
        response = requests.get(url_token, headers=headers, params=query_params)
        response.raise_for_status()  # Lanza un error si la solicitud falla
       
        # Imprimir la respuesta completa para verificar la estructura
        #print(f"Respuesta de la API: {response.text}")
       
        # Suponiendo que el token está en la clave 'token'
        response_json = response.json()
        token = response_json.get('token', None)
       
        if token:
            #print(f"Nuevo token obtenido y guardado: {token}")
            return token
        else:
            raise Exception("No se encontró el token en la respuesta.")
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener el token: {e}")