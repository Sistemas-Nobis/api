# config.py
import json
from fastapi import FastAPI, Depends
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
import requests

PASSWORD_FILE = "password.json"

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