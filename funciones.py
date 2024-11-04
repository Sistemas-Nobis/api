from config import actualizar_token_gecros

# Función que simplemente devuelve el token en caché sin reiniciar el tiempo de expiración
async def obtener_token_gecros():
    token = await actualizar_token_gecros()  # Llama a la función que lo obtiene de la caché
    return token