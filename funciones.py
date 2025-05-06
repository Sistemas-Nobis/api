from config import actualizar_token_gecros, actualizar_token_wise

# Función que simplemente devuelve el token en caché sin reiniciar el tiempo de expiración
async def obtener_token_gecros():
    token = await actualizar_token_gecros()  # Llama a la función que lo obtiene de la caché
    return token

async def obtener_token_wise():
    # Verifica si el token está en el caché
    token = await actualizar_token_wise()
    return token