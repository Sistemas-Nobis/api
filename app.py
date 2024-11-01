from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel
from config import load_password, update_password
from funciones import consulta_aportes, obtener_token_gecros
from fastapi.responses import StreamingResponse
import requests
from pandas import json_normalize
import io
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend

app = FastAPI(
    title="API NOBIS - TEST",  # Cambia el nombre de la pestaña
    description="Contraseñas y consultas.",
    version="1.0.0",
)

# Definir un modelo para la entrada de la nueva contraseña
class PasswordUpdate(BaseModel):
    password: str

# Configurar FastAPICache al crear la aplicación
FastAPICache.init(InMemoryBackend())

# Endpoint para obtener la contraseña actual
@app.get("/obtener_contrasena")
async def obtener_contraseña():
    password = load_password()
    if password:
        return {"Contraseña": password}
    raise HTTPException(status_code=404, detail="Contraseña no encontrada")


# Endpoint para actualizar la contraseña
@app.post("/actualizar_contrasena")
async def actualizar_contraseña(password_update: PasswordUpdate):
    new_password = password_update.password
    if not new_password:
        raise HTTPException(status_code=400, detail="No se ha proporcionado una nueva contraseña")
    
    # Actualizar la contraseña en el archivo
    update_password(new_password)
    return {"Mensaje": "Password updated successfully"}


# Endpoint con consultas de aportes para Widget de retención
@app.get("/ultimos_aportes")
async def ultimos_aportes(dni: int = Query(..., description="Número de DNI del beneficiario")):
    try:
        # Ejecuta la consulta y obtiene el DataFrame
        df = consulta_aportes(dni)
        
        # Convierte el DataFrame a JSON para la respuesta
        data_json = df.to_dict(orient="records")
        return {"data": data_json}
    
    except Exception as e:
        return {"error": str(e)}
    

# Endpoint con consultas de fecha de alta y patologias para Widget de retención
@app.get("/fecha_alta_y_patologias")
async def consulta_fecha_alta_y_patologias(dni: int = Query(..., description="Número de afiliado")):
    try:
        # Ejecuta la consulta y obtiene el DataFrame
        df = consulta_fecha_alta_y_patologias(dni)
        
        # Convierte el DataFrame a JSON para la respuesta
        data_json = df.to_dict(orient="records")
        return {"data": data_json}
    
    except Exception as e:
        return {"error": str(e)}


# Endpoint para descargar PDF de autorización
@app.get("/descargar_autorizacion/{dni}&{id_aut}")
async def descargar_autorizacion(dni: int, id_aut: int, token:str = Depends(obtener_token_gecros)):

    print(token)

    # Token y headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    # URL para obtener el ID de beneficiario
    url = f"https://appmobile.nobissalud.com.ar/api/afiliados?numero={dni}"
    response_afiliado = requests.get(url, headers=headers)
    if response_afiliado.status_code != 200:
        raise HTTPException(status_code=response_afiliado.status_code, detail="Error al obtener datos del afiliado")

    data_afiliado = response_afiliado.json().get('data', [])
    df_afiliado = json_normalize(data_afiliado)
    id_ben = df_afiliado.get("benId").iloc[0]

    # URL para obtener el PDF
    url_template = f"https://appmobile.nobissalud.com.ar/api/AppBenef/{id_ben}/Autorizaciones/{id_aut}/pdf"
    response_template = requests.get(url_template, headers=headers)

    # Verifica que la respuesta fue exitosa
    if response_template.status_code == 200:
        pdf_bytes = io.BytesIO(response_template.content)
        headers = {
            'Content-Disposition': f'attachment; filename="Autorizacion_{id_aut}.pdf"'
        }
        return StreamingResponse(pdf_bytes, media_type="application/pdf", headers=headers)
    else:
        raise HTTPException(status_code=response_template.status_code, detail="Error al descargar el PDF")


# Endpoint para descargar PDF de boleta (comprobante de deuda)
@app.get("/descargar_boleta/{id_ben}&{id_comp}")
async def descargar_boleta(id_ben: int, id_comp: int, token:str = Depends(obtener_token_gecros)):

    print(token)

    # Token y headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    # URL para obtener el PDF
    url_template = f"https://appmobile.nobissalud.com.ar/api/AppBenef/{id_ben}/CuentaCorrienteDeuda/{id_comp}"
    response_template = requests.get(url_template, headers=headers)

    # Verifica que la respuesta fue exitosa
    if response_template.status_code == 200:
        pdf_bytes = io.BytesIO(response_template.content)
        headers = {
            'Content-Disposition': f'attachment; filename="Boleta_{id_comp}.pdf"'
        }
        return StreamingResponse(pdf_bytes, media_type="application/pdf", headers=headers)
    else:
        raise HTTPException(status_code=response_template.status_code, detail="Error al descargar el PDF")