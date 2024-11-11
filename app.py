from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel
from config import load_password, update_password
from funciones import obtener_token_gecros
from fastapi.responses import StreamingResponse, RedirectResponse
import requests
from pandas import json_normalize
import io
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
import pyodbc
import pandas as pd
import json
import pymysql
import random
import string

app = FastAPI(
    title="API NOBIS",  # Cambia el nombre de la pestaña
    description="Utilidades para automatizaciones de procesos.",
    version="2.1.2",
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
@app.get("/ultimos_aportes/{dni}")
async def ultimos_aportes(dni: int):
    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

    # Definir la consulta SQL
    query = f"""
    DECLARE @PeriodoActual INT = CAST(FORMAT(GETDATE(), 'yyyyMM') AS INT);
    SELECT DISTINCT
        A.fecha,
        A.emp_id,
        A.Periodo,
        A.Sueldo,
        A.aporte,
        B.numero
    FROM
        aportes AS A
    LEFT JOIN
        benef AS B ON A.ben_id = B.ben_id
    WHERE
        A.Periodo >= @PeriodoActual - 2
        AND A.aporte > 1
        AND B.numero = {dni}
    ORDER BY
        A.Periodo DESC;
    """
    
    # Ejecutar la consulta y convertir los resultados a JSON
    try:
        df = pd.read_sql_query(query, conn)
        result_json = df.to_json(orient="records", date_format="iso")
        return json.loads(result_json)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al ejecutar la consulta SQL")
    
    finally:
        conn.close()
    

# Endpoint con consultas de fecha de alta y patologias para Widget de retención
@app.get("/fecha_alta_y_patologias/{dni}")
async def consulta_fecha_alta_y_patologias(dni: int):

    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

    # Definir la consulta SQL
    query = f"""
    SELECT DISTINCT
        C.ben_id,
        C.doc_id,
        B.bc_fecha AS fecha_alta,
        STUFF((
            SELECT ', ' + TC.tcob_nombre
            FROM coberturaesp AS E
            LEFT JOIN tipocobertura AS TC ON E.id_tipocob = TC.id_tipocob
            WHERE E.ben_id = C.ben_id
                AND GETDATE() BETWEEN E.cobesp_desde AND E.cobesp_hasta
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS cobertura_especial
    FROM benef C
        LEFT JOIN BenefCambio B ON B.ben_id = C.ben_id
        LEFT JOIN tipomov T ON T.tm_id = B.tcambio_id
    WHERE 
        T.tm_id = 2 AND C.doc_id = {dni}
    GROUP BY 
        C.ben_id, C.doc_id, B.bc_fecha
    """
    
    # Ejecutar la consulta y convertir los resultados a JSON
    try:
        df = pd.read_sql_query(query, conn)

        # Eliminar espacios en blanco al inicio y al final de los valores en la columna 'cobertura_especial'
        df['cobertura_especial'] = df['cobertura_especial'].str.strip().replace("  ", "")

        result_json = df.to_json(orient="records", date_format="iso")
        return json.loads(result_json)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta SQL: {e}")
    
    finally:
        conn.close()


# Endpoint para descargar PDF de autorización
@app.get("/descargar_autorizacion/{dni}-{id_aut}")
async def descargar_autorizacion(dni: int, id_aut: int, token:str = Depends(obtener_token_gecros)):

    #print(token)

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
@app.get("/descargar_boleta/{id_ben}-{id_comp}")
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
    

### ACORTADOR DE ENLACES / GENERADOR DE ALIAS ###

# Configuración de la conexión a MySQL
def get_db_connection():
    return pymysql.connect(
        host="10.2.0.7",
        user="api",
        password="nobisapi",
        database="enlaces"
    )


# Inicialización de la base de datos
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS autorizaciones
                      (alias VARCHAR(50) PRIMARY KEY, original_url TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS boletas
                      (alias VARCHAR(50) PRIMARY KEY, original_url TEXT)''')
    conn.commit()
    cursor.close()
    conn.close()
init_db()


# Generador de alias único
def generate_unique_alias():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=6))


@app.post("/acortar_autorizacion")
async def acortar_autorizacion(original_url: str, alias: str = None):
    if not original_url:
        raise HTTPException(status_code=400, detail="Falta la URL original")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Si no se proporciona un alias, genera uno único
    if not alias:
        alias = generate_unique_alias()
        cursor.execute("SELECT alias FROM autorizaciones WHERE alias = %s", (alias,))
        while cursor.fetchone():
            alias = generate_unique_alias()  # Reintenta hasta encontrar uno único

    # Verifica si el alias ya existe en caso de que el usuario lo haya especificado
    cursor.execute("SELECT alias FROM autorizaciones WHERE alias = %s", (alias,))
    if cursor.fetchone():
        cursor.close()
        conn.close()
        raise HTTPException(status_code=400, detail="El alias ya existe")

    # Inserta el enlace con el alias en la base de datos
    cursor.execute("INSERT INTO autorizaciones (alias, original_url) VALUES (%s, %s)", (alias, original_url))
    conn.commit()
    cursor.close()
    conn.close()

    return f"http://api.nobis.com.ar/{alias}"


@app.post("/acortar_boleta")
async def acortar_boleta(original_url: str, alias: str = None):
    if not original_url:
        raise HTTPException(status_code=400, detail="Falta la URL original")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Si no se proporciona un alias, genera uno único
    if not alias:
        alias = generate_unique_alias()
        cursor.execute("SELECT alias FROM boletas WHERE alias = %s", (alias,))
        while cursor.fetchone():
            alias = generate_unique_alias()  # Reintenta hasta encontrar uno único

    # Verifica si el alias ya existe en caso de que el usuario lo haya especificado
    cursor.execute("SELECT alias FROM boletas WHERE alias = %s", (alias,))
    if cursor.fetchone():
        cursor.close()
        conn.close()
        raise HTTPException(status_code=400, detail="El alias ya existe")

    # Inserta el enlace con el alias en la base de datos
    cursor.execute("INSERT INTO boletas (alias, original_url) VALUES (%s, %s)", (alias, original_url))
    conn.commit()
    cursor.close()
    conn.close()

    return f"http://api.nobis.com.ar/{alias}"

# Función para buscar el alias en todas las tablas
def find_alias_in_all_tables(alias: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Obtener todas las tablas de la base de datos
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        # Recorrer cada tabla y buscar el alias
        for (table_name,) in tables:
            query = f"SELECT original_url FROM {table_name} WHERE alias = %s"
            cursor.execute(query, (alias,))
            result = cursor.fetchone()
            if result:
                return result[0]  # Retorna la URL si se encuentra el alias
        return None  # Retorna None si no encuentra el alias en ninguna tabla
    finally:
        cursor.close()
        conn.close()

@app.get("/{alias}")
async def redirect_link(alias: str):
    original_url = find_alias_in_all_tables(alias)
    if original_url:
        return RedirectResponse(original_url)
    else:
        raise HTTPException(status_code=404, detail="Alias no encontrado")