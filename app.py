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
from fastapi.responses import JSONResponse

app = FastAPI(
    title="API NOBIS",  # Cambia el nombre de la pestaña
    description="Utilidades para automatizaciones de procesos.",
    version="4.0.0",
)

# Definir un modelo para la entrada de la nueva contraseña
class PasswordUpdate(BaseModel):
    password: str

# Configurar FastAPICache al crear la aplicación
FastAPICache.init(InMemoryBackend())

# Middleware para manejo global de excepciones
@app.middleware("http")
async def exception_handling_middleware(request, call_next):
    try:
        response = await call_next(request)
        return response
    except HTTPException as http_exc:
        return JSONResponse(status_code=http_exc.status_code, content={"detail": http_exc.detail})
    except Exception as exc:
        return JSONResponse(status_code=500, content={"Detalle middleware": f"Internal Server Error: {exc}"})

# Endpoint para obtener la contraseña actual
@app.get("/obtener_contrasena", tags=["Contraseña | Macena DB"])
async def obtener_contraseña():
    password = load_password()
    if password:
        return {"Contraseña": password}
    raise HTTPException(status_code=404, detail="Contraseña no encontrada")


# Endpoint para actualizar la contraseña
@app.post("/actualizar_contrasena", tags=["Contraseña | Macena DB"])
async def actualizar_contraseña(password_update: PasswordUpdate):
    new_password = password_update.password
    if not new_password:
        raise HTTPException(status_code=400, detail="No se ha proporcionado una nueva contraseña")
    
    # Actualizar la contraseña en el archivo
    update_password(new_password)
    return {"Mensaje": "Password updated successfully"}


# Endpoint con consultas de aportes para Widget de retención
@app.get("/ultimos_aportes/{dni}", tags=["Consultas | Macena DB"])
async def ultimos_aportes(dni: int):
    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

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
@app.get("/fecha_alta_y_patologias/{dni}", tags=["Consultas | Macena DB"])
async def consulta_fecha_alta_y_patologias(dni: int):

    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

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


# Endpoint con consultas de aportes de cuenta corriente para Widget de retención
@app.get("/ultimos_aportes_ctacte/{dni}", tags=["Consultas | Macena DB"])
async def ultimos_aportes_cuenta_corriente(dni: int):
    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

    # Definir la consulta SQL
    query = f"""
    DECLARE @PeriodoActual INT = CAST(FORMAT(GETDATE(), 'yyyyMM') AS INT);
    SELECT C.comp_id, A.numero, C.comp_peri, C.comp_total, C.comp_fecha FROM benef AS A
    LEFT JOIN benefagecta AS B ON A.ben_gr_id = B.ben_gr_id
    LEFT JOIN compctacte AS C ON B.agecta_id = C.agecta_id
    WHERE 
        C.tcomp_id = 15 
        AND C.estado = 'N' 
        AND A.numero = {dni} 
        AND C.comp_peri >= @PeriodoActual - 2
    ORDER BY C.comp_fecha DESC
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


# Endpoint para descargar PDF de autorización
@app.get("/descargar_autorizacion/{dni}-{id_aut}", tags=["Auxiliares | BOT WISE"])
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
@app.get("/descargar_boleta/{id_ben}-{id_comp}", tags=["Auxiliares | BOT WISE"])
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


@app.post("/acortar_autorizacion", tags=["Auxiliares | BOT WISE"])
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

    return f"https://descargar.nobis.com.ar/{alias}"


@app.post("/acortar_boleta", tags=["Auxiliares | BOT WISE"])
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

    return f"https://descargar.nobis.com.ar/{alias}"

# Función para buscar el alias en todas las tablas
def buscar_alias(alias: str):
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

@app.get("/{alias}", tags=["Auxiliares | BOT WISE"])
async def redireccionar(alias: str):
    original_url = buscar_alias(alias)
    if original_url:
        return RedirectResponse(original_url)
    else:
        raise HTTPException(status_code=404, detail="Alias no encontrado")


# Endpoint consulta de forma de pago y bonificaciones del afiliado
@app.get("/fpago_bonif/{dni}", tags=["Consultas | Macena DB"])
async def forma_de_pago_y_bonificaciones(dni: int):
    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

    # Definir la consulta SQL
    query = f"""
    SELECT A.agecta_id, B.ben_gr_id, A.doc_id, F.fpago_nombre, C.peri_hasta, C.porcentaje, C.BonficaRec_obs, R.rg_id, R.rg_nombre, R.rg_descrip FROM agentescta AS A
    LEFT JOIN formapago AS F ON A.fpago_id = F.fpago_id
    LEFT JOIN benefagecta AS B ON A.agecta_id = B.agecta_id
    LEFT JOIN BonificaRecargoBenef AS C ON B.ben_gr_id = C.ben_gr_id
	LEFT JOIN ReglasComerciales AS R ON C.rg_id = R.rg_id
    WHERE A.doc_id = {dni}
    ORDER BY C.peri_hasta DESC
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


# Endpoint asociacion de aportes y subdivisiones para Widget de retención
@app.get("/desglose_aportes/{aporte_id}", tags=["Consultas | Macena DB"])
async def detalle_de_aportes(aporte_id: int):
    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

    # Definir la consulta SQL
    query = f"""
    SELECT A.comp_id AS Aporte_id, A.comp_total AS Aporte_total, B.compid_pri AS CompId_Asociado, C.comp_suc AS CompSuc_Asociado,C.comp_nro AS CompNum_Asociado, B.compid_total AS AporteExtraido FROM compctacte AS A
    LEFT JOIN cancelacompctacte AS B ON A.comp_id = B.compid_rel
    LEFT JOIN compctacte AS C ON C.comp_id = B.compid_pri
    WHERE A.comp_id = {aporte_id}
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


# Endpoint pool de aportes por grupo
@app.get("/pool/{group_id}", tags=["Consultas | Macena DB"])
async def pool_de_aportes_log(group_id: int):
    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

    # Definir la consulta SQL
    query = f"""
    SELECT DISTINCT(A.fecha) AS UltimoPool, A.resultado AS Resultado, R.rg_nombre, R.parametros, R.Acumular FROM ReglasComercialesLog AS A
    LEFT JOIN benef AS B ON A.ben_gr_id = B.ben_gr_id
    LEFT JOIN ReglasComerciales AS R ON A.rg_id = R.rg_id
    WHERE R.rgProc_id IN (8,11) AND A.Operacion = 'I' AND B.ben_gr_id = {group_id}
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


# Endpoint localidades
@app.get("/localidades", tags=["Taxo. | Proyecto aplicativo"])
async def lista_de_localidades():
    contraseña = load_password()

    try:
        # Conexión a la base de datos
        conne = pyodbc.connect(
            fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes"
        )
    except pyodbc.InterfaceError:
        raise HTTPException(status_code=500, detail="Error de conexión: No se pudo encontrar el controlador ODBC.")
    except pyodbc.OperationalError:
        raise HTTPException(status_code=500, detail="Error de conexión: No se pudo conectar al servidor.")
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

    # Definir la consulta SQL
    querys = """SELECT L.loc_id, L.loc_nombre, L.prov_id, L.cod_postal, P.prov_nombre FROM localidades AS L
    LEFT JOIN provincias AS P ON L.prov_id = P.prov_id"""

    try:
        # Ejecutar la consulta y convertir los resultados a JSON
        df = pd.read_sql(querys, conne)
        result_json = df.to_json(orient="records", date_format="iso")
        return json.loads(result_json)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar los resultados: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta SQL: {e}")
    finally:
        conne.close()