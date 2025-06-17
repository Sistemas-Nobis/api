from fastapi import FastAPI, HTTPException, Query, Depends, status, Request, Security, WebSocket
from pydantic import BaseModel
from config import load_password, update_password, load_password_admin, update_password_admin
from funciones import obtener_token_gecros, obtener_token_wise
from fastapi.responses import StreamingResponse, RedirectResponse
import requests
from pandas import json_normalize
import io
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
import pyodbc
import pandas as pd
import json
import random
import string
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from database import init_db, get_db_connection
from fastapi.security import OAuth2PasswordBearer
from usuarios import router as users_router, decode_token, permisos_rol
from config import verify_secret_key
from models import *
from datetime import datetime
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import re
import httpx

app = FastAPI(
    title="API NOBIS",  # Cambia el nombre de la pestaña
    description="Utilidades para automatizaciones de procesos.",
    version="7.0.0",
)

origenes = [
    "https://api.nobis.com.ar"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # o ["*"] para pruebas
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Montar la carpeta static
app.mount("/static", StaticFiles(directory="static"), name="static")

# Register authentication routes
app.include_router(users_router)

# Iniciar conexion a MySQL
init_db()

# Configure OAuth2 with security scopes
oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="token",
    scopes={
        "admin": "Full access",
        "cuoma": "Acceso de cuoma"
    }
)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    payload = decode_token(token)
    if not payload:
        raise HTTPException(
            status_code=401,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload

# Dependencia para verificar permisos por rol
async def check_permissions(request: Request, current_user: dict = Depends(get_current_user)):
    role = current_user.get("role")
    endpoint = request.url.path

    rutas_permitidas = permisos_rol.get(role, [])

    # Permitir acceso total si tiene '*'
    if "*" in rutas_permitidas:
        return current_user

    for ruta in rutas_permitidas:
        # Convertir la ruta tipo FastAPI a regex
        # Ej: /movfpago/update/1/{grupo_id} → ^/movfpago/update/1/\d+$
        regex = re.sub(r"{[^/]+}", r"\\d+", ruta)
        regex = f"^{regex}$"
        if re.match(regex, endpoint):
            return current_user

    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tiene permiso a esta ruta.")


# Definir un modelo para la entrada de la nueva contraseña
class PasswordUpdate(BaseModel):
    password: str

class PasswordUpdateAdmin(BaseModel):
    admin: str

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


@app.post("/actualizar_contrasena_admin", tags=["Contraseña | Macena DB ADMIN"])
async def actualizar_contraseña_admin(password_update: PasswordUpdateAdmin):
    new_password = password_update.admin
    if not new_password:
        raise HTTPException(status_code=400, detail="No se ha proporcionado una nueva contraseña")
    
    # Actualizar la contraseña en el archivo
    update_password_admin(new_password)
    return {"Mensaje": "Password ADMIN updated successfully"}


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
    DECLARE @FechaLimite DATE = DATEADD(MONTH, -3, GETDATE());
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
        A.aporte > 1
        AND B.numero = {dni}
		AND A.fecha >= @FechaLimite
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
        ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS cobertura_especial,
        STUFF((
            SELECT ', ' + CAST(E.id_tipocob AS NVARCHAR)
            FROM coberturaesp AS E
            WHERE E.ben_id = C.ben_id
                AND GETDATE() BETWEEN E.cobesp_desde AND E.cobesp_hasta
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS id_cobertura_especial
    FROM benef C
    LEFT JOIN BenefCambio B ON B.ben_id = C.ben_id
    LEFT JOIN tipomov T ON T.tm_id = B.tcambio_id
    WHERE 
        T.tm_id = 2 AND C.doc_id = {dni}
    GROUP BY 
        C.ben_id, C.doc_id, B.bc_fecha;
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
    DECLARE @FechaLimite DATE = DATEADD(MONTH, -3, GETDATE());
    SELECT 
    C.comp_id, A.numero, C.comp_peri, C.comp_total, C.comp_fecha 
    FROM benef AS A
    LEFT JOIN benefagecta AS B ON A.ben_gr_id = B.ben_gr_id
    LEFT JOIN compctacte AS C ON B.agecta_id = C.agecta_id
    WHERE 
        C.tcomp_id = 15 
        AND C.estado = 'N' 
        AND A.numero = {dni}
        AND C.comp_fecha >= @FechaLimite
    ORDER BY C.comp_fecha DESC;
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

# Generador de alias único
def generate_unique_alias():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=6))


@app.post("/acortar_autorizacion", tags=["Auxiliares | BOT WISE"], dependencies=[Depends(verify_secret_key)])
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


@app.post("/acortar_boleta", tags=["Auxiliares | BOT WISE"], dependencies=[Depends(verify_secret_key)])
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

# Función para buscar el alias en todas las tablas - Mover a utils
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
@app.get("/fpago_bonif/{grupo_id}", tags=["Consultas | Macena DB"])
async def forma_de_pago_y_bonificaciones(grupo_id: int):
    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

    # Definir la consulta SQL
    query = f"""
    SELECT A.agecta_id, B.ben_gr_id, A.doc_id, F.fpago_nombre, C.peri_desde, C.peri_hasta, C.porcentaje, C.BonficaRec_obs, R.rg_id, R.rg_nombre, R.rg_descrip 
    FROM agentescta AS A
    LEFT JOIN formapago AS F ON A.fpago_id = F.fpago_id
    LEFT JOIN benefagecta AS B ON A.agecta_id = B.agecta_id
    LEFT JOIN BonificaRecargoBenef AS C ON B.ben_gr_id = C.ben_gr_id
    LEFT JOIN ReglasComerciales AS R ON C.rg_id = R.rg_id
    WHERE B.ben_gr_id LIKE '%{grupo_id}%' AND FORMAT(GETDATE(), 'yyyyMM') BETWEEN B.peri_desde AND B.peri_hasta
    ORDER BY C.peri_hasta DESC;
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


# Localidades
@app.get("/localidades/{id}", tags=["Consultas | Macena DB"])
async def localidades(id: int):
    contraseña = load_password()
    if id == 1:
        try:
            conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

        except pyodbc.Error as e:
            raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

        # Definir la consulta SQL
        query = f"""
        SELECT L.loc_id, L.loc_nombre, L.prov_id, L.cod_postal, P.prov_nombre FROM localidades AS L
        LEFT JOIN provincias AS P ON L.prov_id = P.prov_id
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
    else:
        raise HTTPException(status_code=401, detail="Error. Endpoint incorrecto.")


# Endpoint con consultas de aportes de cuenta corriente para Widget de retención
@app.get("/dni_agecta/{grupo_id}", tags=["Consultas | Macena DB"])
async def dni_de_agente_de_cuenta(grupo_id: int):
    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

    # Definir la consulta SQL
    query = f"""
    SELECT A.ben_gr_id, B.agecta_id, B.doc_id FROM benefagecta AS A
    LEFT JOIN agentescta AS B ON A.agecta_id = B.agecta_id
    WHERE A.ben_gr_id = {grupo_id} AND FORMAT(GETDATE(), 'yyyyMM') BETWEEN A.peri_desde AND A.peri_hasta
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


# Endpoint para actualizar los datos
@app.post("/movfpago/update/{count}/{grupo_id}", tags=["Actualización | Macena DB"])
async def actualizar_forma_de_pago(data: MovfPago, count: int, grupo_id: int, current_user: dict = Depends(check_permissions)):
    if count == 1:
    
        usuario = 'CUOMA'
        contraseña = load_password_admin()

        query = f"""
            SELECT TOP 1 A.movfp_id, A.age_id, A.fpago_id, A.entfin_id, A.movfp_desde,
            A.movfp_hasta, A.cbu, A.numero, A.vencimiento, B.ben_gr_id 
            FROM movfpago AS A
            LEFT JOIN benefagecta AS B ON A.age_id = B.agecta_id
            WHERE B.ben_gr_id = {grupo_id}
            ORDER BY movfp_id DESC
            """
        
        conn = None
        try:
            ### PRODUCCION ###
            #conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=sistemas-admin;PWD={contraseña};TrustServerCertificate=yes")
            ##################

            ### TESTS ###
            conn = pyodbc.connect("DRIVER=FreeTDS;SERVER=172.16.0.31;PORT=1433;DATABASE=GecrosPruebas;UID=apitests;PWD=sistemasapi;TDS_Version=8.0")
            #############

            cursor = conn.cursor()
            
            # Busqueda de movimiento actual
            df = pd.read_sql_query(query, conn)
            
            if not df.empty:
                actual_movfp_id = df.loc[0, 'movfp_id']
                #print(f"Mov_id: {actual_movfp_id}")
                agente_id = df.loc[0, 'age_id']
                #print(f"Agente ID: {agente_id}")
                actual_fecha_desde = int(df.loc[0, 'movfp_desde'])
            else:
                actual_movfp_id = None
                raise HTTPException(status_code=500, detail=f"Error sin agente de cuenta: {e}")
            
            # Validar fecha de corte. TC: Día 10 - Debito: Día 16
            periodo_actual = int(datetime.today().strftime("%Y%m"))
            #print(f"Periodo actual: {periodo_actual}")

            dia_actual = datetime.today().day
            #print(f"Dia de hoy: {dia_actual}")

            periodo_hasta = periodo_actual
            if data.fpago_id == 2: # TC
                #print("Tarjeta de credito")
                if dia_actual <= 10:
                    #print(f"La forma de pago actual se vencerá este mes. {periodo_hasta}")
                    periodo_hasta = periodo_actual
                else:
                    periodo_hasta = periodo_hasta + 1
                    #print(f"La forma de pago actual se vencerá el proximo mes. {periodo_hasta}")
            
            elif data.fpago_id == 3: # Debito
                #print("Debito bancario")
                if dia_actual <= 16:
                    #print(f"La forma de pago actual se vencerá este mes. {periodo_hasta}")
                    periodo_hasta = periodo_actual
                else:
                    periodo_hasta = periodo_hasta + 1
                    #print(f"La forma de pago actual se vencerá el proximo mes. {periodo_hasta}")

            else: # Contado
                #print("Contado")
                if dia_actual <= 18:
                    #print(f"La forma de pago actual se vencerá este mes. {periodo_hasta}")
                    periodo_hasta = periodo_actual
                else:
                    periodo_hasta = periodo_hasta + 1
                    #print(f"La forma de pago actual se vencerá el proximo mes. {periodo_hasta}") 

            # Validación de periodos
            if actual_fecha_desde > periodo_hasta:
                raise HTTPException(status_code=500, detail=f"Ya existe un trámite en curso")

            # Realizar UPDATE sobre movimiento actual
            update_query = f"""
            UPDATE movfpago
            SET movfp_hasta = {periodo_hasta},
                movfp_Usumodi = 'CUOMA',
                movfp_fecmodi = GETDATE()
            WHERE movfp_id = {actual_movfp_id}
            """
            try:
                cursor.execute(update_query)
                conn.commit()  # Muy importante para que se apliquen los cambios
                #print("Update ejecutado correctamente.")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error al ejecutar el update: {e}")
            
            nueva_movfp_desde = int(periodo_hasta) + 1
            #print(nueva_movfp_desde)

            # Defaults
            nueva_movfp_hasta = 290012
            nro_auto = ''
            agente_id = int(agente_id)

            insert_query = f"""
            INSERT INTO movfpago
            (age_id, fpago_id, entfin_id, movfp_desde, movfp_hasta, cbu, numero, nro_auto, vencimiento, movfp_UsuAlta, movfp_fecalta)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            try:
                # Forzamos a tomar la entidad GALICIA en TD.
                if data.fpago_id == 3:
                    entidad = 71
                else:
                    entidad = data.entfin_id

                # Ejecutar la consulta
                cursor.execute(
                    insert_query,
                    agente_id, # Agente de cuenta
                    data.fpago_id, # Forma de pago
                    entidad, # Entidad financiera
                    nueva_movfp_desde, # Cierre de la anterior + 1
                    nueva_movfp_hasta, # Cierre de actual = Fijo
                    data.cbu, # CBU | Los CVU se rechazaran (revisar models.py)
                    data.numero, # Número de tarjeta
                    nro_auto,
                    data.vencimiento, # Vencimiento de tarjeta
                    usuario, # Usuario default
                    datetime.now(), # Fecha actual
                )
                conn.commit()
                
                if data.fpago_id == 3:
                    return {"mensaje": "Actualización exitosa",
                            "entidad": f"{data.nombre_entidad}"}
                else:
                    return {"mensaje": "Actualización exitosa"}
            
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error al ejecutar la actualización: {e}")
        
        except pyodbc.Error as e:
            raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error al ejecutar la actualización: {e}")
        
        finally:
            if conn:
                conn.close()

    else:
        raise HTTPException(status_code=505, detail=f"Error al ejecutar la actualización: {e}")


# Endpoint con consultas de aportes de cuenta corriente para Widget de retención
@app.get("/tipo_ben/{dni}", tags=["Consultas | Macena DB"])
async def tipo_beneficiario(dni: int):
    contraseña = load_password()

    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

    # Definir la consulta SQL
    query = f"""
    SELECT TOP 1 B.*, T.tipoBen_id, T.tipoBen_nom FROM BenefCambio AS C
    LEFT JOIN TiposBenef AS T ON C.tipoBen_id = T.tipoBen_id
    INNER JOIN benef AS B ON B.ben_id = C.ben_id
    WHERE B.numero = {dni}
    ORDER BY C.bc_fecha DESC
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


@app.post("/acortar_archivo", tags=["Auxiliares | BOT WISE"], dependencies=[Depends(verify_secret_key)])
async def acortar_archivo(original_url: str, alias: str = None):
    if not original_url:
        raise HTTPException(status_code=400, detail="Falta la URL original")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Si no se proporciona un alias, genera uno único
    if not alias:
        alias = generate_unique_alias()
        cursor.execute("SELECT alias FROM archivos WHERE alias = %s", (alias,))
        while cursor.fetchone():
            alias = generate_unique_alias()  # Reintenta hasta encontrar uno único

    # Verifica si el alias ya existe en caso de que el usuario lo haya especificado
    cursor.execute("SELECT alias FROM archivos WHERE alias = %s", (alias,))
    if cursor.fetchone():
        cursor.close()
        conn.close()
        raise HTTPException(status_code=400, detail="El alias ya existe")

    # Inserta el enlace con el alias en la base de datos
    cursor.execute("INSERT INTO archivos (alias, original_url) VALUES (%s, %s)", (alias, original_url))
    conn.commit()
    cursor.close()
    conn.close()

    return f"https://descargar.nobis.com.ar/{alias}"


# Endpoint para descargar PDF de autorización
@app.get("/descargar_recetas/{id}", tags=["Auxiliares | BOT WISE"])
async def descargar_recetas(id: int, token:str = Depends(obtener_token_gecros)):

    #print(token)

    # Token y headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    # URL para obtener el PDF
    url_template = f"https://appmobile.nobissalud.com.ar/api/Archivo/get-img/{id}"
    response_template = requests.get(url_template, headers=headers)

    # Verifica que la respuesta fue exitosa
    if response_template.status_code == 200:
        pdf_bytes = io.BytesIO(response_template.content)
        headers = {
            'Content-Disposition': f'attachment; filename="Receta_{id}.pdf"'
        }
        return StreamingResponse(pdf_bytes, media_type="application/pdf", headers=headers)
    else:
        raise HTTPException(status_code=response_template.status_code, detail="Error al descargar el PDF")
    

# Endpoint para descargar PDF de autorización
@app.get("/contador/{id}", tags=["Auxiliares | Scripts Internos"], dependencies=[Depends(verify_secret_key)])
async def contador_interno(id: int):
    
    if id == 1:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT cuenta, mes FROM contador WHERE id = 1")
        cuenta, mes_guardado = cursor.fetchone()

        mes_actual = datetime.now().month

        #print(f"Mes guardado: {mes_guardado} - Mes actual: {mes_actual}")

        # Si cambió el mes, reiniciar el contador
        if mes_actual != mes_guardado:
            cuenta = 0
            cursor.execute("UPDATE contador SET cuenta = %s, mes = %s WHERE id = 1", (cuenta, mes_actual))
            conn.commit()

        if cuenta < 4:
            nueva_cuenta = cuenta + 1
            cursor.execute("UPDATE contador SET cuenta = %s WHERE id = 1", (nueva_cuenta,))
            conn.commit()
            cursor.close()
            conn.close()
            return {"status": True, "contador": nueva_cuenta, "mes": mes_actual}
        
        return {"status": False, "mensaje": "Límite alcanzado", "mes": mes_actual}
    else:
        raise HTTPException(status_code=401, detail="Error. Endpoint incorrecto.")
    

# Localidades
@app.get("/tipos_beneficiario/{id}", tags=["Consultas | Macena DB"])
async def tipos_de_beneficiario(id: int):
    contraseña = load_password()
    if id == 1:
        try:
            conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

        except pyodbc.Error as e:
            raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")
        
        try:

            tipos_especificos = [
                'MT - OSIM',
                'MT - OSSACRA',
                'RD - OSPSMBA',
                'RD - OSIM',
                'RD - OSSACRA',
                'RD - OSPYSA',
                'RD - RELACION DE DEPENDENCIA',
                'SD - SERVICIO DOMESTICO OSSACRA',
                'PP - PREPAGO',
                'RD - NOBIS'
            ]

            # Convertir la lista a formato para consulta SQL
            tipos_str = "', '".join(tipos_especificos)

            # Definir la consulta SQL
            query = f"""
            SELECT tipoBen_id, tipoBen_nom FROM TiposBenef
            WHERE tipoBen_nom IN ('{tipos_str}')
            """

            df = pd.read_sql_query(query, conn)

            # Filtrar solo los que empiezan con MT, PP, RD o SD (ignorando espacios al inicio)
            #df = df[df['tipoBen_nom'].str.strip().str.startswith(('MT', 'PP', 'RD', 'SD'))]
            
            result_json = df.to_json(orient="records", date_format="iso")
            return json.loads(result_json)

        
        except Exception as e:
            raise HTTPException(status_code=500, detail="Error al ejecutar la consulta SQL")
        
        finally:
            conn.close()
    else:
        raise HTTPException(status_code=401, detail="Error. Endpoint incorrecto.")
    
# Nomenclador
@app.get("/nomencod/{codigo}", tags=["Consultas | Macena DB"])
async def nomenclador_por_codigo(codigo: int):
    contraseña = load_password()
 
    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")
 
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")
 
    # Definir la consulta SQL
    query = f"""
    SELECT
        A.nom_cod,
        A.nom_nom AS Nombre,
        A.nom_id,
        B.nom_nom AS TipoNomenclador
    FROM nomenclador AS A
    LEFT JOIN tiponom AS B ON A.nom_id = B.nom_id
    WHERE A.nom_cod = ?
    ORDER BY A.nom_cod DESC
    """
 
      # Ejecutar la consulta y convertir los resultados a JSON
    try:
        df = pd.read_sql_query(query, conn, params=[codigo])
        return json.loads(df.to_json(orient="records"))
    except Exception as e:
        print("Error al ejecutar la consulta SQL", e)
        return []
   
    finally:
        conn.close()
 
 
# Ubicacion
@app.get("/ubicacion/{dni}", tags=["Consultas | Macena DB"])
async def ubicacion(dni: int):
    contraseña = load_password()
 
    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")
 
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")
 
    # Definir la consulta SQL
    query = f"""
    SELECT L.loc_id, L.loc_nombre, P.prov_id, P.prov_nombre FROM benef AS A
    LEFT JOIN benradic AS B ON A.ben_id = B.ben_id
    LEFT JOIN localidades AS L ON B.loc_id = L.loc_id
    LEFT JOIN provincias AS P ON L.prov_id = P.prov_id
    WHERE A.numero = ?
    """
 
      # Ejecutar la consulta y convertir los resultados a JSON
    try:
        df = pd.read_sql_query(query, conn, params=[dni])
        return json.loads(df.to_json(orient="records"))
    except Exception as e:
        print("Error al ejecutar la consulta SQL", e)
        return []
   
    finally:
        conn.close()
 
 
# Plan
@app.get("/plan_afiliado/{plan_nom}", tags=["Consultas | Macena DB"])
async def plan_afiliado(plan_nom: str):
    contraseña = load_password()
 
    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")
 
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")
 
    # Definir la consulta SQL
    query = f"""
    SELECT plan_nombre, plan_id FROM planes
    WHERE plan_nombre LIKE '{plan_nom}'
    """
 
      # Ejecutar la consulta y convertir los resultados a JSON
    try:
        df = pd.read_sql_query(query, conn)
        return json.loads(df.to_json(orient="records"))
    except Exception as e:
        print("Error al ejecutar la consulta SQL", e)
        return []
   
    finally:
        conn.close()
 

# Origenes Aranceles
@app.get("/origenes_aranceles/{os_id}/{cod_pra}/{nom_pra}/{prov_id}/{loc_id}/{plan_id}", tags=["Consultas | Macena DB"])
async def origenes_aranceles(os_id: int, cod_pra: int, nom_pra: int, prov_id: int, loc_id: int, plan_id: int):
    contraseña = load_password()
 
    try:
        conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")
 
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")
 
    # Definir la consulta SQL
    query = f"""
    SELECT TOP 20
    TN.nom_nom,
    CF.cta_cdesde AS código,
    N.nom_nom AS nombre_practica,
    SUM(CF.cta_esp + CF.cta_ayu + CF.cta_ane + CF.cta_gto) AS total,
    O.ori_nom,
    MF.vigencia,
    tl.tab_nom,
    L.loc_nombre,
    PR.prov_nombre
    FROM ctafac CF
    LEFT JOIN nomenclador N ON N.nom_cod = CF.cta_cdesde
    LEFT JOIN tiponom TN ON TN.nom_id = N.nom_id
    LEFT JOIN tablas TL ON TL.cta_id = CF.cta_id
    LEFT JOIN medfac MF ON MF.tab_id = TL.tab_id
    LEFT JOIN origenes O ON O.ori_id = MF.ori_id
    LEFT JOIN localidades L ON L.loc_id = O.loc_id
    LEFT JOIN provincias PR ON PR.prov_id = L.prov_id
    LEFT JOIN ExcepAutoExcluirOSPlan EO ON EO.Ori_id = O.ori_id
    WHERE O.ori_nom IS NOT NULL AND MF.os_id = {os_id} AND MF.pre_id = 0
    AND CF.cta_cdesde = {cod_pra} AND TN.nom_id = {nom_pra}
    AND PR.prov_id = {prov_id} AND L.loc_id = {loc_id}
    AND NOT EXISTS (
        SELECT 1
        FROM ExcepAutoExcluirOSPlan EO2
        WHERE EO2.ori_id = O.ori_id AND EO2.plan_id = {plan_id})
    AND MF.vigencia = (
        SELECT MAX(MF2.vigencia)
        FROM medfac MF2
        WHERE MF2.vigencia >= CAST(GETDATE() AS DATE))
    GROUP BY CF.cta_cdesde, N.nom_nom, TN.nom_nom, O.ori_nom,
        MF.vigencia, tl.tab_nom, L.loc_nombre, PR.prov_nombre
    ORDER BY total ASC
    """
 
      # Ejecutar la consulta y convertir los resultados a JSON
    try:
        df = pd.read_sql_query(query, conn, parse_dates=False)
        df["vigencia"] = df["vigencia"].astype(str).str[:10]
        return json.loads(df.to_json(orient="records"))
    except Exception as e:
        print("Error al ejecutar la consulta SQL", e)
        return []
   
    finally:
        conn.close()


@app.get("/nomenclador/{id}", tags=["Consultas | Macena DB"])
async def nomenclador(id: int):
    contraseña = load_password()
    if id == 1:
        try:
            conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")

        except pyodbc.Error as e:
            raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")

        # Definir la consulta SQL
        query = f"""
        SELECT A.nom_id, B.nom_nom, A.nom_cod, A.nom_nom AS cod_nom FROM nomenclador AS A
        LEFT JOIN tiponom AS B ON A.nom_id = B.nom_id
        WHERE A.nom_id NOT IN (26,4)
        """
        # Excluye nomencladores "NO USAR (26)" y "FARMACIA (4)"
        # Ejecutar la consulta y convertir los resultados a JSON
        try:
            df = pd.read_sql_query(query, conn)
            result_json = df.to_json(orient="records", date_format="iso")
            return json.loads(result_json)
        
        except Exception as e:
            raise HTTPException(status_code=500, detail="Error al ejecutar la consulta SQL")
        
        finally:
            conn.close()
    else:
        raise HTTPException(status_code=401, detail="Error. Endpoint incorrecto.")
    

@app.post("/obtener_prestadores", tags=["Auxiliares | BOT WISE"])
async def obtener_prestadores(
    data: PrestadoresRequest,
    token: str = Depends(obtener_token_gecros)
):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    url = "https://appmobile.nobissalud.com.ar/api/prestadores/cartilla"
    todos_prestadores = []

    async with httpx.AsyncClient() as client:
        for esp_id in data.esp_ids:
            body = {
                "preId": None,
                "nombrePrestador": "",
                "oriId": 0,
                "osId": data.os_id,
                "planId": data.plan_id,
                "espId": esp_id,
                "locId": data.loc_id,
                "mostrarOcultos": 2,
                "farmacias": False
            }
            resp = await client.post(url, headers=headers, json=body)
            if resp.status_code == 200:
                resp_json = resp.json()
                # Extrae los prestadores si existen
                prestadores = resp_json.get("result", {}).get("prestadores", [])
                todos_prestadores.extend(prestadores)
            else:
                # Si falla una consulta, puedes decidir si lanzar error o seguir
                continue

    return {"prestadores": todos_prestadores}