from fastapi import FastAPI, HTTPException, Query, Depends, status, Request, Security, WebSocket
from pydantic import BaseModel
from config import load_password, update_password
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

app = FastAPI(
    title="API NOBIS",  # Cambia el nombre de la pestaña
    description="Utilidades para automatizaciones de procesos.",
    version="6.0.0",
)

# Montar la carpeta static
app.mount("/static", StaticFiles(directory="static"), name="static")

# Register authentication routes
app.include_router(users_router)

# Iniciar conexion a MySQL
#init_db()

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
    endpoint = request.url.path  # Obtiene la ruta solicitada automáticamente
    
    # Verificar si el rol tiene permisos para acceder al endpoint
    if "*" not in permisos_rol[role] and endpoint not in permisos_rol[role]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tiene permiso a esta ruta.")
    
    return current_user

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
@app.put("/movfpago/update/{count}", tags=["Actualización | Macena DB"])
async def actualizar_forma_de_pago(data: MovfPago, count: int, current_user: dict = Depends(check_permissions)):
    if count == 1:
    
        usuario = 'CUOMA'
        contraseña = load_password()
        
        try:
            conn = pyodbc.connect(fr"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=10.2.0.6\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña};TrustServerCertificate=yes")
            cursor = conn.cursor()
            
            # Consulta SQL para realizar el UPDATE
            update_query = f"""
            UPDATE movfpago
            SET age_id = ?, fpago_id = ?, entfin_id = ?, movfp_desde = ?, movfp_hasta = ?, cbu = ?, numero = ?, vencimiento = ?, movfp_UsuModi = ?, movfp_fecmodi = ?
            WHERE movfp_id = ?
            """
            
            # Ejecutar la consulta
            cursor.execute(
                update_query,
                data.age_id,
                data.fpago_id,
                data.entfin_id,
                data.movfp_desde,
                data.movfp_hasta,
                data.cbu,
                data.numero,
                data.vencimiento,
                usuario,
                datetime.now(),
                data.movfp_id  # Cambia a la clave correcta si `movfp_id` es diferente
            )
            conn.commit()
            
            return {"message": "Actualización exitosa"}
        
        except pyodbc.Error as e:
            raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {e}")
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error al ejecutar la actualización: {e}")
        
        finally:
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
    WITH BenefcambioWithRowNumber AS (
        SELECT ben_id, os_id, plan_id, tipoBen_id, ROW_NUMBER() OVER (PARTITION BY ben_id ORDER BY fecha_cambio DESC) AS rn  
        FROM BenefCambio)  
    SELECT DISTINCT
        B.*,
        C.tipoBen_id,
        C.tipoBen_nom
    FROM BenefcambioWithRowNumber D  
    LEFT JOIN benef B ON D.ben_id = B.ben_id      
    LEFT JOIN TiposBenef C ON D.tipoBen_id = C.tipoBen_id
    LEFT OUTER JOIN benefagecta BA ON BA.ben_gr_id = B.ben_gr_id
    LEFT OUTER JOIN v_consultapadronnobis V ON V.doc_id = B.doc_id  
    WHERE D.rn = 1 AND B.numero = {dni}
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

        # Definir la consulta SQL
        query = f"""
        SELECT tipoBen_id, tipoBen_nom FROM TiposBenef
        """
        
        try:
            df = pd.read_sql_query(query, conn)

            # Filtrar solo los que empiezan con MT, PP, RD o SD (ignorando espacios al inicio)
            df = df[df['tipoBen_nom'].str.strip().str.startswith(('MT', 'PP', 'RD', 'SD'))]

            result_json = df.to_json(orient="records", date_format="iso")
            return json.loads(result_json)

        
        except Exception as e:
            raise HTTPException(status_code=500, detail="Error al ejecutar la consulta SQL")
        
        finally:
            conn.close()
    else:
        raise HTTPException(status_code=401, detail="Error. Endpoint incorrecto.")
    

# NUEVO ----
from fastapi import FastAPI, Request, Form, WebSocket, Depends, HTTPException, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from uuid import uuid4
from datetime import datetime
import requests

templates = Jinja2Templates(directory="templates")

registros_disponibles = []
llamadores_activados = {}  # clave: box_sucursal → websocket
prellamadores_activados = {}  # clave: sucursal → lista de websockets
websockets_conectados = []

# === WebSocket llamador ===a
@app.websocket("/ws/{sucursal}")
async def websocket_llamador(websocket: WebSocket, sucursal: str):
    key = f"box_{sucursal.lower()}"
    await websocket.accept()
    
    # Inicializar la lista si no existe
    if key not in llamadores_activados:
        llamadores_activados[key] = []
    
    # Añadir este websocket a la lista
    llamadores_activados[key].append(websocket)
    print(f"Nuevo llamador conectado para {key}. Total: {len(llamadores_activados[key])}")
    
    try:
        # Mantener la conexión abierta
        while True:
            data = await websocket.receive_text()
            # Si recibimos 'ping', respondemos con 'pong'
            if data == 'ping':
                await websocket.send_text('pong')
    except Exception as e:
        print(f"Error en websocket llamador: {e}")
    finally:
        # Eliminar este websocket de la lista cuando se desconecta
        if key in llamadores_activados and websocket in llamadores_activados[key]:
            llamadores_activados[key].remove(websocket)
            print(f"Llamador desconectado de {key}. Restantes: {len(llamadores_activados[key])}")


# === WebSocket pre-llamador (para actualización en tiempo real) ===
@app.websocket("/ws/prellamador/{sucursal}")
async def websocket_prellamador(websocket: WebSocket, sucursal: str):
    await websocket.accept()
    
    # Agregar a la lista de websockets para esta sucursal
    if sucursal.lower() not in prellamadores_activados:
        prellamadores_activados[sucursal.lower()] = []
    
    prellamadores_activados[sucursal.lower()].append(websocket)
    
    try:
        # Mantenemos el socket abierto
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        # Eliminar de la lista cuando se desconecta
        if sucursal.lower() in prellamadores_activados:
            if websocket in prellamadores_activados[sucursal.lower()]:
                prellamadores_activados[sucursal.lower()].remove(websocket)



@app.get("/llamador/{id}", response_class=HTMLResponse)
def ver_llamador(request: Request, id: int):
    id_to_sucursal = {
        1: "casa central",
        2: "sgo del estero",
        3: "salta",
        4: "catamarca"
    }

    if id not in id_to_sucursal:
        raise HTTPException(status_code=404, detail="Sucursal no válida")

    return templates.TemplateResponse("llamador.html", {
        "request": request,
        "sucursal": id_to_sucursal[id]
    })


# === Webhook ===
@app.post("/webhook/{id}")
async def recibir_webhook(request: Request, id: int, token: str = Depends(obtener_token_wise)):
    if id != 1:
        return {"status": "ignorado"}

    data = await request.json()

    headers_wise = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'x-api-key': 'be9dd08a9cd8422a9af1372a445ec8e4'
    }

    caso = data["case_id"]
    actividad = data["activity_id"]

    # Obtener datos de actividad
    url = f'https://api.wcx.cloud/core/v1/cases/{caso}/activities/{actividad}?fields=id,type,user_id,content,contact_from,contacts_to,attachments,created_at,sending_status,channel'
    response = requests.get(url, headers=headers_wise)
    data_wise = response.json()

    # Obtener datos del contacto
    contacto = data['contact_id']
    url_contacto = f'https://api.wcx.cloud/core/v1/contacts/{contacto}?fields=id,email,personal_id,phone,name,guid,password,custom_fields,last_update,organization_id,address'
    response_contacto = requests.get(url_contacto, headers=headers_wise)
    data_contacto = response_contacto.json()

    contenido = data_wise['content']
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    #print(f"Datos de contacto: {data_contacto}")
    # Determinar la sucursal según el contenido
    contenido_lower = contenido.lower()
    if "casa central" in contenido_lower:
        sucursal = "casa central"
    elif "catamarca" in contenido_lower:
        sucursal = "catamarca"
    elif "salta" in contenido_lower:
        sucursal = "salta"
    elif "sgo del estero" in contenido_lower:
        sucursal = "sgo del estero"
    else:
        sucursal = "otros"


    nuevo_registro = {
        "id": uuid4().hex,
        "nombre": data_contacto["name"],
        "dni": data_contacto["personal_id"],
        "fecha": now,
        "sucursal": sucursal.lower(),
        "bloqueado": False,
        "llamado": False
    }

    registros_disponibles.append(nuevo_registro)

    # Notificar a todos los pre-llamadores conectados para esta sucursal
    if sucursal.lower() in prellamadores_activados:
        notificacion = {
            "action": "nuevo_registro",
            "registro": nuevo_registro
        }
        for ws in prellamadores_activados[sucursal.lower()]:
            try:
                await ws.send_json(notificacion)
            except:
                continue  # Si falla, continuamos con el siguiente websocket


    return {"status": "registrado", "data": nuevo_registro}


# === GET: Pre-llamador con formulario y registros (mostrando todos) ===
@app.get("/pre-llamador/{id}", response_class=HTMLResponse)
def pre_llamador_get(request: Request, id: int):
    # Enviar TODOS los registros, no solo los no bloqueados
    return templates.TemplateResponse("prellamador.html", {"request": request, "registros": registros_disponibles})

# === POST: Selecciona box/sucursal (solo guardar datos en sesión si aplica) ===
@app.post("/pre-llamador/{id}")
def pre_llamador_post(request: Request, id: int, box: str = Form(...), sucursal: str = Form(...)):
    # En este ejemplo no guardamos estado persistente del form
    return RedirectResponse("/pre-llamador/1", status_code=303)


# === POST: Llamar a un paciente (marca como llamado y envía a llamador) ===
# Modificar la función llamar_registro
@app.post("/llamar/{id}")
async def llamar_registro(request: Request, id: int, registro_id: str = Form(...), box: str = Form(...), sucursal: str = Form(...)):
    for reg in registros_disponibles:
        if reg["id"] == registro_id and not reg.get("llamado", False):
            # Marcar como llamado
            reg["llamado"] = True
            reg["bloqueado"] = True
            reg["box_llamado"] = box
            
            # Enviar al llamador por WebSocket
            key = f"box_{sucursal.lower()}"
            mensaje = {
                "id": reg["id"],
                "name": reg["nombre"],
                "dni": reg["dni"],
                "fecha": reg["fecha"],
                "descripcion": reg["sucursal"],
                "box": box
            }
            
            # Enviar a TODOS los llamadores conectados para esta sucursal
            if key in llamadores_activados and llamadores_activados[key]:
                websockets_con_error = []
                for idx, ws in enumerate(llamadores_activados[key]):
                    try:
                        await ws.send_json(mensaje)
                        print(f"Mensaje enviado a llamador {idx+1} de {key}")
                    except Exception as e:
                        print(f"Error al enviar a llamador {idx+1}: {e}")
                        websockets_con_error.append(ws)
                
                # Limpiar websockets con error
                for ws in websockets_con_error:
                    if ws in llamadores_activados[key]:
                        llamadores_activados[key].remove(ws)
            
            # Notificar a los pre-llamadores (código existente)
            if sucursal.lower() in prellamadores_activados:
                notificacion = {
                    "action": "actualizar_registro",
                    "registro": reg
                }
                for ws in prellamadores_activados[sucursal.lower()]:
                    try:
                        await ws.send_json(notificacion)
                    except:
                        continue
                
            break
    return RedirectResponse("/pre-llamador/1", status_code=303)


@app.post("/repetir-llamado/{id}")
async def repetir_llamado(
    request: Request,
    id: int,
    registro_id: str = Form(...),
    box: str = Form(...),
    sucursal: str = Form(...)
):
    print(f"Repetir llamado recibido: registro_id={registro_id}, box={box}, sucursal={sucursal}")

    # Validar entrada
    if not registro_id or not box or not sucursal:
        raise HTTPException(status_code=400, detail="Faltan parámetros requeridos: registro_id, box o sucursal")

    # Buscar el registro existente
    registro_encontrado = None
    for reg in registros_disponibles:
        if reg["id"] == registro_id:
            registro_encontrado = reg
            break

    if not registro_encontrado:
        print(f"No se encontró el registro con ID {registro_id}")
        raise HTTPException(status_code=404, detail=f"Registro con ID {registro_id} no encontrado")

    # Actualizar el box de llamado
    registro_encontrado["box_llamado"] = box

    # Enviar a TODOS los llamadores activos de la sucursal
    key = f"box_{sucursal.lower()}"
    mensaje = {
        "id": registro_encontrado["id"],
        "name": registro_encontrado["nombre"],
        "dni": registro_encontrado["dni"],
        "fecha": registro_encontrado["fecha"],
        "descripcion": registro_encontrado["sucursal"],
        "box": box,
        "repetido": True
    }
    
    # Verificar si hay llamadores conectados
    if key not in llamadores_activados or not llamadores_activados[key]:
        print(f"No se encontraron llamadores para el box {key}")
        raise HTTPException(status_code=503, detail=f"No hay llamadores activos para la sucursal {sucursal}")
    
    # Enviar a todos los llamadores conectados
    websockets_con_error = []
    for idx, ws in enumerate(llamadores_activados[key]):
        try:
            await ws.send_json(mensaje)
            print(f"Mensaje enviado a llamador {idx+1} de {key}")
        except Exception as e:
            print(f"Error al enviar a llamador {idx+1}: {e}")
            websockets_con_error.append(ws)
    
    # Limpiar websockets con error
    for ws in websockets_con_error:
        if ws in llamadores_activados[key]:
            llamadores_activados[key].remove(ws)

    # Notificar a los pre-llamadores conectados sobre el cambio
    if sucursal.lower() in prellamadores_activados:
        notificacion = {
            "action": "actualizar_registro",
            "registro": registro_encontrado
        }
        for prellamador_ws in prellamadores_activados[sucursal.lower()]:
            try:
                await prellamador_ws.send_json(notificacion)
            except Exception as e:
                print(f"Error al notificar a pre-llamador: {e}")
                continue

    print(f"Repetición de llamado exitosa para registro {registro_id}")
    return RedirectResponse(f"/pre-llamador/{id}", status_code=303)


@app.get("/diagnostico/{id}")
async def diagnostico(id: int):
    """Ruta para diagnosticar las conexiones WebSocket activas"""
    resultado = {
        "llamadores": {},
        "prellamadores": {}
    }
    
    # Contar llamadores activos
    for key, conexiones in llamadores_activados.items():
        resultado["llamadores"][key] = len(conexiones)
    
    # Contar prellamadores activos
    for key, conexiones in prellamadores_activados.items():
        resultado["prellamadores"][key] = len(conexiones)
    
    return resultado