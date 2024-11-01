import pyodbc
import pandas as pd
from config import load_password, actualizar_token_gecros

def consulta_aportes(dni):
    # Configura la conexión a SQL Server
    contraseña = load_password()

    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER=MACENA-DB\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña}')
    
    # Consulta SQL
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
    
    # Ejecutar la consulta y almacenar en un DataFrame
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def consulta_fecha_alta_y_patologias(dni):
    # Configura la conexión a SQL Server
    contraseña = load_password()

    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER=MACENA-DB\SQLMACENA;DATABASE=Gecros;UID=soporte_nobis;PWD={contraseña}')
    
    # Consulta SQL
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
    
    # Ejecutar la consulta y almacenar en un DataFrame
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Función que simplemente devuelve el token en caché sin reiniciar el tiempo de expiración
async def obtener_token_gecros():
    token = await actualizar_token_gecros()  # Llama a la función que lo obtiene de la caché
    return token