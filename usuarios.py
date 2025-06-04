from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from config import create_access_token, verify_password, get_password_hash
from database import get_db_connection
import pymysql
from config import decode_token

# 🌟 Función para registrar un nuevo usuario
def create_user(user: str, password: str, role: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    hashed_password = get_password_hash(password)
    try:
        cursor.execute(
            "INSERT INTO usuarios (user, hash_password, role) VALUES (%s, %s, %s)",
            (user, hashed_password, role),
        )
        conn.commit()
    except pymysql.MySQLError as e:
        print(f"Error al crear usuario: {e}")
    finally:
        cursor.close()
        conn.close()

#create_user('apicuoma', '52deuCNKe2uE', 'cuoma')

# 🌟 Función para autenticar un usuario
def authenticate_user(username: str, password: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM usuarios WHERE user = %s", (username,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()

    if not user:
        return False
    # Acceder a los valores usando los índices correctos
    if not verify_password(password, user[2]):  # user[2] es 'hash_password'
        return False
    
    return {"user": user[1], "role": user[3]}  # user[1] es 'user' y user[3] es 'role'

# 🌟 Ruta para generar un token
router = APIRouter()

@router.post("/token/{id}", tags=["Seguridad"])
async def token_de_acceso(id:int, form_data: OAuth2PasswordRequestForm = Depends()):
    if id == 1:
        user = authenticate_user(form_data.username, form_data.password)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario o contraseña incorrecto.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        access_token = create_access_token(data={"sub": user["user"], "role": user["role"]})
        return {"access_token": access_token, "token_type": "bearer"}
    else:
         return {"mensaje": "Puerto invalido."}

permisos_rol = {
    "admin": ["*"],  # ADMIN
    "cuoma": ["/movfpago/update/1/{grupo_id}"],  # CUOMA
}