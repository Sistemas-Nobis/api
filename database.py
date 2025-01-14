import pymysql

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
    cursor.execute('''CREATE TABLE IF NOT EXISTS usuarios
                      (id INT AUTO_INCREMENT PRIMARY KEY, 
                      user VARCHAR(50) UNIQUE, 
                      hash_password VARCHAR(255), 
                      role VARCHAR(50)
                      )''')
    conn.commit()
    cursor.close()
    conn.close()