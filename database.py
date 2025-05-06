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
    cursor.execute('''CREATE TABLE IF NOT EXISTS archivos
                      (alias VARCHAR(50) PRIMARY KEY, original_url TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS usuarios
                      (id INT AUTO_INCREMENT PRIMARY KEY, 
                      user VARCHAR(50) UNIQUE, 
                      hash_password VARCHAR(255), 
                      role VARCHAR(50)
                      )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS contador (
                      id INT AUTO_INCREMENT PRIMARY KEY,
                      fuente TEXT,
                      cuenta INT DEFAULT 0,
                      mes INT DEFAULT 0)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS movimientos_llamador (
                        visita_id INT AUTO_INCREMENT PRIMARY KEY,
                        evento TEXT,
                        caso_id INT,
                        caso_created_at DATETIME,
                        contacto_id INT,
                        actividad_id INT,
                        actividad_type TEXT,
                        sucursal TEXT)''')
    #cursor.execute('''CREATE TABLE ''')
    conn.commit()
    cursor.close()
    conn.close()