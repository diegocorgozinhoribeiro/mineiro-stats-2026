import mysql.connector
from werkzeug.security import generate_password_hash

# Configuração do Banco (A mesma do seu app)
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '1234',
    'database': 'mineiro'
}

def criar_usuario(username, senha):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Gera o hash seguro da senha
    senha_hash = generate_password_hash(senha)

    try:
        sql = "INSERT INTO users (username, password_hash) VALUES (%s, %s)"
        cursor.execute(sql, (username, senha_hash))
        conn.commit()
        print(f"Usuário '{username}' criado com sucesso!")
    except mysql.connector.Error as err:
        print(f"Erro ao criar usuário: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Pode mudar o usuário e senha aqui se quiser
    criar_usuario("admin", "1234")
