import os
import mysql.connector

# Configurações centralizadas
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 3306)),
    'user': os.environ.get('DB_USER', 'root'), # Usuário padrão
    'password': os.environ.get('DB_PASS', '1234'), # Senha padrão local
    'database': os.environ.get('DB_NAME', 'mineiro')
}

def get_db_connection():
    """Retorna conexão padrão via mysql.connector (usado no app e login)."""
    return mysql.connector.connect(**DB_CONFIG)

def get_sqlalchemy_conn_string():
    """Retorna string de conexão para SQLAlchemy (usado no scraping)."""
    return f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
