import os
import mysql.connector
from urllib.parse import quote_plus

# Configurações centralizadas
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 3306)),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASS', '1234'),
    'database': os.environ.get('DB_NAME', 'mineiro')
}

def get_db_connection():
    """Retorna conexão padrão via mysql.connector (usado no app e login)."""
    # Nota: mysql.connector lida bem com senhas puras, não precisa de quote_plus aqui
    return mysql.connector.connect(**DB_CONFIG)

def get_sqlalchemy_conn_string():
    """
    Retorna string de conexão para SQLAlchemy (usado no scraping).
    Usa quote_plus para evitar que caracteres especiais na senha quebrem a URL de conexão.
    """
    user = quote_plus(DB_CONFIG['user'])
    password = quote_plus(DB_CONFIG['password'])
    host = DB_CONFIG['host']
    port = DB_CONFIG['port']
    db_name = DB_CONFIG['database']

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
