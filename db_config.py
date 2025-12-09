
# Configure database connection parameters
DB_USER = "postgres"
DB_PASS = "123456"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "dia_db"

def get_database_url():
    """Constructs and returns the database URL."""
    return f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"