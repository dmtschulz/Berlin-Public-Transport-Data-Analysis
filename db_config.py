# Configure database connection parameters (for Docker)
DB_USER = "dia_user"
DB_PASS = "dia"
DB_HOST = "127.0.0.1"
DB_PORT = "5434" # The external port defined in docker-compose
DB_NAME = "db_berlin"

def get_database_url():
    """Constructs and returns the database URL."""
    return f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"