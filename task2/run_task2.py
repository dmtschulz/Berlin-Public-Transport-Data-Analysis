import os
import sys
from sqlalchemy import create_engine, text

# Add parent directory to path to import db_config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    # Adjust this import based on your actual db_config file name/location
    from db_config import get_database_url 
    DATABASE_URL = get_database_url()
except ImportError:
    # Fallback if config import fails
    DB_USER = "postgres"
    DB_PASS = "123456"
    DB_HOST = "127.0.0.1"
    DB_PORT = "5432"
    DB_NAME = "dia_db"
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def run_query(file_path, params=None):
    """Reads a SQL file and executes it with parameters."""
    if not os.path.exists(file_path):
        print(f"Error: SQL file {file_path} not found.")
        return

    with open(file_path, 'r') as f:
        sql_content = f.read()

    engine = create_engine(DATABASE_URL)
    
    print(f"\n--- Running {os.path.basename(file_path)} ---")
    try:
        with engine.connect() as conn:
            # Execute query with parameters
            result = conn.execute(text(sql_content), params or {})
            
            # Print columns
            keys = result.keys()
            print(f"{' | '.join(keys)}")
            print("-" * 30)
            
            # Print rows
            rows = result.fetchall()
            if not rows:
                print("(No results found)")
            for row in rows:
                print(row)
                
    except Exception as e:
        print(f"Error executing query: {e}")

if __name__ == "__main__":
    # Define test parameters
    test_station = "Berlin Hauptbahnhof"
    test_lat = 52.5200  # Near Berlin TV Tower
    test_lon = 13.4050
    # Choose a snapshot time that definitely exists in your DimTime
    # (e.g., one of the first folders you processed)
    test_snapshot = "2025-09-05 12:00" 

    # Execute all tasks
    run_query("task2/query_2_1.sql", {"station_name": test_station})
    run_query("task2/query_2_2.sql", {"input_lat": test_lat, "input_lon": test_lon})
    run_query("task2/query_2_3.sql", {"snapshot_time": test_snapshot})
    run_query("task2/query_2_4.sql", {"station_name": test_station})