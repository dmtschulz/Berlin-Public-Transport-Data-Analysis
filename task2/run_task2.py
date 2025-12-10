import os
from sqlalchemy import create_engine, text
from db_config import get_database_url

DATABASE_URL = get_database_url()

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
    # test_station = "Berlin Hauptbahnhof"
    test_station = "Alexanderplatz"
    test_lat = 52.5200  # Near Berlin TV Tower
    test_lon = 13.4050
    # Choose a snapshot time that definitely exists in DimTime
    # (e.g., one of the first folders)
    test_date = "2025-10-03"
    test_hour = 12

    # Execute all tasks
    run_query("task2/query_2_1.sql", {"station_name": test_station})
    run_query("task2/query_2_2.sql", {"input_lat": test_lat, "input_lon": test_lon})
    run_query("task2/query_2_3.sql", {
        "target_date": test_date, 
        "target_hour": test_hour
    })
    run_query("task2/query_2_4.sql", {"station_name": test_station})