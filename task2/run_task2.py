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
    # Task 2.1 (Given a station name, return its coordinates and identifier.)
    station_coord = "Alexanderplatz"
    print(f"Task 2.1 - Query station: {station_coord}")
    
    # Task 2.2 (Given coordinates, find the nearest station.)
    test_lat = 52.5200  # Near Berlin TV Tower
    test_lon = 13.4050
    print(f"Task 2.2 - Query coordinates: lat={test_lat}, lon={test_lon}")
    
    # Task 2.3 (Given a timesnapshot, return the total number of canceled trains.)
    test_date = "2025-09-12"
    test_hour = 12
    print(f"Task 2.3 - Query date/time: {test_date} at hour {test_hour}")
    
    # Task 2.4 (Given a station name, return the average delay of arriving trains over the past month.)
    station_delay = "Berlin Hauptbahnhof"
    print(f"Task 2.4 - Query station: {station_delay}")

    # Execute all tasks
    run_query("task2/query_2_1.sql", {"station_name": station_coord})
    run_query("task2/query_2_2.sql", {"input_lat": test_lat, "input_lon": test_lon})
    run_query("task2/query_2_3.sql", {
        "target_date": test_date, 
        "target_hour": test_hour
    })
    run_query("task2/query_2_4.sql", {"station_name": station_delay})