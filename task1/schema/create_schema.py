import psycopg2
from db_config import get_database_url

DATABASE_URL = get_database_url()

def init_db():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Read the single SQL file
        with open('task1/schema/star_schema.sql', 'r') as f:
            sql_commands = f.read()
            
        # Execute it
        cur.execute(sql_commands)
        conn.commit()
        print("Schema created successfully.")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    init_db()