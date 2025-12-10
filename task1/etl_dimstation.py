import pandas as pd
import json
from sqlalchemy import create_engine, text
from db_config import get_database_url

DATABASE_URL = get_database_url()

JSON_FILE_NAME = "dataset/station_data.json"

# Core Dimension Loading Logic

def extract_station_data(station):
    """
    Extracts and flattens required fields from a single station JSON object.
    Uses JSON as the authoritative (true) source for metadata.
    """
    
    # Find main EVA number entry
    main_eva = next(
        (eva for eva in station.get('evaNumbers', []) if eva.get('isMain')),
        None
    )
    
    if not main_eva:
        return None
    
    station_id = main_eva.get('number')
    if not station_id:
        return None
    
    # Coordinates are nested under geographicCoordinates
    coords = main_eva.get('geographicCoordinates', {}).get('coordinates', [None, None])
    
    zipcode = station.get('mailingAddress', {}).get('zipcode')
    station_name = station.get('name')
    
    # Ensure has_wifi is Boolean for the database
    has_wifi = station.get('hasWiFi')
    if isinstance(has_wifi, str):
        has_wifi = has_wifi.lower() == 'true'
    
    return {
        'station_id': station_id,
        'station_name': station_name,
        # Note: JSON is typically Lon, Lat. We map to Lat, Lon for database convention.
        'latitude': coords[1],
        'longitude': coords[0],
        'ifopt_id': station.get('ifopt'),
        'category': station.get('category'),
        'price_category': station.get('priceCategory'),
        'zipcode': zipcode,
        'has_stepless_access': station.get('hasSteplessAccess'),
        'has_wifi': has_wifi
    }

# Main Execution

def load_dim_station_pipeline():
    """Main ETL pipeline to load DimStation from JSON."""
    print("🚉 Starting ETL for DimStation (JSON Metadata only)...")
    
    # 1. Load and transform JSON
    try:
        with open(JSON_FILE_NAME, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        station_list = data.get('result', [])
        transformed = [extract_station_data(s) for s in station_list]
        transformed = [d for d in transformed if d is not None]
        
        df = pd.DataFrame(transformed)
        
        # Enforce data types for robustness
        df['station_id'] = df['station_id'].astype('Int64')
        df['category'] = df['category'].astype('Int64')
        df['price_category'] = df['price_category'].astype('Int64')
        df['has_wifi'] = df['has_wifi'].astype(bool)
        
    except Exception as e:
        print(f"Error reading JSON: {e}")
        return
    
    # 2. Load to database
    try:
        engine = create_engine(DATABASE_URL)
        
        # Use TRUNCATE CASCADE to safely clear the DimStation table
        with engine.begin() as conn:
            # Enable the pg_trgm extension here, as it's needed for the next step's fuzzy search.
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            print("Clearing existing DimStation data via TRUNCATE CASCADE...")
            conn.execute(text("TRUNCATE TABLE dimstation CASCADE"))
        
        # Load DimStation (Clean JSON names)
        print(f"Loading {len(df)} stations into DimStation...")
        df.to_sql('dimstation', engine, if_exists='append', index=False)
        
        print("\n✅ DimStation populated successfully!")
        
    except Exception as e:
        print(f"\n❌ Database error during DimStation ETL: {e}")

if __name__ == '__main__':
    load_dim_station_pipeline()