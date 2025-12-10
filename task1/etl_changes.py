import os
import glob
import xml.etree.ElementTree as ET
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from db_config import get_database_url

DATABASE_URL = get_database_url()

# Path to the dataset root
DATA_FOLDER = "dataset"
CHANGE_DIR = os.path.join(DATA_FOLDER, "timetable_changes")

# Manual Overrides (Reduced List). Consistent with etl_timetables.py
MANUAL_XML_TO_JSON = {
    # 1. Too short for fuzzy match
    "Berlin Hbf": "Berlin Hauptbahnhof",
    # 2. Distinct Platforms (S1/S2 logic)
    "Berlin Yorckstr.(S1)": "Yorckstraße (Großgörschenstraße)",
    "Berlin Yorckstr.(S2)": "Yorckstraße"
}

# Database Setup
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Caching
station_cache = {} 

# Helper Functions

def parse_db_timestamp(ts_string):
    """Parses YYMMDDHHMM strings from XML attributes."""
    if not ts_string:
        return None
    return datetime.strptime(ts_string, '%y%m%d%H%M')

def get_station_id(session, xml_name):
    """
    Resolves XML station name to DB station_id.
    Strategy:
    1. Check Cache.
    2. Check Manual Map.
    3. Clean Name (Remove 'Berlin ' prefix).
    4. Exact Match (Clean Name).
    5. Fuzzy Match (Clean Name).
    """
    # 1. Check Cache
    if xml_name in station_cache:
        return station_cache[xml_name]
    
    # 2. Check Manual Overrides
    if xml_name in MANUAL_XML_TO_JSON:
        target_name = MANUAL_XML_TO_JSON[xml_name]
        query = text("SELECT station_id FROM DimStation WHERE station_name = :name")
        res = session.execute(query, {"name": target_name}).scalar_one_or_none()
        if res:
            station_cache[xml_name] = res
            return res
        else:
            return None

    # 3. Cleaning: Remove "Berlin " prefix if present
    if xml_name.startswith("Berlin "):
        search_name = xml_name[7:].strip()
    else:
        search_name = xml_name

    # 4. Try Exact Match (Fastest) using Clean Name
    query_exact = text("SELECT station_id FROM DimStation WHERE station_name = :name")
    res = session.execute(query_exact, {"name": search_name}).scalar_one_or_none()
    
    if res:
        station_cache[xml_name] = res
        return res

    # 5. Fuzzy Match (Fallback)
    query_fuzzy = text(f"""
        SELECT station_id
        FROM DimStation
        WHERE similarity(station_name, :search_name) > 0.4
        ORDER BY similarity(station_name, :search_name) DESC
        LIMIT 1;
    """)
    
    res = session.execute(query_fuzzy, {"search_name": search_name}).scalar_one_or_none()
    
    if res:
        station_cache[xml_name] = res
        return res
    
    print("⚠ No match for station!", xml_name)
    return None

def process_changes():
    print("🚀 Starting Timetable Updates...")
    
    # Enable fuzzy matching extension just in case
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))

    session = Session()
    
    if not os.path.exists(CHANGE_DIR):
        print(f"Error: Directory {CHANGE_DIR} not found.")
        return

    folders = sorted(os.listdir(CHANGE_DIR))
    total_folders = len(folders)
    print(f"Found {total_folders} update folders.")

    try:
        for idx, folder_name in enumerate(folders):
            folder_path = os.path.join(CHANGE_DIR, folder_name)
            
            if not os.path.isdir(folder_path):
                continue
                
            xml_files = glob.glob(os.path.join(folder_path, "*_change.xml"))
            updates_count = 0
            
            for xml_file in xml_files:
                try:
                    tree = ET.parse(xml_file)
                    root = tree.getroot()
                    
                    xml_station_name = root.attrib.get('station')
                    if not xml_station_name: continue

                    station_fk = get_station_id(session, xml_station_name)
                    if not station_fk: continue

                    for s_tag in root.findall('s'):
                        stop_id = s_tag.attrib.get('id')
                        
                        # Processing Arrival Changes
                        ar = s_tag.find('ar')
                        if ar is not None:
                            ct = ar.attrib.get('ct') # Changed Time
                            cp = ar.attrib.get('cp') # Changed Platform
                            c_status = ar.attrib.get('cs') # Cancellation Status

                            if ct or cp or c_status:
                                actual_time = parse_db_timestamp(ct) if ct else None
                                
                                update_query = text("""
                                    UPDATE FactTrainMovement
                                    SET 
                                        actual_time = COALESCE(:actual_time, actual_time),
                                        actual_platform = COALESCE(:actual_platform, actual_platform),
                                        is_canceled = CASE WHEN :is_canceled THEN TRUE ELSE is_canceled END,
                                        delay_minutes = CASE 
                                            WHEN :actual_time IS NOT NULL THEN 
                                                EXTRACT(EPOCH FROM (:actual_time - planned_time)) / 60 
                                            ELSE delay_minutes 
                                        END
                                    WHERE station_id = :station_id 
                                      AND stop_id = :stop_id 
                                      AND is_arrival = TRUE
                                """)
                                
                                result = session.execute(update_query, {
                                    "actual_time": actual_time,
                                    "actual_platform": cp,
                                    "is_canceled": (c_status == 'c'),
                                    "station_id": station_fk,
                                    "stop_id": stop_id
                                })
                                updates_count += result.rowcount

                        # Processing Departure Changes
                        dp = s_tag.find('dp')
                        if dp is not None:
                            ct = dp.attrib.get('ct')
                            cp = dp.attrib.get('cp')
                            c_status = dp.attrib.get('cs')

                            if ct or cp or c_status:
                                actual_time = parse_db_timestamp(ct) if ct else None
                                
                                update_query = text("""
                                    UPDATE FactTrainMovement
                                    SET 
                                        actual_time = COALESCE(:actual_time, actual_time),
                                        actual_platform = COALESCE(:actual_platform, actual_platform),
                                        is_canceled = CASE WHEN :is_canceled THEN TRUE ELSE is_canceled END,
                                        delay_minutes = CASE 
                                            WHEN :actual_time IS NOT NULL THEN 
                                                EXTRACT(EPOCH FROM (:actual_time - planned_time)) / 60 
                                            ELSE delay_minutes 
                                        END
                                    WHERE station_id = :station_id 
                                      AND stop_id = :stop_id 
                                      AND is_arrival = FALSE
                                """)
                                
                                result = session.execute(update_query, {
                                    "actual_time": actual_time,
                                    "actual_platform": cp,
                                    "is_canceled": (c_status == 'c'),
                                    "station_id": station_fk,
                                    "stop_id": stop_id
                                })
                                updates_count += result.rowcount

                except Exception as e:
                    print(f"Error processing {os.path.basename(xml_file)}: {e}")

            session.commit()
            if updates_count > 0:
                print(f"[{idx+1}/{total_folders}] {folder_name}: Updated {updates_count} movements.")

    except KeyboardInterrupt:
        print("\nStopping updates...")
    finally:
        session.close()
        print("✅ Timetable Changes/Updates Complete.")

if __name__ == "__main__":
    process_changes()