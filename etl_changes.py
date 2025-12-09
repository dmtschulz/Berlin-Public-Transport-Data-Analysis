import os
import glob
import xml.etree.ElementTree as ET
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# --- Configuration ---
DB_USER = "postgres"
DB_PASS = "123456"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "dia_db"
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DATA_FOLDER = "dataset"
CHANGE_DIR = os.path.join(DATA_FOLDER, "timetable_changes")

# Reuse the same manual overrides for consistency
MANUAL_XML_TO_JSON = {
    "Berlin Attilastr.": "Attilastraße",
    "Berlin Storkower Str": "Storkower Straße",
    "Berlin Feuerbachstr.": "Feuerbachstraße",
    "Berlin Poelchaustr.": "Poelchaustraße",
    "Berlin Messe Nord/ZOB (Witzleben)": "Messe Nord / ZOB",
    "Berlin Sundgauer Str": "Sundgauer Straße",
    "Berlin Hbf": "Berlin Hauptbahnhof",
    "Berlin Yorckstr.(S1)": "Yorckstraße",
    "Berlin Yorckstr.(S2)": "Yorckstraße"
}

# --- Database Setup ---
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# --- Caching ---
station_cache = {} 

# --- Helper Functions ---

def parse_db_timestamp(ts_string):
    """Parses YYMMDDHHMM strings from XML attributes."""
    if not ts_string:
        return None
    return datetime.strptime(ts_string, '%y%m%d%H%M')

def get_station_id(session, xml_name):
    """Resolves XML station name to DB station_id."""
    if xml_name in station_cache:
        return station_cache[xml_name]
    
    search_name = MANUAL_XML_TO_JSON.get(xml_name, xml_name)
    
    query_exact = text("SELECT station_id FROM DimStation WHERE station_name = :name")
    res = session.execute(query_exact, {"name": search_name}).scalar_one_or_none()
    
    if res:
        station_cache[xml_name] = res
        return res

    query_fuzzy = text(f"""
        SELECT station_id
        FROM DimStation
        WHERE similarity(station_name, :xml_name) > 0.4
        ORDER BY similarity(station_name, :xml_name) DESC
        LIMIT 1;
    """)
    
    res = session.execute(query_fuzzy, {"xml_name": xml_name}).scalar_one_or_none()
    
    if res:
        station_cache[xml_name] = res
        return res
    
    return None

def process_changes():
    print("🚀 Starting Timetable Updates (Delays/Cancellations)...")
    
    session = Session()
    
    if not os.path.exists(CHANGE_DIR):
        print(f"Error: Directory {CHANGE_DIR} not found.")
        return

    # Sort folders to apply updates chronologically (HH:01 -> HH:16 -> HH:31 -> HH:46)
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
                        
                        # --- Processing Arrival Changes ---
                        ar = s_tag.find('ar')
                        if ar is not None:
                            ct = ar.attrib.get('ct') # Changed Time
                            cp = ar.attrib.get('cp') # Changed Platform
                            c_status = ar.attrib.get('c') # Cancellation Status

                            if ct or cp or c_status:
                                actual_time = parse_db_timestamp(ct) if ct else None
                                
                                # Only update if we find a matching Planned movement
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
                                    "is_canceled": (c_status == 'c'), # Explicitly check for 'c' string
                                    "station_id": station_fk,
                                    "stop_id": stop_id
                                })
                                updates_count += result.rowcount

                        # --- Processing Departure Changes ---
                        dp = s_tag.find('dp')
                        if dp is not None:
                            ct = dp.attrib.get('ct')
                            cp = dp.attrib.get('cp')
                            c_status = dp.attrib.get('c')

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
            # Else: No updates (likely because bus updates were skipped or no changes occurred)

    except KeyboardInterrupt:
        print("\nStopping updates...")
    finally:
        session.close()
        print("✅ Timetable Updates Complete.")

if __name__ == "__main__":
    process_changes()