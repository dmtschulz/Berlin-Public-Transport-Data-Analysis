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
TIMETABLE_DIR = os.path.join(DATA_FOLDER, "timetables")

# Map cases that Fuzzy matching cannot solve.
MANUAL_XML_TO_JSON = {
    # 1. Simply because it is too short for fuzzy match (Hbf vs Hauptbahnhof)
    "Berlin Hbf": "Berlin Hauptbahnhof",
    # 2. Distinct stations with same base name
    "Berlin Yorckstr.(S1)": "Yorckstraße (Großgörschenstraße)",
    "Berlin Yorckstr.(S2)": "Yorckstraße"
}

# Database Setup
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Caching
station_cache = {}  # { 'XML_Name': station_id }
train_cache = set() # { 'train_id' }
time_cache = {}     # { datetime_obj: time_id }

# Helper Functions

def parse_db_timestamp(ts_string):
    """Parses YYMMDDHHMM strings from XML attributes."""
    if not ts_string:
        return None
    return datetime.strptime(ts_string, '%y%m%d%H%M')

def parse_folder_timestamp(folder_name):
    """Parses YYMMDDHHMM strings from folder names."""
    return datetime.strptime(folder_name, '%y%m%d%H%M')

def get_station_id(session, xml_name):
    """
    Resolves XML station name to DB station_id.
    Strategy:
    1. Check Cache.
    2. Check Manual Map (for special cases like Yorckstr).
    3. Clean Name (Remove 'Berlin ' prefix).
    4. Exact Match (on Clean Name).
    5. Fuzzy Match (on Clean Name).
    """
    # 1. Check Cache
    if xml_name in station_cache:
        return station_cache[xml_name]
    
    # 2. Check Manual Overrides first
    if xml_name in MANUAL_XML_TO_JSON:
        target_name = MANUAL_XML_TO_JSON[xml_name]
        # Fetch ID for the manual target
        query = text("SELECT station_id FROM DimStation WHERE station_name = :name")
        res = session.execute(query, {"name": target_name}).scalar_one_or_none()
        if res:
            station_cache[xml_name] = res
            return res
        else:
            print(f"⚠ Critical: Manual mapping '{target_name}' not found in DB!")
            return None

    # 3. Cleaning: Remove "Berlin " prefix if present
    if xml_name.startswith("Berlin "):
        search_name = xml_name[7:].strip() # Remove first 7 chars ("Berlin ")
    else:
        search_name = xml_name

    # 3. Try Exact Match (Fastest) using Clean Name
    # e.g. If XML is "Berlin Alexanderplatz", search_name is "Alexanderplatz", which hits exactly.
    query_exact = text("SELECT station_id FROM DimStation WHERE station_name = :name")
    res = session.execute(query_exact, {"name": search_name}).scalar_one_or_none()
    
    # Exact Match Found
    if res:
        station_cache[xml_name] = res
        return res

    # 4. Fuzzy Match (Fallback) using pg_trgm similarity
    # e.g. "Attilastr." (Clean XML) vs "Attilastraße" (DB) -> High Similarity
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

def ensure_train_exists(session, category, number, owner):
    """Inserts train into DimTrain if not exists."""
    train_id = f"{category}-{number}"
    
    if train_id in train_cache:
        return train_id

    query = text("""
        INSERT INTO DimTrain (train_id, line_category, train_number, owner_id)
        VALUES (:tid, :cat, :num, :own)
        ON CONFLICT (train_id) DO NOTHING
    """)
    
    session.execute(query, {
        "tid": train_id, 
        "cat": category, 
        "num": number, 
        "own": owner
    })
    
    train_cache.add(train_id)
    return train_id

def get_time_id(session, dt_obj):
    """Fetches time_id from DimTime for given datetime object."""
    if dt_obj in time_cache:
        return time_cache[dt_obj]
    
    query = text("""
        SELECT time_id FROM DimTime 
        WHERE date = :d AND hour = :h AND minute = :m
    """)
    res = session.execute(query, {
        "d": dt_obj.date(),
        "h": dt_obj.hour,
        "m": dt_obj.minute
    }).scalar_one_or_none()
    
    if res:
        time_cache[dt_obj] = res
        return res
    return None

# Main Processing Logic

def process_timetables():
    print("🚀 Starting Timetable Ingestion...")
    
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm")) # Ensure pg_trgm is available

    session = Session()
    
    if not os.path.exists(TIMETABLE_DIR):
        print(f"Error: Directory {TIMETABLE_DIR} not found.")
        return

    folders = sorted(os.listdir(TIMETABLE_DIR))
    total_folders = len(folders)
    print(f"Found {total_folders} snapshot folders.")

    try:
        for idx, folder_name in enumerate(folders):
            folder_path = os.path.join(TIMETABLE_DIR, folder_name)
            
            if not os.path.isdir(folder_path):
                continue
                
            try:
                snapshot_dt = parse_folder_timestamp(folder_name)
            except ValueError:
                continue
                
            time_fk = get_time_id(session, snapshot_dt)
            if not time_fk:
                print(f"⚠ Warning: Skipping folder {folder_name} (Time ID not found)") # Outside time dimension range
                continue

            xml_files = glob.glob(os.path.join(folder_path, "*_timetable.xml"))
            batch_movements = []
            
            for xml_file in xml_files:
                try:
                    tree = ET.parse(xml_file)
                    root = tree.getroot()
                    
                    xml_station_name = root.attrib.get('station')
                    if not xml_station_name:
                        continue

                    # Resolve Station ID
                    station_fk = get_station_id(session, xml_station_name)
                    if not station_fk:
                        print(f"⚠ Skipping {os.path.basename(xml_file)}: Unresolvable station '{xml_station_name}'")
                        continue

                    for s_tag in root.findall('s'):
                        stop_id = s_tag.attrib.get('id')
                        
                        tl = s_tag.find('tl')
                        if tl is None: continue
                        
                        t_cat = tl.attrib.get('c')
                        t_num = tl.attrib.get('n')
                        t_owner = tl.attrib.get('o')

                        # SKIP BUSES
                        if t_cat == "Bus": 
                            continue
                        
                        train_fk = ensure_train_exists(session, t_cat, t_num, t_owner)
                        
                        ar = s_tag.find('ar') # arrival
                        if ar is not None:
                            batch_movements.append({
                                "station_id": station_fk,
                                "time_id": time_fk,
                                "train_id": train_fk,
                                "stop_id": stop_id,
                                "is_arrival": True,
                                "planned_platform": ar.attrib.get('pp'),
                                "planned_time": parse_db_timestamp(ar.attrib.get('pt')),
                                "is_canceled": ar.attrib.get('cs') == 'c' and ar.attrib.get('clt') is not None
                            })

                        dp = s_tag.find('dp') # departure
                        if dp is not None:
                            batch_movements.append({
                                "station_id": station_fk,
                                "time_id": time_fk,
                                "train_id": train_fk,
                                "stop_id": stop_id,
                                "is_arrival": False,
                                "planned_platform": dp.attrib.get('pp'),
                                "planned_time": parse_db_timestamp(dp.attrib.get('pt')),
                                "is_canceled": dp.attrib.get('cs') == 'c' and dp.attrib.get('clt') is not None
                            })
                            
                except Exception as e:
                    print(f"Error parsing {os.path.basename(xml_file)}: {e}")

            if batch_movements:
                insert_stmt = text("""
                    INSERT INTO FactTrainMovement (
                        station_id, time_id, train_id, stop_id, is_arrival, 
                        planned_platform, planned_time, is_canceled
                    ) VALUES (
                        :station_id, :time_id, :train_id, :stop_id, :is_arrival, 
                        :planned_platform, :planned_time, :is_canceled
                    )
                    ON CONFLICT (station_id, time_id, stop_id, is_arrival) DO NOTHING
                """)
                session.execute(insert_stmt, batch_movements)
                session.commit()
                print(f"[{idx+1}/{total_folders}] Processed {folder_name}: {len(batch_movements)} movements.")
            else:
                 print(f"[{idx+1}/{total_folders}] Processed {folder_name}: No valid train movements found.")

    except KeyboardInterrupt:
        print("\nStopping ingestion...")
    finally:
        session.close()
        print("✅ Planned Timetable Ingestion Complete.")

if __name__ == "__main__":
    process_timetables()