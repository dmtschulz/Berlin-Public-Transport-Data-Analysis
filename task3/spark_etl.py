import os
import xml.etree.ElementTree as ET
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, FloatType, DateType
from pyspark.sql import functions as F

DATA_FOLDER = "dataset"
OUTPUT_FOLDER = "dataset/parquet_out"

# --- Configuration ---
spark = SparkSession.builder \
    .appName("BerlinTransportETL") \
    .config("spark.driver.memory", "4g") \
    .master("local[*]") \
    .getOrCreate()

# --- Station Name Mapping (Consistency with Task 1) ---
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

# --- Helper Functions (Run on Worker Nodes) ---

def parse_timestamp(ts_str):
    """Parses YYMMDDHHMM string to datetime object."""
    if not ts_str:
        return None
    try:
        return datetime.strptime(ts_str, '%y%m%d%H%M')
    except ValueError:
        return None

def normalize_station_name(raw_name):
    """Applies manual overrides to match DB schema."""
    if not raw_name:
        return None
    return MANUAL_XML_TO_JSON.get(raw_name, raw_name)

def parse_xml_content(file_data):
    """
    Input: (filename, content_string)
    Output: List of tuples representing movement events
    """
    filename, content = file_data
    events = []
    
    try:
        root = ET.fromstring(content)
        raw_station_name = root.attrib.get('station')
        if not raw_station_name:
            return []

        # --- FIX: Normalize Name ---
        station_name = normalize_station_name(raw_station_name)

        # Determine file type based on filename suffix
        is_change_file = filename.endswith("_change.xml")

        for s_tag in root.findall('s'):
            stop_id = s_tag.attrib.get('id')
            
            # Identify Train (Timeline ID)
            tl = s_tag.find('tl')
            train_id = None
            if tl is not None:
                # Composite ID: Category + Number
                c = tl.attrib.get('c', 'Unknown')
                n = tl.attrib.get('n', '0')
                train_id = f"{c}-{n}"
            
            # --- ARRIVALS ---
            ar = s_tag.find('ar')
            if ar is not None:
                if is_change_file:
                    # Update Event
                    ct = parse_timestamp(ar.attrib.get('ct'))
                    canceled = (ar.attrib.get('c') == 'c')
                    events.append(((station_name, stop_id, train_id, True), 
                                   {'actual_time': ct, 'is_canceled': canceled, 'type': 'update'}))
                else:
                    # Planned Event
                    pt = parse_timestamp(ar.attrib.get('pt'))
                    events.append(((station_name, stop_id, train_id, True), 
                                   {'planned_time': pt, 'type': 'plan'}))

            # --- DEPARTURES ---
            dp = s_tag.find('dp')
            if dp is not None:
                if is_change_file:
                    ct = parse_timestamp(dp.attrib.get('ct'))
                    canceled = (dp.attrib.get('c') == 'c')
                    events.append(((station_name, stop_id, train_id, False), 
                                   {'actual_time': ct, 'is_canceled': canceled, 'type': 'update'}))
                else:
                    pt = parse_timestamp(dp.attrib.get('pt'))
                    events.append(((station_name, stop_id, train_id, False), 
                                   {'planned_time': pt, 'type': 'plan'}))
                                   
    except Exception:
        # Malformed XMLs are skipped
        pass
        
    return events

def merge_events(a, b):
    """Reducer function to merge Planned and Update dictionaries."""
    merged = a.copy()
    merged.update(b)
    return merged

# --- Main Pipeline ---

if __name__ == "__main__":
    print("🚀 Starting Spark ETL Job (With Name Normalization)...")

    # 1. Read Timetables (Planned)
    planned_rdd = spark.sparkContext.wholeTextFiles(os.path.join(DATA_FOLDER, "timetables/*/*.xml"))
    
    # 2. Read Changes (Updates)
    changes_rdd = spark.sparkContext.wholeTextFiles(os.path.join(DATA_FOLDER, "timetable_changes/*/*.xml"))

    # 3. Union and Parse
    all_raw_rdd = planned_rdd.union(changes_rdd).flatMap(parse_xml_content)

    # 4. Reduce / Merge
    merged_rdd = all_raw_rdd.reduceByKey(merge_events)

    # 5. Transform to Row format for DataFrame
    def to_row(record):
        (station, stop_id, train_id, is_arrival), data = record
        
        pt = data.get('planned_time')
        at = data.get('actual_time')
        canceled = data.get('is_canceled', False)
        
        # Calculate Delay (Minutes)
        delay = 0.0
        if pt and at:
            # Difference in seconds / 60
            delay = (at - pt).total_seconds() / 60.0
        
        # Partition Date
        ref_time = pt if pt else at
        evt_date = ref_time.date() if ref_time else None

        return (station, stop_id, train_id, is_arrival, pt, at, float(delay), canceled, evt_date)

    # Define Schema
    schema = StructType([
        StructField("station_name", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("train_id", StringType(), True),
        StructField("is_arrival", BooleanType(), True),
        StructField("planned_time", TimestampType(), True),
        StructField("actual_time", TimestampType(), True),
        StructField("delay_minutes", FloatType(), True),
        StructField("is_canceled", BooleanType(), True),
        StructField("event_date", DateType(), True)
    ])

    final_df = spark.createDataFrame(merged_rdd.map(to_row), schema=schema)

    # 6. Write to Parquet (Partitioned by Date)
    print("💾 Writing to Parquet...")
    final_df \
        .filter(F.col("event_date").isNotNull()) \
        .write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(OUTPUT_FOLDER)

    print(f"✅ ETL Complete! Data saved to {OUTPUT_FOLDER}")
    spark.stop()