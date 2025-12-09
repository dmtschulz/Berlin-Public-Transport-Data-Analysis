import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, hour, lit

# --- Configuration ---
spark = SparkSession.builder \
    .appName("BerlinTransportAnalysis") \
    .master("local[*]") \
    .getOrCreate()

INPUT_FOLDER = "dataset/parquet_out"

def run_analysis():
    print("📂 Loading Parquet Data...")
    df = spark.read.parquet(INPUT_FOLDER)
    
    # Cache for performance as we use it twice
    df.cache()

    # ---------------------------------------------------------
    # Task 3.2: Average Daily Delay for a given station
    # ---------------------------------------------------------
    target_station = "Berlin Hauptbahnhof"
    print(f"\n--- Task 3.2: Average Daily Delay for {target_station} ---")
    
    # Logic: Filter Station -> Group by Date -> Avg(Delay)
    # We focus on Arrivals for delay metrics usually, or both.
    daily_delay = df.filter(col("station_name") == target_station) \
                    .filter(col("is_canceled") == False) \
                    .groupBy("event_date") \
                    .agg(avg("delay_minutes").alias("avg_daily_delay")) \
                    .orderBy("event_date")

    daily_delay.show(5)

    # ---------------------------------------------------------
    # Task 3.3: Average Departures in Peak Hours
    # ---------------------------------------------------------
    print("\n--- Task 3.3: Avg Departures per Station (Peak Hours) ---")

    # Logic: 
    # 1. Filter Departures (is_arrival = False)
    # 2. Filter Peak Hours (7-9, 17-19) based on PLANNED time
    # 3. Count per Station/Date -> Average over dates
    
    peak_hours_filter = (
        ((hour(col("planned_time")) >= 7) & (hour(col("planned_time")) < 9)) | 
        ((hour(col("planned_time")) >= 17) & (hour(col("planned_time")) < 19))
    )

    peak_departures = df.filter(col("is_arrival") == False) \
                        .filter(peak_hours_filter) \
                        .groupBy("station_name", "event_date") \
                        .count() \
                        .withColumnRenamed("count", "daily_peak_departures") \
                        .groupBy("station_name") \
                        .agg(avg("daily_peak_departures").alias("avg_peak_departures")) \
                        .orderBy(col("avg_peak_departures").desc())

    peak_departures.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    run_analysis()