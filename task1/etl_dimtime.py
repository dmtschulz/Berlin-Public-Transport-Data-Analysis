import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
from db_config import get_database_url

DATABASE_URL = get_database_url()

# Date range from exercise document
START_DATE = '2025-09-02'
END_DATE = '2025-10-15'

def load_dim_time():
    """
    Generates time slices at 15-minute intervals for the period 
    and loads them into DimTime, aligning with the timetable change frequency.
    """
    print("🚉 Starting ETL for DimTime...")

    # --- KEY CHANGE 1: Generate 15-minute intervals ---
    # We use '15min' frequency. Since timetable changes are at HH:01, HH:16, etc., 
    # we might need to adjust the generated times later if we only want those specific minutes.
    # For a comprehensive dimension, 15-minute resolution is a good choice.
    date_range = pd.date_range(start=START_DATE, end=END_DATE, freq='15min')
    df = pd.DataFrame({'datetime': date_range})

    # Calculate dimension attributes
    df['date'] = df['datetime'].dt.date
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    df['day'] = df['datetime'].dt.day
    df['day_of_week'] = df['datetime'].dt.day_name()
    df['hour'] = df['datetime'].dt.hour
    df['minute'] = df['datetime'].dt.minute
    
    # Weekend: Saturday (5) or Sunday (6) in 0-indexed dayofweek
    df['is_weekend'] = df['datetime'].dt.dayofweek >= 5

    # Peak hours: 07:00:00 to 09:00:00 AND 17:00:00 to 19:00:00
    peak_morning = (df['hour'] >= 7) & (df['hour'] <= 9)
    peak_evening = (df['hour'] >= 17) & (df['hour'] <= 19)
    df['is_peak_hour'] = peak_morning | peak_evening
    
    # Resetting the sequential time_id if we want it to be a surrogate key starting from 1
    # Note: Since DimTime uses SERIAL, we can drop this line and let the DB handle it,
    # but for local DF consistency, we can keep it.
    df['time_id'] = np.arange(1, len(df) + 1)
    
    # Select and reorder columns
    df_final = df[[
        # PK is SERIAL, so we can omit time_id here if preferred, but including it for clarity
        'time_id', 'date', 'year', 'month', 'day', 
        'day_of_week', 'hour', 'minute', 'is_peak_hour', 'is_weekend'
    ]].copy()
    
    # Enforce data types
    df_final['time_id'] = df_final['time_id'].astype('int64')
    df_final['year'] = df_final['year'].astype('int64')
    df_final['month'] = df_final['month'].astype('int64')
    df_final['day'] = df_final['day'].astype('int64')
    df_final['hour'] = df_final['hour'].astype('int64')
    df_final['minute'] = df_final['minute'].astype('int64')
    df_final['is_peak_hour'] = df_final['is_peak_hour'].astype(bool)
    df_final['is_weekend'] = df_final['is_weekend'].astype(bool)
    
    print(f"Generated {len(df_final)} 15-minute interval records.")

    # Insert into PostgreSQL
    try:
        engine = create_engine(DATABASE_URL)
        
        # Ensure pg_trgm extension is active
        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            # Clear table before loading if we're generating all possible slots
            conn.execute(text("TRUNCATE TABLE dimtime RESTART IDENTITY CASCADE"))
            print("✓ Cleared existing DimTime data and restarted identity sequence.")

        # Load to database
        print("Loading data to PostgreSQL...")
        # Omitting the 'time_id' column if using the SERIAL PK in the database
        df_final.drop(columns=['time_id']).to_sql(
            'dimtime', engine, if_exists='append', index=False, method='multi', chunksize=1000
        )
        
        # Verify insertion
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM dimtime")).scalar()
            print(f"✓ Verified {count} records in table")
            
        print("\nPreview (first 5 records):")
        # Sample the DF to show the minute granularity
        print(df_final.head()[['day_of_week', 'hour', 'minute', 'is_peak_hour']].to_string(index=False))
        
        print("\n✓ ETL completed successfully")

    except Exception as e:
        print(f"\n✗ Database error: {e}")
        print("Hint: Ensure PostgreSQL is running and connection details are correct")

if __name__ == '__main__':
    load_dim_time()