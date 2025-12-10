-- create_schema.sql
-- Master script to initialize the Star Schema for Berlin Public Transport Data

-- =========================================================
-- 1. CREATE DIMENSION TABLES
-- =========================================================

-- DimStation: Contains static metadata for the 133 Berlin stations.
CREATE TABLE DimStation (
    station_id INTEGER PRIMARY KEY,         -- PK: e.g., 8011003
    station_name VARCHAR(255) NOT NULL,     -- e.g., 'Ahrensfelde'
    latitude NUMERIC(9,6) NOT NULL,         -- e.g., 52.571375
    longitude NUMERIC(9,6) NOT NULL,        -- e.g., 13.565154
    ifopt_id VARCHAR(25),                   -- e.g., 'de:11000:900170004'
    category INTEGER,                       -- e.g., 4
    price_category INTEGER,                 -- e.g., 4
    zipcode VARCHAR(5),                     -- e.g., '12689'
    has_stepless_access VARCHAR(10),        -- e.g., 'yes', 'no', 'partial'
    has_wifi BOOLEAN                        -- e.g., FALSE
);

-- DimTime: Contains attributes related to time slices (day, hour, peak status).
CREATE TABLE DimTime (
    time_id SERIAL PRIMARY KEY,             -- PK: Auto-incrementing ID
    date DATE NOT NULL,                     -- The calendar date
    year INTEGER NOT NULL,                  -- The year (e.g., 2025)
    month INTEGER NOT NULL,                 -- The month number (1-12)
    day INTEGER NOT NULL,                   -- The day of the month (1-31)
    day_of_week VARCHAR(9) NOT NULL,        -- Full name of the day (e.g., 'Sunday')
    hour INTEGER NOT NULL,                  -- The hour of the day (0-23)
    minute INTEGER NOT NULL,                -- Minute (0-59) - Added for 15-min granularity
    is_peak_hour BOOLEAN NOT NULL,          -- TRUE if between 07:00-09:00 or 17:00-19:00
    is_weekend BOOLEAN NOT NULL             -- TRUE if Saturday or Sunday
);

-- DimTrain: Contains unique identifiers for lines/trips found in the XML files.
CREATE TABLE DimTrain (
    train_id VARCHAR(60) PRIMARY KEY,       -- PK: Composite ID (Category + Number, e.g., 'RE-73768')
    line_category VARCHAR(10) NOT NULL,     -- e.g., 'RE', 'RB', 'IC'
    train_number INTEGER NOT NULL,          -- e.g., 73768
    owner_id VARCHAR(20)                    -- Owner ID (o tag in XML)
);

-- =========================================================
-- 2. CREATE FACT TABLE
-- =========================================================

-- FactTrainMovement: Central table for all planned and actual train movements.
CREATE TABLE FactTrainMovement (
    movement_pk SERIAL PRIMARY KEY,         -- PK: Auto-incrementing Surrogate Key
    
    -- Foreign Keys to Dimensions
    station_id INTEGER NOT NULL REFERENCES DimStation(station_id),
    time_id INTEGER NOT NULL REFERENCES DimTime(time_id),
    train_id VARCHAR(60) NOT NULL REFERENCES DimTrain(train_id),
    
    -- Movement Identification
    stop_id VARCHAR(100) NOT NULL,          -- Unique stop identifier (s id from XML)
    
    -- Movement Type and Platform
    is_arrival BOOLEAN NOT NULL,            -- TRUE for Arrival (<ar>), FALSE for Departure (<dp>)
    planned_platform VARCHAR(10),           -- Planned Platform (pp)
    actual_platform VARCHAR(10),            -- Actual Platform (cp) - From updates
    
    -- Measures (Planned vs. Actual)
    planned_time TIMESTAMP,                 -- Planned time (pt)
    actual_time TIMESTAMP,                  -- Actual/Revised time (ct)
    delay_minutes INTEGER,                  -- Calculated delay
    is_canceled BOOLEAN DEFAULT FALSE,      -- Cancellation status
    
    -- Constraint: Prevent duplicate entries for the same movement event
    UNIQUE (station_id, time_id, stop_id, is_arrival)
);