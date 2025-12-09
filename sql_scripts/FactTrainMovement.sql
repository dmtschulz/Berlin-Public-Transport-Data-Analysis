-- FactTrainMovement: Central table for all planned and actual train movements.
CREATE TABLE FactTrainMovement (
    movement_pk BIGSERIAL PRIMARY KEY,  -- PK: Surrogate key for each movement instance
    
    -- Foreign Keys to Dimensions
    station_id INTEGER NOT NULL REFERENCES DimStation(station_id), -- FK to DimStation (The station where the movement occurs)
    time_id INTEGER NOT NULL REFERENCES DimTime(time_id),          -- FK to DimTime (The time snapshot/download time)
    train_id VARCHAR(60) NOT NULL REFERENCES DimTrain(train_id),   -- FK to DimTrain (The unique train/line identifier)
    
    -- Movement Type and Platform
    stop_id VARCHAR(100) NOT NULL,        -- Unique stop identifier (s id from XML)
    is_arrival BOOLEAN NOT NULL,          -- TRUE for Arrival (<ar>), FALSE for Departure (<dp>)

    -- Platforms
    planned_platform VARCHAR(10),         -- Planned Platform (pp)
    actual_platform VARCHAR(10),          -- Actual/Revised Platform (cp)
    
    -- Times & Metrics
    planned_time TIMESTAMP NOT NULL,      -- Planned time (pt)
    actual_time TIMESTAMP,                -- Actual/Revised time (ct) - NULL if no change
    delay_minutes INTEGER,                -- Calculated delay: (Actual Time - Planned Time) / 60
    is_canceled BOOLEAN NOT NULL,         -- TRUE if movement was canceled
    
    -- Constraint to ensure a combination of FKs and stop_id is unique
    UNIQUE (station_id, time_id, stop_id, is_arrival)
);