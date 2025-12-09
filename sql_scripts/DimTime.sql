-- DimTime: Contains attributes related to time slices (day, hour, peak status).
CREATE TABLE DimTime (
    time_id SERIAL PRIMARY KEY,            -- PK: Calculated as sequential hours since start date (1, 2, 3...)
    time_desc VARCHAR(20),                  -- e.g., '2025-09-05 14:00'
    date DATE NOT NULL,                    -- The calendar date (e.g., '2025-09-05')
    year INTEGER NOT NULL,                 -- The year (e.g., 2025)
    month INTEGER NOT NULL,                -- The month number (1-12)
    day INTEGER NOT NULL,                  -- The day of the month (1-31)
    day_of_week VARCHAR(9) NOT NULL,       -- Full name of the day (e.g., 'Sunday')
    hour INTEGER NOT NULL,                 -- The hour of the day (0-23)
    minute INTEGER NOT NULL,               -- Minutes (0-59)
    is_peak_hour BOOLEAN NOT NULL,         -- TRUE if between 07:00-09:00 or 17:00-19:00
    is_weekend BOOLEAN NOT NULL            -- TRUE if Saturday or Sunday
);