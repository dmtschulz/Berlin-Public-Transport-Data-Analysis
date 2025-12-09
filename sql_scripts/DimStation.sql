-- DimStation: Contains static metadata for the 133 Berlin stations.
CREATE TABLE DimStation (
    station_id INTEGER PRIMARY KEY,         -- PK: e.g., 8011003 (from evaNumbers where isMain=true)
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