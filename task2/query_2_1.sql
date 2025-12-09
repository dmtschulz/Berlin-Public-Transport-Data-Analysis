-- Task 2.1: Given a station name, return its coordinates and identifier.
-- Replace :station_name with the actual name in the python runner.
SELECT 
    station_id,
    station_name, 
    latitude, 
    longitude 
FROM DimStation 
WHERE station_name = :station_name;