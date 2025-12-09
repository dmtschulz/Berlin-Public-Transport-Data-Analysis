-- Task 2.2: Given latitude/longitude, return the name of the closest station.
-- We order by the squared Euclidean distance ((lat-lat)^2 + (lon-lon)^2)
SELECT 
    station_name,
    latitude,
    longitude,
    (POWER(latitude - :input_lat, 2) + POWER(longitude - :input_lon, 2)) AS distance_score
FROM DimStation
ORDER BY distance_score ASC
LIMIT 1;