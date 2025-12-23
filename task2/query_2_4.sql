-- Task 2.4: Given a station name, return the average train delay.
WITH UniqueMovements AS (
    SELECT 
        f.delay_minutes,
        -- Rank snapshots to find the latest update for each unique stop event
        ROW_NUMBER() OVER (
            PARTITION BY f.station_id, f.stop_id, f.is_arrival 
            ORDER BY f.time_id DESC
        ) as rn
    FROM FactTrainMovement f
    JOIN DimStation s ON f.station_id = s.station_id
    WHERE s.station_name = :station_name
      AND f.is_arrival = TRUE -- Focusing on Arrival Delay (standard metric)
)
SELECT 
    AVG(GREATEST(0, delay_minutes)) AS avg_delay_minutes
FROM UniqueMovements
WHERE rn = 1 -- Only take the most recent snapshot for that train
  AND delay_minutes IS NOT NULL; -- Exclude trains with no delay info