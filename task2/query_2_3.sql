-- Task 2.3: Given a time snapshot (date hour), return total canceled trains.
-- Join FactTrainMovement with DimTime and filter by the specific date and hour columns.

SELECT 
    COUNT(*) AS total_canceled_trains
FROM FactTrainMovement f
JOIN DimTime t ON f.time_id = t.time_id
WHERE t.date = :target_date  -- e.g., '2025-09-12'
  AND t.hour = :target_hour  -- e.g., 12
  AND f.is_canceled = TRUE;