-- Task 2.3: Given a time snapshot (date hour), return total canceled trains.
SELECT 
    COUNT(*) AS total_canceled_trains
FROM FactTrainMovement f
JOIN DimTime t ON f.time_id = t.time_id
WHERE t.time_desc = :snapshot_time -- Format: 'YYYY-MM-DD HH:MM'
  AND f.is_canceled = TRUE;