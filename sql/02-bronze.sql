-- =========================
-- +      BRONZE LAYER     +
-- =========================

-- 1. Cek Total Baris Data pada Tabel Bronze
SELECT 
    'bronze.raw_taxi_zones' AS table_name, 
    COUNT(*) AS total_rows 
FROM bronze.raw_taxi_zones
UNION ALL
SELECT 
    'bronze.raw_taxi_trips' AS table_name, 
    COUNT(*) AS total_rows 
FROM bronze.raw_taxi_trips;