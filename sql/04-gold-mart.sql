-- =========================
-- +       GOLD LAYER      +
-- =========================

-- 1. Truncate Seluruh Tabel Gold Data Mart
TRUNCATE TABLE gold.daily_trip_summary CASCADE;
TRUNCATE TABLE gold.hourly_demand_summary CASCADE;
TRUNCATE TABLE gold.zone_performance_summary CASCADE;
TRUNCATE TABLE gold.payment_behavior_summary CASCADE;
TRUNCATE TABLE gold.route_performance_summary CASCADE;

-- 2. Aggregation: Daily Trip Summary
INSERT INTO gold.daily_trip_summary (
    summary_date,
    total_trips,
    total_revenue,
    avg_fare,
    avg_trip_distance,
    avg_duration_min,
    unique_pickup_zones,
    unique_dropoff_zones,
    updated_at
)
SELECT
    pickup_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_duration_min,
    COUNT(DISTINCT pulocation_id) AS unique_pickup_zones,
    COUNT(DISTINCT dolocation_id) AS unique_dropoff_zones,
    CURRENT_TIMESTAMP
FROM silver.taxi_trips_cleaned
GROUP BY pickup_date
ON CONFLICT (summary_date) DO UPDATE SET
    total_trips = EXCLUDED.total_trips,
    total_revenue = EXCLUDED.total_revenue,
    avg_fare = EXCLUDED.avg_fare,
    avg_trip_distance = EXCLUDED.avg_trip_distance,
    avg_duration_min = EXCLUDED.avg_duration_min,
    unique_pickup_zones = EXCLUDED.unique_pickup_zones,
    unique_dropoff_zones = EXCLUDED.unique_dropoff_zones,
    updated_at = CURRENT_TIMESTAMP;


-- 3. Aggregation: Hourly Demand Summary
INSERT INTO gold.hourly_demand_summary (
    hour_slot,
    avg_trips,
    max_trips,
    total_revenue,
    updated_at
)
SELECT
    pickup_hour,
    AVG(trip_count)::NUMERIC(10,2) AS avg_trips,
    MAX(trip_count) AS max_trips,
    SUM(total_revenue) AS total_revenue,
    CURRENT_TIMESTAMP
FROM (
    SELECT
        pickup_hour,
        pickup_date,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_revenue
    FROM silver.taxi_trips_cleaned
    GROUP BY pickup_hour, pickup_date
) sub
GROUP BY pickup_hour
ON CONFLICT (hour_slot) DO UPDATE SET
    avg_trips = EXCLUDED.avg_trips,
    max_trips = EXCLUDED.max_trips,
    total_revenue = EXCLUDED.total_revenue,
    updated_at = CURRENT_TIMESTAMP;


-- 4. Aggregation: Zone Performance Summary
INSERT INTO gold.zone_performance_summary (
    location_id,
    borough,
    zone,
    total_pickups,
    total_dropoffs,
    total_revenue,
    avg_fare,
    avg_tip,
    updated_at
)
SELECT
    z.location_id,
    z.borough,
    z.zone,
    COUNT(CASE WHEN t.pulocation_id = z.location_id THEN 1 END) AS total_pickups,
    COUNT(CASE WHEN t.dolocation_id = z.location_id THEN 1 END) AS total_dropoffs,
    COALESCE(SUM(CASE WHEN t.pulocation_id = z.location_id THEN t.total_amount ELSE 0 END), 0) AS total_revenue,
    COALESCE(AVG(CASE WHEN t.pulocation_id = z.location_id THEN t.fare_amount END), 0) AS avg_fare,
    COALESCE(AVG(CASE WHEN t.pulocation_id = z.location_id THEN t.tip_amount END), 0) AS avg_tip,
    CURRENT_TIMESTAMP
FROM silver.taxi_zones z
LEFT JOIN silver.taxi_trips_cleaned t
    ON t.pulocation_id = z.location_id OR t.dolocation_id = z.location_id
GROUP BY z.location_id, z.borough, z.zone
ON CONFLICT (location_id) DO UPDATE SET
    total_pickups = EXCLUDED.total_pickups,
    total_dropoffs = EXCLUDED.total_dropoffs,
    total_revenue = EXCLUDED.total_revenue,
    avg_fare = EXCLUDED.avg_fare,
    avg_tip = EXCLUDED.avg_tip,
    updated_at = CURRENT_TIMESTAMP;

WITH total_trips_all AS (
    SELECT COUNT(*) AS total_all FROM silver.taxi_trips_cleaned
)


-- 5. Aggregation: Payment Behavior Summary
INSERT INTO gold.payment_behavior_summary (
    payment_type_label,
    total_trips,
    total_revenue,
    avg_tip,
    percentage_of_total,
    updated_at
)
SELECT
    payment_type_label,
    COUNT(*) AS total_trips,
    COALESCE(SUM(total_amount), 0) AS total_revenue,
    COALESCE(AVG(tip_amount), 0) AS avg_tip,
    ROUND((COUNT(*) * 100.0 / (SELECT total_all FROM total_trips_all)), 2) AS percentage_of_total,
    CURRENT_TIMESTAMP
FROM silver.taxi_trips_cleaned
GROUP BY payment_type_label
ON CONFLICT (payment_type_label) DO UPDATE SET
    total_trips = EXCLUDED.total_trips,
    total_revenue = EXCLUDED.total_revenue,
    avg_tip = EXCLUDED.avg_tip,
    percentage_of_total = EXCLUDED.percentage_of_total,
    updated_at = CURRENT_TIMESTAMP;

-- 6. Aggregation: Route Performance Summary
INSERT INTO gold.route_performance_summary (
    pickup_zone_id,
    dropoff_zone_id,
    trip_count,
    avg_fare,
    avg_duration,
    updated_at
)
SELECT
    pulocation_id,
    dolocation_id,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_duration_minutes) AS avg_duration,
    CURRENT_TIMESTAMP
FROM silver.taxi_trips_cleaned
GROUP BY pulocation_id, dolocation_id
HAVING COUNT(*) >= 10;