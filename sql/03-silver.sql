-- ====================================================
-- +       PROCESESS TRANSFORM BRONZE TO SILVER       +
-- ====================================================

TRUNCATE TABLE silver.taxi_zones CASCADE;
TRUNCATE TABLE silver.taxi_trips_cleaned;
TRUNCATE TABLE silver.data_quality_issues;

-- transform Bronze.raw_taxi_zones to Silver.Taxi Zones
INSERT INTO silver.taxi_zones (
    location_id,
    borough,
    zone,
    service_zone,
    updated_at
)
SELECT
    location_id,        
    borough,            
    zone,               
    service_zone,
    CURRENT_TIMESTAMP
FROM bronze.raw_taxi_zones
ON CONFLICT (location_id) DO UPDATE SET
    borough = EXCLUDED.borough,
    zone = EXCLUDED.zone,
    service_zone = EXCLUDED.service_zone,
    updated_at = CURRENT_TIMESTAMP;


-- Insert data valid ke silver.taxi_trips_cleaned
INSERT INTO silver.taxi_trips_cleaned (
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pulocation_id,
    dolocation_id,
    payment_type,
    payment_type_label,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    pickup_date,
    pickup_hour,
    pickup_day_name,
    is_weekend,
    time_period,
    trip_duration_minutes,
    updated_at
)
SELECT
    -- Kolom asli
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    -- Mapping payment type
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
        ELSE 'Other'
    END AS payment_type_label,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    -- Kolom turunan dari pickup_datetime
    DATE(tpep_pickup_datetime) AS pickup_date,
    EXTRACT(HOUR FROM tpep_pickup_datetime)::INT AS pickup_hour,
    TO_CHAR(tpep_pickup_datetime, 'Day') AS pickup_day_name,
    CASE
        WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    CASE
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 17 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_period,
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60 AS trip_duration_minutes,
    CURRENT_TIMESTAMP AS updated_at
FROM bronze.raw_taxi_trips
WHERE
    -- Filter data valid (business rules)
    passenger_count > 0
    AND trip_distance >= 0
    AND fare_amount >= 0
    AND total_amount >= 0
    AND pu_location_id IS NOT NULL
    AND do_location_id IS NOT NULL
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_dropoff_datetime > tpep_pickup_datetime
    AND DATE(tpep_pickup_datetime) BETWEEN '2026-01-01' AND '2026-01-31';


-- Insert data yang tidak valid ke silver.data_quality_issues
INSERT INTO silver.data_quality_issues (
    issue_type,
    severity,
    description,
    raw_data,
    updated_at
)
SELECT
    CASE
        WHEN passenger_count <= 0 THEN 'Invalid Passenger Count'
        WHEN trip_distance < 0 THEN 'Negative Trip Distance'
        WHEN fare_amount < 0 THEN 'Negative Fare'
        WHEN total_amount < 0 THEN 'Negative Total Amount'
        WHEN pu_location_id IS NULL OR do_location_id IS NULL THEN 'Missing Location'
        WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 'Invalid Time'
        WHEN DATE(tpep_pickup_datetime) NOT BETWEEN '2026-01-01' AND '2026-01-31' THEN 'Outside Date Range'
        ELSE 'Other'
    END AS issue_type,
    CASE
        WHEN passenger_count <= 0 OR total_amount < 0 THEN 'HIGH'
        WHEN trip_distance < 0 OR fare_amount < 0 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    'Row violates business rules' AS description,
    to_jsonb(bronze.raw_taxi_trips.*) AS raw_data,
    CURRENT_TIMESTAMP AS updated_at
FROM bronze.raw_taxi_trips
WHERE NOT (
    passenger_count > 0
    AND trip_distance >= 0
    AND fare_amount >= 0
    AND total_amount >= 0
    AND pu_location_id IS NOT NULL
    AND do_location_id IS NOT NULL
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_dropoff_datetime > tpep_pickup_datetime
    AND DATE(tpep_pickup_datetime) BETWEEN '2026-01-01' AND '2026-01-31'
);