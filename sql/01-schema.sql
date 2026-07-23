-- =========================
-- +     INITIALIZATION    +
-- =========================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;


-- =========================
-- +      BRONZE LAYER     +
-- =========================

-- 1. Tabel raw_taxi_zones
CREATE TABLE IF NOT EXISTS bronze.raw_taxi_zones(
    "location_id" INT PRIMARY KEY,
    "borough" VARCHAR(255),
    "zone" VARCHAR(255),
    "service_zone" VARCHAR(255)

);

-- 2. Tabel raw_taxi_trips
CREATE TABLE IF NOT EXISTS bronze.raw_taxi_trips (
    vendor_id INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance NUMERIC(12, 3),
    ratecode_id INTEGER,
    store_and_fwd_flag TEXT,
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount NUMERIC(12, 2),
    extra NUMERIC(12, 2),
    mta_tax NUMERIC(12, 2),
    tip_amount NUMERIC(12, 2),
    tolls_amount NUMERIC(12, 2),
    improvement_surcharge NUMERIC(12, 2),
    total_amount NUMERIC(12, 2),
    congestion_surcharge NUMERIC(12, 2),
    airport_fee NUMERIC(12, 2),
    cbd_congestion_fee NUMERIC(12, 2)
);


-- =========================
-- +      AUDIT LAYER      +
-- =========================

-- 1. Tabel audit
CREATE TABLE IF NOT EXISTS audit.load_audit (
    audit_id        BIGSERIAL PRIMARY KEY,
    run_id          UUID        NOT NULL,
    stage           VARCHAR(50) NOT NULL,          -- bronze_load / silver_transform / gold_build / query
    object_name     VARCHAR(200) NOT NULL,         -- nama tabel / file yang diproses
    rows_affected   BIGINT      DEFAULT 0 CHECK (rows_affected >= 0),
    status          VARCHAR(20) NOT NULL CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED')),
    message         TEXT,
    started_at      TIMESTAMP   NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMP
);


-- ==========================
-- +      SILVER LAYER      +
-- ==========================

-- 1. Tabel Taxi Zones 
DROP TABLE IF EXISTS silver.taxi_zones CASCADE;
CREATE TABLE silver.taxi_zones (
    location_id   INT PRIMARY KEY,
    borough       VARCHAR(50) NOT NULL,
    zone          VARCHAR(100) NOT NULL,
    service_zone  VARCHAR(50),
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tabel Taxi Trips Cleaned 
DROP TABLE IF EXISTS silver.taxi_trips_cleaned CASCADE;
CREATE TABLE silver.taxi_trips_cleaned (
    trip_id                 SERIAL PRIMARY KEY,
    vendor_id               INT,
    pickup_datetime         TIMESTAMP NOT NULL,
    dropoff_datetime        TIMESTAMP NOT NULL,
    passenger_count         INT,
    trip_distance           NUMERIC(10,2),
    ratecode_id             INT,
    store_and_fwd_flag      VARCHAR(1),
    pulocation_id           INT NOT NULL,
    dolocation_id           INT NOT NULL,
    payment_type            INT,
    payment_type_label      VARCHAR(30),
    fare_amount             NUMERIC(10,2) NOT NULL,
    extra                   NUMERIC(10,2),
    mta_tax                 NUMERIC(10,2),
    tip_amount              NUMERIC(10,2),
    tolls_amount            NUMERIC(10,2),
    improvement_surcharge   NUMERIC(10,2),
    total_amount            NUMERIC(10,2) NOT NULL,
    congestion_surcharge    NUMERIC(10,2),
    airport_fee             NUMERIC(10,2),
    -- Kolom turunan (wajib)
    pickup_date             DATE NOT NULL,
    pickup_hour             INT NOT NULL,
    pickup_day_name         VARCHAR(10) NOT NULL,
    is_weekend              BOOLEAN NOT NULL,
    time_period             VARCHAR(20) NOT NULL,
    trip_duration_minutes   NUMERIC(10,2),
    -- Audit
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT chk_passenger_count CHECK (passenger_count > 0),
    CONSTRAINT chk_trip_distance CHECK (trip_distance >= 0),
    CONSTRAINT chk_fare_amount CHECK (fare_amount >= 0),
    CONSTRAINT chk_total_amount CHECK (total_amount >= 0),
    CONSTRAINT chk_pickup_hour CHECK (pickup_hour BETWEEN 0 AND 23),
    CONSTRAINT fk_pulocation FOREIGN KEY (pulocation_id) REFERENCES silver.taxi_zones(location_id),
    CONSTRAINT fk_dolocation FOREIGN KEY (dolocation_id) REFERENCES silver.taxi_zones(location_id)
);

-- 3. Tabel Data quality issues 
DROP TABLE IF EXISTS silver.data_quality_issues CASCADE;
CREATE TABLE silver.data_quality_issues (
    issue_id       SERIAL PRIMARY KEY,
    issue_type     VARCHAR(50) NOT NULL,
    severity       VARCHAR(20) DEFAULT 'MEDIUM',
    description    TEXT,
    raw_data       JSONB,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ==========================
-- +      GOLD LAYER      +
-- ==========================

-- 1. Tabel Daily Trips Summary
DROP TABLE IF EXISTS gold.daily_trip_summary CASCADE;
CREATE TABLE gold.daily_trip_summary (
    summary_date        DATE PRIMARY KEY,
    total_trips         INT NOT NULL,
    total_revenue       NUMERIC(12,2) NOT NULL,
    avg_fare            NUMERIC(10,2),
    avg_trip_distance   NUMERIC(10,2),
    avg_duration_min    NUMERIC(10,2),
    unique_pickup_zones INT,
    unique_dropoff_zones INT,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tabel Hourly Demand Summary
DROP TABLE IF EXISTS gold.hourly_demand_summary CASCADE;
CREATE TABLE gold.hourly_demand_summary (
    hour_slot     INT PRIMARY KEY,   
    avg_trips     NUMERIC(10,2),
    max_trips     INT,
    total_revenue NUMERIC(12,2),
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Tabel Zone Perfomance Summary
DROP TABLE IF EXISTS gold.zone_performance_summary CASCADE;
CREATE TABLE gold.zone_performance_summary (
    location_id    INT PRIMARY KEY,
    borough        VARCHAR(50),
    zone           VARCHAR(100),
    total_pickups  INT,
    total_dropoffs INT,
    total_revenue  NUMERIC(12,2),
    avg_fare       NUMERIC(10,2),
    avg_tip        NUMERIC(10,2),
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_id) REFERENCES silver.taxi_zones(location_id)
);

-- 4. Tabel Payment Behavior Summary
DROP TABLE IF EXISTS gold.payment_behavior_summary CASCADE;
CREATE TABLE gold.payment_behavior_summary (
    payment_type_label VARCHAR(30) PRIMARY KEY,
    total_trips        INT,
    total_revenue      NUMERIC(12,2),
    avg_tip            NUMERIC(10,2),
    percentage_of_total NUMERIC(5,2),   
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Tabel Route Perfomance Summary
DROP TABLE IF EXISTS gold.route_performance_summary CASCADE;
CREATE TABLE gold.route_performance_summary (
    route_id        SERIAL PRIMARY KEY,
    pickup_zone_id  INT,
    dropoff_zone_id INT,
    trip_count      INT,
    avg_fare        NUMERIC(10,2),
    avg_duration    NUMERIC(10,2),
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pickup_zone_id) REFERENCES silver.taxi_zones(location_id),
    FOREIGN KEY (dropoff_zone_id) REFERENCES silver.taxi_zones(location_id)
);

