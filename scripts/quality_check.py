# Import Libary in quality_check.py
import sys
from pathlib import Path
from sqlalchemy import text

# ===============================
# +      Path Configuration     +
# ===============================

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))
sys.path.append(str(BASE_DIR / "scripts"))

from initial_database import DatabaseConnection

# =======================
# +      Fungction      +
# =======================

def check_bronze_table():
    """
    Quality Check 1: Layer Bronze (Raw Data)
    - Memastikan data mentah terisi (> 0 rows).
    - Memastikan tanggal pickup/dropoff utama tidak NULL.
    """
    print("🔍 [Quality Check 1/3] Memeriksa Layer Bronze...")
    db_conn = DatabaseConnection()
    engine = db_conn.get_engine()

    with engine.connect() as conn:
        trips_count = conn.execute(text("SELECT COUNT(*) FROM bronze.raw_taxi_trips")).scalar()
        zones_count = conn.execute(text("SELECT COUNT(*) FROM bronze.raw_taxi_zones")).scalar()

        print(f"   📊 Bronze Trips Count : {trips_count:,} rows")
        print(f"   📊 Bronze Zones Count : {zones_count:,} rows")

        if trips_count == 0 or zones_count == 0:
            raise ValueError("❌ Quality Check Gagal: Table Bronze (trips/zones) KOSONG!")

        null_dates = conn.execute(text("""
            SELECT COUNT(*) 
            FROM bronze.raw_taxi_trips 
            WHERE tpep_pickup_datetime IS NULL 
            OR tpep_dropoff_datetime IS NULL
        """)).scalar()

        print(f"   📊 Null Pickup/Dropoff Dates: {null_dates:,} rows")
        if null_dates > 0:
            raise ValueError(f"❌ Quality Check Gagal: Ditemukan {null_dates:,} baris dengan tanggal utama NULL!")

    print("✅ Quality Check Bronze PASSED!\n")


def check_silver_table():
    """
    Quality Check 2: Layer Silver (Cleaned & Transformed Data)
    - Memastikan tabel silver.taxi_trips_cleaned terisi (> 0 rows).
    - Memastikan hasil cleansing bersih (tidak ada total_amount < 0 dan trip_distance < 0).
    - Memastikan tidak ada duplikasi location_id pada silver.taxi_zones.
    """
    print("🔍 [Quality Check 2/3] Memeriksa Layer Silver...")
    db_conn = DatabaseConnection()
    engine = db_conn.get_engine()

    with engine.connect() as conn:
        # 1. Row Count Check
        silver_trips_count = conn.execute(text("SELECT COUNT(*) FROM silver.taxi_trips_cleaned")).scalar()
        silver_zones_count = conn.execute(text("SELECT COUNT(*) FROM silver.taxi_zones")).scalar()

        print(f"   📊 Silver Cleaned Trips Count : {silver_trips_count:,} rows")
        print(f"   📊 Silver Zones Count         : {silver_zones_count:,} rows")

        if silver_trips_count == 0 or silver_zones_count == 0:
            raise ValueError("❌ Quality Check Gagal: Table Silver (taxi_trips_cleaned/taxi_zones) KOSONG!")

        # 2. Data Integrity / Cleansing Check
        neg_total = conn.execute(text("SELECT COUNT(*) FROM silver.taxi_trips_cleaned WHERE total_amount < 0")).scalar()
        neg_dist  = conn.execute(text("SELECT COUNT(*) FROM silver.taxi_trips_cleaned WHERE trip_distance < 0")).scalar()

        print(f"   📊 Negative Total Amount di Silver : {neg_total:,} rows")
        print(f"   📊 Negative Trip Distance di Silver : {neg_dist:,} rows")

        if neg_total > 0 or neg_dist > 0:
            raise ValueError("❌ Quality Check Gagal: Data bernilai negatif masih lolos ke Layer Silver!")

        # 3. Primary Key Uniqueness Check di Silver Zones
        dup_zones = conn.execute(text("""
            SELECT location_id 
            FROM silver.taxi_zones 
            GROUP BY location_id 
            HAVING COUNT(*) > 1
        """)).fetchall()

        if len(dup_zones) > 0:
            raise ValueError(f"❌ Quality Check Gagal: Ditemukan {len(dup_zones)} duplikasi location_id pada silver.taxi_zones!")

    print("✅ Quality Check Silver PASSED!\n")


def check_gold_output():
    """
    Quality Check 3: Layer Gold Mart
    - Memastikan seluruh 5 tabel Gold Mart terisi (> 0 rows).
    - Memastikan logika agregasi valid (percentage <= 100%, trip_count route >= 10).
    - Memastikan tidak ada duplikasi Primary Key pada tabel Gold.
    """
    print("🔍 [Quality Check 3/3] Memeriksa Layer Gold Mart...")
    db_conn = DatabaseConnection()
    engine = db_conn.get_engine()

    with engine.connect() as conn:
        # 1. Row Count Check untuk 5 Tabel Gold
        daily_count   = conn.execute(text("SELECT COUNT(*) FROM gold.daily_trip_summary")).scalar()
        hourly_count  = conn.execute(text("SELECT COUNT(*) FROM gold.hourly_demand_summary")).scalar()
        zone_count    = conn.execute(text("SELECT COUNT(*) FROM gold.zone_performance_summary")).scalar()
        payment_count = conn.execute(text("SELECT COUNT(*) FROM gold.payment_behavior_summary")).scalar()
        route_count   = conn.execute(text("SELECT COUNT(*) FROM gold.route_performance_summary")).scalar()

        print(f"   📊 Gold Daily Trip Summary Count   : {daily_count:,} rows")
        print(f"   📊 Gold Hourly Demand Count        : {hourly_count:,} rows")
        print(f"   📊 Gold Zone Performance Count     : {zone_count:,} rows")
        print(f"   📊 Gold Payment Behavior Count     : {payment_count:,} rows")
        print(f"   📊 Gold Route Performance Count    : {route_count:,} rows")

        gold_counts = {
            "daily_trip_summary": daily_count,
            "hourly_demand_summary": hourly_count,
            "zone_performance_summary": zone_count,
            "payment_behavior_summary": payment_count,
            "route_performance_summary": route_count
        }

        for table_name, count in gold_counts.items():
            if count == 0:
                raise ValueError(f"❌ Quality Check Gagal: Table gold.{table_name} KOSONG!")

        # 2. Validasi Logika Business Rules
        invalid_pct = conn.execute(text("SELECT COUNT(*) FROM gold.payment_behavior_summary WHERE percentage_of_total > 100 OR percentage_of_total < 0")).scalar()
        if invalid_pct > 0:
            raise ValueError("❌ Quality Check Gagal: Ditemukan percentage_of_total di luar batas (0-100%)!")

        invalid_routes = conn.execute(text("SELECT COUNT(*) FROM gold.route_performance_summary WHERE trip_count < 10")).scalar()
        if invalid_routes > 0:
            raise ValueError(f"❌ Quality Check Gagal: Ditemukan {invalid_routes} rute dengan trip_count < 10 pada route_performance_summary!")

        # 3. Primary Key Uniqueness Check
        dup_daily = conn.execute(text("SELECT summary_date FROM gold.daily_trip_summary GROUP BY summary_date HAVING COUNT(*) > 1")).fetchall()
        if len(dup_daily) > 0:
            raise ValueError("❌ Quality Check Gagal: Ditemukan duplikasi summary_date pada gold.daily_trip_summary!")

        dup_hourly = conn.execute(text("SELECT hour_slot FROM gold.hourly_demand_summary GROUP BY hour_slot HAVING COUNT(*) > 1")).fetchall()
        if len(dup_hourly) > 0:
            raise ValueError("❌ Quality Check Gagal: Ditemukan duplikasi hour_slot pada gold.hourly_demand_summary!")

    print("✅ Quality Check Gold Mart PASSED!\n")


if __name__ == "__main__":
    check_bronze_table()
    check_silver_table()
    check_gold_output()