# ============================
# +     Import Libarary      +
# ============================
from pathlib import Path

from initial_database import DatabaseConnection
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Set-up logging 
from utils_helper import setup_logger
logger = setup_logger(__name__)

# ===============================
# +     Path Configuration      +
# ===============================
BASE_DIR = Path(__file__).resolve().parent.parent


def validate_bronze_layer(engine: Engine | None = None) -> None:

    """
    Memastikan kualitas dan integritas data pada layer Bronze.

    Args:
        
        engine (Engine | None, optional): Objek SQLAlchemy Engine.
            Jika None, fungsi akan membuat koneksi baru.

    Raises:

        ValueError: Jika tabel Bronze kosong atau mengandung timestamp NULL.
        Exception: Jika terjadi kesalahan pada eksekusi query database.
    """

    logger.info("Starting Bronze layer validation")

    # 1. Inisialisasi koneksi database jika belum diberikan
    if engine is None:
        try:
            engine = DatabaseConnection().get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection engine")
            raise

    # 2. Periksa jumlah data pada tabel Bronze
    try:
        with engine.connect() as conn:
            trips_count = conn.execute(
                text("SELECT COUNT(*) FROM bronze.raw_taxi_trips")
            ).scalar()

            zones_count = conn.execute(
                text("SELECT COUNT(*) FROM bronze.raw_taxi_zones")
            ).scalar()

        logger.info("Bronze trips count: %s rows", f"{trips_count:,}")
        logger.info("Bronze zones count: %s rows", f"{zones_count:,}")

        if trips_count == 0 or zones_count == 0:
            raise ValueError("Bronze layer validation failed: Empty table detected.")
        
    except Exception:
        logger.exception("Failed during Bronze layer row count check")
        raise

    # 3. Periksa nilai NULL pada kolom tanggal utama
    try:
        with engine.connect() as conn:
            null_dates = conn.execute(
                text("""
                SELECT COUNT(*)
                FROM bronze.raw_taxi_trips
                WHERE tpep_pickup_datetime IS NULL
                OR tpep_dropoff_datetime IS NULL
            """)
            ).scalar()

        logger.info("Missing pickup/dropoff timestamp count: %s rows", f"{null_dates:,}")

        if null_dates > 0:
            raise ValueError(
                f"Bronze layer validation failed: {null_dates:,} rows contain NULL timestamps."
            )
    except Exception:
        logger.exception("Failed during Bronze layer null timestamp check")
        raise

    logger.info("Bronze layer validation completed successfully")


def validate_silver_layer(engine: Engine | None = None) -> None:

    """
    Memastikan kualitas dan integritas data pada layer Silver.

    Args:

        engine (Engine | None, optional): Objek SQLAlchemy Engine.
            Jika None, fungsi akan membuat koneksi baru.

    Raises:

        ValueError: Jika tabel Silver kosong, mengandung nilai negatif, atau ada duplikasi ID.
        Exception: Jika terjadi kesalahan pada eksekusi query database.
    """

    logger.info("Starting Silver layer validation")

    # 1. Inisialisasi koneksi database jika belum diberikan
    if engine is None:
        try:
            engine = DatabaseConnection().get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection engine")
            raise

    # 2. Periksa jumlah data pada tabel Silver
    try:
        with engine.connect() as conn:
            trips_count = conn.execute(
                text("SELECT COUNT(*) FROM silver.taxi_trips_cleaned")
            ).scalar()

            zones_count = conn.execute(
                text("SELECT COUNT(*) FROM silver.taxi_zones")
            ).scalar()

        logger.info("Silver trips count: %s rows", f"{trips_count:,}")
        logger.info("Silver zones count: %s rows", f"{zones_count:,}")

        if trips_count == 0 or zones_count == 0:
            raise ValueError("Silver layer validation failed: Empty table detected.")
    except Exception:
        logger.exception("Failed during Silver layer row count check")
        raise

    # 3. Periksa hasil cleansing terhadap nilai negatif
    try:
        with engine.connect() as conn:
            negative_amount = conn.execute(
                text("SELECT COUNT(*) FROM silver.taxi_trips_cleaned WHERE total_amount < 0")
            ).scalar()

            negative_distance = conn.execute(
                text("SELECT COUNT(*) FROM silver.taxi_trips_cleaned WHERE trip_distance < 0")
            ).scalar()

        logger.info("Negative total amount records: %s", f"{negative_amount:,}")
        logger.info("Negative trip distance records: %s", f"{negative_distance:,}")

        if negative_amount > 0 or negative_distance > 0:
            raise ValueError(
                "Silver layer validation failed: Invalid negative values detected."
            )
    except Exception:
        logger.exception("Failed during Silver layer negative value check")
        raise

    # 4. Periksa duplikasi location_id pada tabel taxi_zones
    try:
        with engine.connect() as conn:
            duplicate_zones = conn.execute(
                text("""
                SELECT location_id
                FROM silver.taxi_zones
                GROUP BY location_id
                HAVING COUNT(*) > 1
            """)
            ).fetchall()

        if duplicate_zones:
            raise ValueError(
                f"Silver layer validation failed: Found {len(duplicate_zones)} duplicate location_id records."
            )
    except Exception:
        logger.exception("Failed during Silver layer duplicate location_id check")
        raise

    logger.info("Silver layer validation completed successfully")

def validate_goldmart_layer(engine: Engine | None = None) -> None:
    """
    Memastikan kualitas dan integritas data pada layer Gold.

    Args:

        engine (Engine | None, optional): Objek SQLAlchemy Engine.
            Jika None, fungsi akan membuat koneksi baru.

    Raises:

        ValueError: Jika tabel Gold kosong, melanggar aturan bisnis, atau mengandung duplikasi PK.
        Exception: Jika terjadi kesalahan pada eksekusi query database.
    """

    logger.info("Starting Gold layer validation")

    # 1. Inisialisasi koneksi database jika belum diberikan
    if engine is None:
        try:
            engine = DatabaseConnection().get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection engine")
            raise

    # 2. Periksa jumlah data pada seluruh tabel Gold
    try:
        gold_counts = {}
        tables = [
            "daily_trip_summary",
            "hourly_demand_summary",
            "zone_performance_summary",
            "payment_behavior_summary",
            "route_performance_summary",
        ]

        with engine.connect() as conn:
            for table_name in tables:
                gold_counts[table_name] = conn.execute(
                    text(f"SELECT COUNT(*) FROM gold.{table_name}")
                ).scalar()

        for table_name, count in gold_counts.items():
            logger.info("Gold %s count: %s rows", table_name, f"{count:,}")

            if count == 0:
                raise ValueError(
                    f"Gold layer validation failed: Empty table gold.{table_name}."
                )
    except Exception:
        logger.exception("Failed during Gold layer row count check")
        raise

    # 3. Validasi aturan bisnis (business rules)
    try:
        with engine.connect() as conn:
            invalid_pct = conn.execute(
                text("""
                SELECT COUNT(*)
                FROM gold.payment_behavior_summary
                WHERE percentage_of_total < 0
                OR percentage_of_total > 100
            """)
            ).scalar()

            if invalid_pct > 0:
                raise ValueError(
                    "Gold layer validation failed: Invalid percentage value detected."
                )

            invalid_routes = conn.execute(
                text("""
                SELECT COUNT(*)
                FROM gold.route_performance_summary
                WHERE trip_count < 10
            """)
            ).scalar()

            if invalid_routes > 0:
                raise ValueError(
                    f"Gold layer validation failed: {invalid_routes} routes have trip_count below minimum threshold."
                )
    except Exception:
        logger.exception("Failed during Gold layer business rules validation step")
        raise

    # 4. Periksa duplikasi Primary Key pada tabel Gold
    try:
        with engine.connect() as conn:
            duplicate_daily = conn.execute(
                text("""
                SELECT summary_date
                FROM gold.daily_trip_summary
                GROUP BY summary_date
                HAVING COUNT(*) > 1
            """)
            ).fetchall()

            if duplicate_daily:
                raise ValueError(
                    "Gold layer validation failed: Duplicate summary_date detected in daily_trip_summary."
                )

            duplicate_hourly = conn.execute(
                text("""
                SELECT hour_slot
                FROM gold.hourly_demand_summary
                GROUP BY hour_slot
                HAVING COUNT(*) > 1
            """)
            ).fetchall()

            if duplicate_hourly:
                raise ValueError(
                    "Gold layer validation failed: Duplicate hour_slot detected in hourly_demand_summary."
                )
    except Exception:
        logger.exception("Failed during Gold layer primary key uniqueness check")
        raise

    logger.info("Gold layer validation completed successfully")

if __name__ == "__main__":
    validate_bronze_layer()
    validate_silver_layer()
    validate_goldmart_layer()