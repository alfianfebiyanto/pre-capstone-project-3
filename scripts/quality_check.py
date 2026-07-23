# ============================
# +      Import Library      +
# ============================
import uuid
from initial_database import DatabaseConnection
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from utils_helper import AuditLogger, setup_logger

# Set-up Logging
logger = setup_logger(__name__)

# ===============================
# +      Helper Functions       +
# ===============================
def validate_bronze_layer(
    engine: Engine | None = None, run_id: uuid.UUID | None = None
) -> None:
    """Memastikan kualitas dan integritas data pada layer Bronze.

    Args:
        engine (Engine | None, optional): Objek SQLAlchemy Engine.
        run_id (uuid.UUID | None, optional): Unique ID untuk pelacakan audit run.

    Raises:
        ValueError: Jika tabel Bronze kosong atau mengandung timestamp NULL.
        Exception: Jika terjadi kesalahan pada eksekusi query database.
    """
    if engine is None:
        try:
            engine = DatabaseConnection().get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection engine")
            raise

    run_id = run_id or uuid.uuid4()
    audit = AuditLogger(engine=engine, logger=logger)

    audit_id = audit.log_start(
        run_id=run_id,
        stage="bronze_validation",
        object_name="bronze.raw_taxi_trips",
    )

    try:
        # 1. Periksa jumlah data pada tabel Bronze
        with engine.connect() as conn:
            trips_count = (
                conn.execute(
                    text("SELECT COUNT(*) FROM bronze.raw_taxi_trips")
                ).scalar()
                or 0
            )

            zones_count = (
                conn.execute(
                    text("SELECT COUNT(*) FROM bronze.raw_taxi_zones")
                ).scalar()
                or 0
            )

        logger.info("Bronze trips count: %s rows", f"{trips_count:,}")
        logger.info("Bronze zones count: %s rows", f"{zones_count:,}")

        if trips_count == 0 or zones_count == 0:
            raise ValueError("Bronze layer validation failed: Empty table detected.")

        # 2. Periksa nilai NULL pada kolom tanggal utama
        with engine.connect() as conn:
            null_dates = (
                conn.execute(
                    text("""
                    SELECT COUNT(*)
                    FROM bronze.raw_taxi_trips
                    WHERE tpep_pickup_datetime IS NULL
                    OR tpep_dropoff_datetime IS NULL
                """)
                ).scalar()
                or 0
            )

        logger.info("Missing pickup/dropoff timestamp count: %s rows", f"{null_dates:,}")

        if null_dates > 0:
            raise ValueError(
                f"Bronze layer validation failed: {null_dates:,} rows contain NULL timestamps."
            )

        audit.log_success(
            audit_id=audit_id,
            rows_affected=trips_count,
            message="Bronze layer validation passed successfully",
        )

    except Exception as e:
        audit.log_failure(audit_id=audit_id, error_message=str(e))
        raise


def validate_silver_layer(
    engine: Engine | None = None, run_id: uuid.UUID | None = None
) -> None:
    """Memastikan kualitas dan integritas data pada layer Silver.

    Args:
        engine (Engine | None, optional): Objek SQLAlchemy Engine.
        run_id (uuid.UUID | None, optional): Unique ID untuk pelacakan audit run.

    Raises:
        ValueError: Jika tabel Silver kosong, mengandung nilai negatif, atau ada duplikasi ID.
        Exception: Jika terjadi kesalahan pada eksekusi query database.
    """
    if engine is None:
        try:
            engine = DatabaseConnection().get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection engine")
            raise

    run_id = run_id or uuid.uuid4()
    audit = AuditLogger(engine=engine, logger=logger)

    audit_id = audit.log_start(
        run_id=run_id,
        stage="silver_validation",
        object_name="silver.taxi_trips_cleaned",
    )

    try:
        # 1. Periksa jumlah data pada tabel Silver
        with engine.connect() as conn:
            trips_count = (
                conn.execute(
                    text("SELECT COUNT(*) FROM silver.taxi_trips_cleaned")
                ).scalar()
                or 0
            )

            zones_count = (
                conn.execute(
                    text("SELECT COUNT(*) FROM silver.taxi_zones")
                ).scalar()
                or 0
            )

        logger.info("Silver trips count: %s rows", f"{trips_count:,}")
        logger.info("Silver zones count: %s rows", f"{zones_count:,}")

        if trips_count == 0 or zones_count == 0:
            raise ValueError("Silver layer validation failed: Empty table detected.")

        # 2. Periksa hasil cleansing terhadap nilai negatif
        with engine.connect() as conn:
            negative_amount = (
                conn.execute(
                    text("SELECT COUNT(*) FROM silver.taxi_trips_cleaned WHERE total_amount < 0")
                ).scalar()
                or 0
            )

            negative_distance = (
                conn.execute(
                    text("SELECT COUNT(*) FROM silver.taxi_trips_cleaned WHERE trip_distance < 0")
                ).scalar()
                or 0
            )

        logger.info("Negative total amount records: %s", f"{negative_amount:,}")
        logger.info("Negative trip distance records: %s", f"{negative_distance:,}")

        if negative_amount > 0 or negative_distance > 0:
            raise ValueError(
                "Silver layer validation failed: Invalid negative values detected."
            )

        # 3. Periksa duplikasi location_id pada tabel taxi_zones
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

        audit.log_success(
            audit_id=audit_id,
            rows_affected=trips_count,
            message="Silver layer validation passed successfully",
        )

    except Exception as e:
        audit.log_failure(audit_id=audit_id, error_message=str(e))
        raise


def validate_goldmart_layer(
    engine: Engine | None = None, run_id: uuid.UUID | None = None
) -> None:
    """Memastikan kualitas dan integritas data pada layer Gold.

    Args:
        engine (Engine | None, optional): Objek SQLAlchemy Engine.
        run_id (uuid.UUID | None, optional): Unique ID untuk pelacakan audit run.

    Raises:
        ValueError: Jika tabel Gold kosong, melanggar aturan bisnis, atau mengandung duplikasi PK.
        Exception: Jika terjadi kesalahan pada eksekusi query database.
    """
    if engine is None:
        try:
            engine = DatabaseConnection().get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection engine")
            raise

    run_id = run_id or uuid.uuid4()
    audit = AuditLogger(engine=engine, logger=logger)

    audit_id = audit.log_start(
        run_id=run_id,
        stage="gold_validation",
        object_name="gold.daily_trip_summary",
    )

    try:
        # 1. Periksa jumlah data pada seluruh tabel Gold
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
                ).scalar() or 0

        total_gold_rows = sum(gold_counts.values())

        for table_name, count in gold_counts.items():
            logger.info("Gold %s count: %s rows", table_name, f"{count:,}")

            if count == 0:
                raise ValueError(
                    f"Gold layer validation failed: Empty table gold.{table_name}."
                )

        # 2. Validasi aturan bisnis (business rules)
        with engine.connect() as conn:
            invalid_pct = (
                conn.execute(
                    text("""
                    SELECT COUNT(*)
                    FROM gold.payment_behavior_summary
                    WHERE percentage_of_total < 0
                    OR percentage_of_total > 100
                """)
                ).scalar()
                or 0
            )

            if invalid_pct > 0:
                raise ValueError(
                    "Gold layer validation failed: Invalid percentage value detected."
                )

            invalid_routes = (
                conn.execute(
                    text("""
                    SELECT COUNT(*)
                    FROM gold.route_performance_summary
                    WHERE trip_count < 10
                """)
                ).scalar()
                or 0
            )

            if invalid_routes > 0:
                raise ValueError(
                    f"Gold layer validation failed: {invalid_routes} routes have trip_count below minimum threshold."
                )

        # 3. Periksa duplikasi Primary Key pada tabel Gold
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

        audit.log_success(
            audit_id=audit_id,
            rows_affected=total_gold_rows,
            message="Gold layer validation passed successfully",
        )

    except Exception as e:
        audit.log_failure(audit_id=audit_id, error_message=str(e))
        raise


if __name__ == "__main__":
    shared_run_id = uuid.uuid4()
    validate_bronze_layer(run_id=shared_run_id)
    validate_silver_layer(run_id=shared_run_id)
    validate_goldmart_layer(run_id=shared_run_id)