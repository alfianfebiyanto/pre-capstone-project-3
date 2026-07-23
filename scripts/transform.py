# ============================
# +     Import Libarary      +
# ============================

import uuid
from sqlalchemy import text
from sqlalchemy.engine import Engine
from initial_database import DatabaseConnection, SchemaManager

from utils_helper import (
    SQL_02_BRONZE,
    SQL_03_SILVER,
    SQL_04_GOLD,
    AuditLogger
    )
# Set-up logging 
from utils_helper import setup_logger
logger = setup_logger(__name__)

# ==========================================
# +       TRANSFORM SILVER LAYER           +
# ==========================================

def silver_transform(
    engine: Engine | None = None, run_id: uuid.UUID | None = None
) -> None:
    """Mengeksekusi transformasi SQL dari Bronze ke Silver layer dengan Audit Logging."""
    if engine is None:
        try:
            db_connection = DatabaseConnection()
            engine = db_connection.get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection instance")
            raise

    run_id = run_id or uuid.uuid4()
    audit = AuditLogger(engine=engine, logger=logger)

    audit_id = audit.log_start(
        run_id=run_id,
        stage="silver_transform",
        object_name="silver.taxi_trips_cleaned",
    )

    try:
        schema_manager = SchemaManager(db_engine=engine)

        # Jalankan eksekusi 02-bronze.sql jika ada (health check/views)
        if SQL_02_BRONZE.exists():
            schema_manager.execute_sql_file(SQL_02_BRONZE)

        # Jalankan eksekusi 03-silver.sql
        schema_manager.execute_sql_file(SQL_03_SILVER)

        # Hitung total baris yang berhasil di-clean di Silver Layer
        with engine.connect() as conn:
            cleaned_rows = (
                conn.execute(
                    text("SELECT COUNT(*) FROM silver.taxi_trips_cleaned")
                ).scalar()
                or 0
            )

        audit.log_success(
            audit_id=audit_id,
            rows_affected=cleaned_rows,
            message="Silver layer transformed and cleaned successfully",
        )

    except Exception as e:
        audit.log_failure(audit_id=audit_id, error_message=str(e))
        raise


# ==========================================
# +        TRANSFORM GOLD LAYER            +
# ==========================================

def goldmart_transform(
    engine: Engine | None = None, run_id: uuid.UUID | None = None
) -> None:
    """Mengeksekusi agregasi SQL dari Silver ke Gold Data Mart dengan Audit Logging."""
    if engine is None:
        try:
            db_connection = DatabaseConnection()
            engine = db_connection.get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection instance")
            raise

    run_id = run_id or uuid.uuid4()
    audit = AuditLogger(engine=engine, logger=logger)

    audit_id = audit.log_start(
        run_id=run_id,
        stage="gold_build",
        object_name="gold.daily_trip_summary",
    )

    try:
        schema_manager = SchemaManager(db_engine=engine)

        # Jalankan eksekusi 04-gold-mart.sql
        schema_manager.execute_sql_file(SQL_04_GOLD)

        # Hitung akumulasi baris di seluruh tabel Gold
        tables = [
            "daily_trip_summary",
            "hourly_demand_summary",
            "zone_performance_summary",
            "payment_behavior_summary",
            "route_performance_summary",
        ]
        total_gold_rows = 0

        with engine.connect() as conn:
            for tbl in tables:
                total_gold_rows += (
                    conn.execute(text(f"SELECT COUNT(*) FROM gold.{tbl}")).scalar()
                    or 0
                )

        audit.log_success(
            audit_id=audit_id,
            rows_affected=total_gold_rows,
            message="Gold layer datamarts built successfully",
        )

    except Exception as e:
        audit.log_failure(audit_id=audit_id, error_message=str(e))
        raise


if __name__ == "__main__":

    # Generate 1 run_id bersama untuk eksekusi mandiri lokal
    shared_run_id = uuid.uuid4()
    logger.info("Starting local standalone execution (Run ID: %s)", shared_run_id)

    silver_transform(run_id=shared_run_id)
    goldmart_transform(run_id=shared_run_id)