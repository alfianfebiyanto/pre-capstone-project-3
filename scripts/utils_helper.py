# =============================
# +      Import Library       +
# =============================
import uuid
import logging
from pathlib import Path
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine


# ===============================
# +     Path Configuration      +
# ===============================
# 1. Base Directory (Root Proyek)
BASE_DIR = Path(__file__).resolve().parent.parent

# 2. Folder Data Lake
DATA_LAKE_DIR = BASE_DIR / "data_lake"
RAW_PATH = DATA_LAKE_DIR / "raw"
STAG_PATH = DATA_LAKE_DIR / "staging"

# 3. File Raw & Staging
RAW_TAXI_ZONES = RAW_PATH / "raw-taxi-zones.csv"
RAW_TAXI_TRIPS = RAW_PATH / "raw-taxi-trips.parquet"
STAG_TAXI_ZONES = STAG_PATH / "stag-taxi-zones.csv"
STAG_TAXI_TRIPS = STAG_PATH / "stag-taxi-trips.parquet"


# 4. Folder & File SQL
SQL_DIR = BASE_DIR / "sql"
SQL_01_SCHEMA = SQL_DIR / "01-schema.sql"
SQL_02_BRONZE = SQL_DIR / "02-bronze.sql"
SQL_03_SILVER = SQL_DIR / "03-silver.sql"
SQL_04_GOLD = SQL_DIR / "04-gold-mart.sql"

# 5. Source URLs
TAXI_TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
TAXI_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


# ===============================
# +     Logging Helper          +
# ===============================
def setup_logger(name: str) -> logging.Logger:
    """
    Mengonfigurasi dan mengembalikan objek logger standar untuk pipeline.

    Args:
        name (str): Nama modul yang menggunakan logger (biasanya __name__).

    Returns:
        logging.Logger: Objek logger yang siap digunakan.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)


# ===============================
# +        Audit Logger         +
# ===============================
class AuditLogger:
    """
    Helper terpusat untuk mencatat log eksekusi pipeline ke tabel audit.load_audit
    sekaligus mencetaknya ke Python logger (Console / Airflow Task Logs).
    """
    def __init__(self, engine: Engine, logger: Optional[logging.Logger] = None):
        self.engine = engine
        self.logger = logger or default_logger

    def log_start(self, run_id: uuid.UUID, stage: str, object_name: str) -> int:

        """Mencetak log STARTED ke console dan mencatatnya ke database."""

        self.logger.info(
            "[%s] Starting execution for %s (Run ID: %s)",
            stage.upper(),
            object_name,
            run_id,
        )

        query = text("""
            INSERT INTO audit.load_audit (run_id, stage, object_name, status, started_at)
            VALUES (:run_id, :stage, :object_name, 'STARTED', NOW())
            RETURNING audit_id;
        """)
        try:
            with self.engine.begin() as conn:
                audit_id = conn.execute(
                    query,
                    {
                        "run_id": str(run_id),
                        "stage": stage,
                        "object_name": object_name,
                    },
                ).scalar()
            return audit_id
        except Exception:
            self.logger.exception("Failed to write STARTED status to audit.load_audit")
            raise

    def log_success(
        self, audit_id: int, rows_affected: int = 0, message: Optional[str] = None
    ) -> None:

        """Mencetak log SUCCESS ke console dan memperbarui database."""

        msg = message or "Stage execution completed successfully"

        self.logger.info(
            "SUCCESS (Audit ID: %s) | Rows Affected: %s | Message: %s",
            audit_id,
            f"{rows_affected:,}",
            msg,
        )

        query = text("""
            UPDATE audit.load_audit
            SET status = 'SUCCESS',
                rows_affected = :rows_affected,
                message = :message,
                finished_at = NOW()
            WHERE audit_id = :audit_id;
        """)
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    query,
                    {
                        "audit_id": audit_id,
                        "rows_affected": rows_affected,
                        "message": msg,
                    },
                )
        except Exception:
            self.logger.exception("Failed to update SUCCESS status in audit.load_audit")
            raise

    def log_failure(self, audit_id: int, error_message: str) -> None:
        """Mencetak log FAILED/EXCEPTION ke console dan memperbarui database."""
        self.logger.error(
            "FAILED (Audit ID: %s) | Error: %s",
            audit_id,
            error_message,
        )

        query = text("""
            UPDATE audit.load_audit
            SET status = 'FAILED',
                message = :message,
                finished_at = NOW()
            WHERE audit_id = :audit_id;
        """)
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    query,
                    {
                        "audit_id": audit_id,
                        "message": error_message[:1000],  # Truncate agar muat di kolom TEXT/VARCHAR
                    },
                )
        except Exception:
            self.logger.exception("Failed to update FAILED status in audit.load_audit")
            raise