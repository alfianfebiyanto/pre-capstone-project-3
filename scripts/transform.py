# ============================
# +     Import Libarary      +
# ============================

from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from initial_database import DatabaseConnection, SchemaManager

# Set-up logging 
from utils_helper import setup_logger
logger = setup_logger(__name__)

# ===============================
# +     Path Configuration      +
# ===============================

BASE_DIR = Path(__file__).resolve().parent.parent
SQL_02_PATH = BASE_DIR / "sql" / "02-bronze.sql"
SQL_03_PATH = BASE_DIR / "sql" / "03-silver.sql"
SQL_04_PATH = BASE_DIR / "sql" / "04-gold-mart.sql"

def silver_transform(engine: Engine | None = None) -> None:

    """
    Mengeksekusi skrip transformasi SQL dari layer Bronze ke layer Silver.

    Args:
        
        engine (Engine | None, optional): Objek SQLAlchemy Engine.
            Jika None, fungsi akan membuat koneksi baru.

    Raises:
        
        Exception: Jika terjadi kesalahan saat eksekusi file SQL transformasi Silver.
    """

    logger.info("Starting Silver layer transformation execution")

    # 1. Inisialisasi koneksi database dan SchemaManager
    if engine is None:
        try:
            db_conn = DatabaseConnection()
            engine = db_conn.get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection engine")
            raise

    schema_manager = SchemaManager(db_engine=engine)

    # 2. Eksekusi skrip pra-pemrosesan jika tersedia
    if SQL_02_PATH.exists():
        try:
            logger.info("Executing pre-transformation SQL script: %s", SQL_02_PATH)
            schema_manager.execute_sql_file(SQL_02_PATH)
        except Exception:
            logger.exception("Failed executing pre-transformation SQL script")
            raise

    # 3. Eksekusi skrip transformasi Silver
    try:
        logger.info("Executing Silver transformation SQL script: %s", SQL_03_PATH)
        schema_manager.execute_sql_file(SQL_03_PATH)
    except Exception:
        logger.exception("Failed executing Silver transformation SQL script")
        raise

    logger.info("Silver layer transformation completed successfully")


def goldmart_transform(engine: Engine | None = None) -> None:

    """
    Mengeksekusi skrip agregasi SQL dari layer Silver ke Gold Data Mart.

    Args:

        engine (Engine | None, optional): Objek SQLAlchemy Engine.
            Jika None, fungsi akan membuat koneksi baru.

    Raises:
    
        Exception: Jika terjadi kesalahan saat eksekusi file SQL transformasi Gold.
    """

    logger.info("Starting Gold Mart transformation execution")

    # 1. Inisialisasi koneksi database dan SchemaManager
    if engine is None:
        try:
            db_conn = DatabaseConnection()
            engine = db_conn.get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection engine")
            raise

    schema_manager = SchemaManager(db_engine=engine)

    # 2. Eksekusi skrip agregasi Gold Mart
    try:
        logger.info("Executing Gold Mart aggregation SQL script: %s", SQL_04_PATH)
        schema_manager.execute_sql_file(SQL_04_PATH)
    except Exception:
        logger.exception("Failed executing Gold Mart aggregation SQL script")
        raise

    logger.info("Gold Mart transformation completed successfully")


if __name__ == "__main__":
    silver_transform()
    goldmart_transform()