# ============================
# +     Import Libarary      +
# ============================
import os
from pathlib import Path
from dotenv import load_dotenv
from utils_helper import SQL_01_SCHEMA
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Set-up logging 
from utils_helper import setup_logger
logger = setup_logger(__name__)


# ===============================
# +      Helper Functions       +
# ===============================
class DatabaseConnection:
    """Mengelola konfigurasi dan koneksi ke database PostgreSQL."""
    def __init__(self, env_path: Path | None = None) -> None:
        """Menginisialisasi konfigurasi database dari file lingkungan (.env).
        Args:
            env_path (Path | None, optional): Path menuju file .env.
                Jika None, default mengambil dari root project.
        """
        # 1. Muat file .env
        if env_path is None:
            env_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(dotenv_path=env_path)

        # 2. Ambil konfigurasi database dari environment variables
        self.db_user = os.getenv("PRE_CAPS3_USER")
        self.db_password = os.getenv("PRE_CAPS3_PASS")
        self.db_host = os.getenv("PRE_CAPS3_HOST", "localhost")
        self.db_port = os.getenv("PRE_CAPS3_PORT") or "5447"
        self.db_name = os.getenv("PRE_CAPS3_DB")

        # 3. Susun URL koneksi PostgreSQL
        self.url = (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def get_engine(self) -> Engine:
        """Membuat dan mengembalikan SQLAlchemy Database Engine.

        Returns:
            Engine: Objek SQLAlchemy Engine yang siap digunakan.
        """
        # 1. Kembalikan database engine
        return create_engine(self.url)


class SchemaManager:
    """Mengelola pembacaan dan eksekusi file SQL ke database."""
    def __init__(self, db_engine: Engine) -> None:
        """
        Menginisialisasi SchemaManager dengan database engine.

        Args:
            db_engine (Engine): Objek SQLAlchemy Engine untuk koneksi database.
        """

        self.engine = db_engine

    def execute_sql_file(self, file_path: Path) -> None:
        """
        Menjalankan seluruh perintah SQL yang terdapat dalam sebuah file.

        Args:
            file_path (Path): Path file SQL yang akan dieksekusi.

        Raises:
            FileNotFoundError: Jika file SQL pada file_path tidak ditemukan.
            Exception: Jika terjadi kesalahan saat mengeksekusi perintah SQL.
        """

        # 1. Pastikan file SQL tersedia
        if not file_path.exists():
            raise FileNotFoundError(f"SQL file not found at: {file_path}")

        logger.info("Reading and executing SQL file: %s", file_path)

        # 2. Baca isi file SQL
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                sql_script = file.read()
        except Exception:
            logger.exception("Failed to read SQL file at %s", file_path)
            raise

        # 3. Pisahkan query jika bukan blok prosedur/fungsi khusus
        if "CREATE OR REPLACE FUNCTION" in sql_script or "DO $$" in sql_script:
            queries = [sql_script.strip()]
        else:
            queries = [
                query.strip()
                for query in sql_script.split(";")
                if query.strip()
            ]

        # 4. Eksekusi setiap query dalam transaksi database
        try:
            with self.engine.begin() as conn:
                for query in queries:
                    exec_query = query if query.endswith(";") else query + ";"
                    conn.execute(text(exec_query))
            logger.info("Successfully executed SQL script from %s", file_path)
        except Exception:
            logger.exception("Failed executing SQL script from %s", file_path)
            raise


def initial_stage() -> None:
    """Memeriksa koneksi database dan mengeksekusi script schema DDL dasar.
    
    Raises:
        Exception: Jika koneksi gagal atau eksekusi DDL mengalami kesalahan.
    """

    logger.info("Starting initial database setup pipeline execution")

    # 1. Inisialisasi engine database
    try:
        db_connection = DatabaseConnection()
        engine = db_connection.get_engine()
    except Exception:
        logger.exception("Failed to initialize database connection engine")
        raise

    # 2. Verifikasi koneksi database
    logger.info("Testing PostgreSQL database connection")
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection established successfully")
    except Exception:
        logger.exception("Failed to establish PostgreSQL database connection")
        raise

    # 3. Eksekusi script schema database
    logger.info("Executing database schema script")
    try:
        schema_manager = SchemaManager(db_engine=engine)
        schema_manager.execute_sql_file(SQL_01_SCHEMA)
    except Exception:
        logger.exception("Failed during database schema execution step")
        raise

    logger.info("Initial database setup completed successfully")


if __name__ == "__main__":
    initial_stage()