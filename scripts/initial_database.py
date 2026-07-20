# Import Library initial_database.py
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# ============================================
# +      Function in initial_database.py     +
# ============================================

class DatabaseConnection:
    def __init__(self, env_path: str = None):
        # Load .env (kalau path tidak dikasih, cari otomatis dari root project)
        if env_path is None:
            env_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(dotenv_path=env_path)

        self.db_user = os.getenv("DB_USER","pre-caps3-user")
        self.db_password = os.getenv("DB_PASSWORD","pre-caps3-pass")
        self.db_host = os.getenv("DB_HOST","localhost")
        self.db_port = os.getenv("DB_PORT") or "5447"
        self.db_name = os.getenv("DB_NAME")

        self.url = (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def get_engine(self):
        return create_engine(self.url)

class SchemaManager:
    def __init__(self, db_engine):
        self.engine = db_engine

    def execute_sql_file(self, file_path: str):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"[SchemaManager] File SQL tidak ditemukan di: {file_path}")

        print(f"[SchemaManager] Membaca dan mengeksekusi file: {file_path}")

        with open(file_path, "r", encoding="utf-8") as file:
            sql_script = file.read()

        if "CREATE OR REPLACE FUNCTION" in sql_script or "DO $$" in sql_script:
            queries = [sql_script.strip()]
        else:
            queries = [q.strip() for q in sql_script.split(';') if q.strip()]

        with self.engine.begin() as conn:
            for query in queries:
                try:
                    exec_query = query if query.endswith(';') else query + ";"
                    conn.execute(text(exec_query))
                except Exception as query_error:
                    print(f"[Postgres Error] Gagal pada perintah: {query[:60]}... -> {query_error}")
                    raise query_error


# ================================
# +      initial_db Pipeline     +
# ================================

db_connection = DatabaseConnection()
engine = db_connection.get_engine()

def run_initial():
    """Memeriksa koneksi database dan mengeksekusi script schema DDL dasar."""
    print("-------  [2/4] INITIAL DATABASE   -------")

    # 1. Tes Koneksi Database
    print("🔌 Memeriksa koneksi ke PostgreSQL...")
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Koneksi database berhasil!\n")
    except Exception as e:
        print(f"❌ Gagal terhubung ke database! Error: {e}")
        raise e

    # 2. Eksekusi Script Schema (01-schema.sql)
    BASE_DIR = Path(__file__).resolve().parent.parent
    SQL_01_PATH = BASE_DIR / "sql" / "01-schema.sql"

    schema_manager = SchemaManager(db_engine=engine)
    schema_manager.execute_sql_file(SQL_01_PATH)

    print("\n🎉 Eksekusi schema database selesai tanpa kendala!")


if __name__ == "__main__":
    run_initial()