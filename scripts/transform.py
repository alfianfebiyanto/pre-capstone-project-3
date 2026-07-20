# Import Library transform.py
from pathlib import Path
from initial_database import DatabaseConnection, SchemaManager

# ===============================
# +      Path Configuration     +
# ===============================
BASE_DIR = Path(__file__).resolve().parent.parent
SQL_02_PATH = BASE_DIR / "sql" / "02-bronze.sql"
SQL_03_PATH = BASE_DIR / "sql" / "03-silver.sql"
SQL_04_PATH = BASE_DIR / "sql" / "04-gold-mart.sql"

# Buat instance koneksi & schema manager
db_conn = DatabaseConnection()
engine = db_conn.get_engine()
schema_manager = SchemaManager(db_engine=engine)


def run_silver_transformation():
    """Mengeksekusi transformasi dari Layer Bronze ke Layer Silver."""
    print("-------  [TRANSFORMATION: BRONZE TO SILVER]  -------")
    print("🚀 Menjalankan transformasi Silver...")
    
    # Eksekusi 02-bronze.sql jika ada pra-pemrosesan, lalu 03-silver.sql
    if SQL_02_PATH.exists():
        schema_manager.execute_sql_file(SQL_02_PATH)
    
    schema_manager.execute_sql_file(SQL_03_PATH)
    print("✅ Transformasi Silver Selesai!")


def run_gold_transformation():
    """Mengeksekusi agregasi dari Layer Silver ke Layer Gold Data Mart."""
    print("-------  [TRANSFORMATION: SILVER TO GOLD MART]  -------")
    print("🚀 Menjalankan agregasi Gold Mart...")
    
    schema_manager.execute_sql_file(SQL_04_PATH)
    print("✅ Agregasi Gold Mart Selesai!")


if __name__ == "__main__":
    run_silver_transformation()
    run_gold_transformation()