# Import Library
# ini load_to_postgre.py
import io
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from initial_database import DatabaseConnection

class DataLoader:
    @staticmethod
    def copy_dataframe(df, cursor, table, schema='bronze', delimiter='\t', null_rep='\\N'):
        """
        Generic COPY from DataFrame to PostgreSQL.
        """
        buffer = io.StringIO()
        df.to_csv(
            buffer,
            sep=delimiter,
            header=False,
            index=False,
            na_rep=null_rep
        )
        buffer.seek(0)
        # Handle delimiter literal for tab
        if delimiter == '\t':
            delim_literal = "E'\\t'"
        else:
            delim_literal = f"'{delimiter}'"
        cursor.copy_expert(
            f"""
            COPY {schema}.{table}
            FROM STDIN
            WITH (
                FORMAT CSV,
                DELIMITER {delim_literal},
                NULL '{null_rep}'
            )
            """,
            buffer
        )

    @staticmethod
    def load_csv(csv_path, engine, table='raw_taxi_zones', schema='bronze',
                delimiter=',', null_rep='\\N', truncate_first=True):
        """
        Load CSV file ke PostgreSQL menggunakan COPY.
        """
        df = pd.read_csv(csv_path)
        connection = engine.raw_connection()
        try:
            cursor = connection.cursor()

            # --- DITAMBAHKAN: Truncate sebelum COPY untuk Idempotency ---
            if truncate_first:
                cursor.execute(f"TRUNCATE TABLE {schema}.{table};")
                print(f"🧹 Table {schema}.{table} di-truncate sebelum re-load.")
            # -----------------------------------------------------------

            DataLoader.copy_dataframe(
                df,
                cursor,
                table=table,
                schema=schema,
                delimiter=delimiter,
                null_rep=null_rep
            )
            connection.commit()
            print(f"✔ {schema}.{table} : {len(df):,} rows")
        finally:
            connection.close()

    @staticmethod
    def load_parquet(parquet_path, engine, table='raw_taxi_trips', schema='bronze',
                    integer_cols=None, batch_size=500000,
                    delimiter='\t', null_rep='\\N', decode_bytes=True, truncate_first=True):

        if integer_cols is None:
            integer_cols = []

        parquet = pq.ParquetFile(parquet_path)
        total = parquet.metadata.num_rows
        loaded = 0
        connection = engine.raw_connection()

        try:
            cursor = connection.cursor()

            # --- DITAMBAHKAN: Truncate sebelum loop batching untuk Idempotency ---
            if truncate_first:
                cursor.execute(f"TRUNCATE TABLE {schema}.{table};")
                print(f"🧹 Table {schema}.{table} di-truncate sebelum re-load.")
            # --------------------------------------------------------------------


            for batch in parquet.iter_batches(batch_size=batch_size):
                frame = batch.to_pandas()

                # Decode bytes columns if needed
                if decode_bytes:
                    # PERBAIKAN: Pilih kolom dengan tipe 'object' ATAU 'str'
                    for col in frame.select_dtypes(include=['object']).columns: # hapus str
                        if len(frame) > 0 and isinstance(frame[col].iloc[0], bytes):
                            frame[col] = frame[col].str.decode('utf-8', errors='replace')
                # Convert specified columns to Int64
                for col in integer_cols:
                    if col in frame.columns:
                        frame[col] = frame[col].astype("Int64")

                DataLoader.copy_dataframe(
                    frame,
                    cursor,
                    table=table,
                    schema=schema,
                    delimiter=delimiter,
                    null_rep=null_rep
                )

                loaded += len(frame)
                print(f"{loaded:,}/{total:,} rows")

            connection.commit()
            print(f"✔ COPY selesai untuk {schema}.{table}")

        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def run_load_to_postgres(engine=None):
    """Membaca file staging dan memuat data ke PostgreSQL layer Bronze."""
    print("-------  [3/4] LOAD TO POSTGRES  -------")

    # 1. Buat koneksi jika engine belum dioperkan
    if engine is None:
        db_connection = DatabaseConnection()
        engine = db_connection.get_engine()
        print("🔌 Memeriksa koneksi ke PostgreSQL...")
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Koneksi database berhasil!\n")
        except Exception as e:
            print(f"❌ Gagal terhubung ke database! Error: {e}")
            raise e

    # 2. Tentukan Path File Staging
    STAG_ZONE_FILE = "data_lake/staging/stag-zone-taxi.csv"
    STAG_TRIP_FILE = "data_lake/staging/stag-trip-taxi.parquet"

    # 3. Load Data ke Database
    print("🚀 Membaca file staging & mengunggah data ke PostgreSQL...")

    try:
        # Load Zone Lookup
        DataLoader.load_csv(
            csv_path=STAG_ZONE_FILE,
            engine=engine,
            table="raw_taxi_zones", 
            schema="bronze", 
            delimiter=',',
            null_rep=''
        )

        # Load Trip Data (Batched)
        DataLoader.load_parquet(
            parquet_path=STAG_TRIP_FILE,
            engine=engine,
            table="raw_taxi_trips",
            schema="bronze",
            batch_size=500000,
            decode_bytes=True,     
            integer_cols=["vendor_id", "passenger_count", "ratecode_id", "payment_type"]
        )
        
        print("✅ Semua data berhasil di-load ke PostgreSQL (Bronze Layer)!\n")

    except Exception as e:
        print(f"❌ Gagal memuat data ke PostgreSQL! Error: {e}")
        raise e


if __name__ == "__main__":
    run_load_to_postgres()