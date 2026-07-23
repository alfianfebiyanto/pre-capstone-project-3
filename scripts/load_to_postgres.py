# ============================
# +     Import Libarary      +
# ============================
import io
import uuid
import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from initial_database import DatabaseConnection
from utils_helper import STAG_TAXI_TRIPS, STAG_TAXI_ZONES, AuditLogger

# Set-up logging 
from utils_helper import setup_logger
logger = setup_logger(__name__)


# ===============================
# +      Helper Functions       +
# ===============================
class DataLoader:

    """Mengelola proses pemuatan data ke database PostgreSQL menggunakan metode COPY."""

    @staticmethod
    def copy_dataframe(df: pd.DataFrame, cursor, table: str, schema: str = "bronze", delimiter: str = "\t", null_rep: str = "\\N",) -> None:

        """Menyalin isi DataFrame ke tabel PostgreSQL menggunakan perintah COPY.

        Args:

            df (pd.DataFrame): DataFrame yang akan dimuat ke database.
            cursor: Cursor database PostgreSQL yang aktif.
            table (str): Nama tabel tujuan.
            schema (str, optional): Nama schema database tujuan. Default "bronze".
            delimiter (str, optional): Karakter pemisah antar kolom. Default "\t".
            null_rep (str, optional): Representasi string untuk nilai NULL. Default "\\N".

        Raises:

            Exception: Jika terjadi kesalahan saat eksekusi copy_expert.
        """

        # 1. Konversi DataFrame ke buffer CSV
        buffer = io.StringIO()
        df.to_csv(
            buffer,
            sep=delimiter,
            header=False,
            index=False,
            na_rep=null_rep,
        )
        buffer.seek(0)

        # 2. Tentukan delimiter untuk perintah COPY
        delim_literal = "E'\\t'" if delimiter == "\t" else f"'{delimiter}'"

        # 3. Jalankan perintah COPY
        try:
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
                buffer,
            )
        except Exception:
            logger.exception("Failed executing COPY command for table %s.%s", schema, table)
            raise

    @staticmethod
    def load_csv(csv_path: Path, engine: Engine, table: str = "raw_taxi_zones", schema: str = "bronze", delimiter: str = ",", null_rep: str = "\\N", truncate_first: bool = True,) -> None:

        """
        Memuat file CSV ke tabel PostgreSQL menggunakan metode COPY.

        Args:

            csv_path (Path): Path file CSV yang akan dimuat.
            engine (Engine): Objek SQLAlchemy Engine untuk koneksi database.
            table (str, optional): Nama tabel tujuan. Default "raw_taxi_zones".
            schema (str, optional): Nama schema database tujuan. Default "bronze".
            delimiter (str, optional): Karakter pemisah pada file CSV. Default ",".
            null_rep (str, optional): Representasi string untuk nilai NULL. Default "\\N".
            truncate_first (bool, optional): Jika True, kosongkan tabel sebelum memuat data.
                Default True.

        Raises:

            FileNotFoundError: Jika file CSV tidak ditemukan.
            Exception: Jika terjadi kesalahan saat proses pemuatan data.
        """

        # 1. Pastikan file CSV tersedia
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")

        # 2. Baca file CSV
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            logger.exception("Failed to read CSV file at %s", csv_path)
            raise

        # 3. Buka koneksi database mentah
        connection = engine.raw_connection()

        try:
            cursor = connection.cursor()

            # 4. Kosongkan tabel jika diperlukan
            if truncate_first:
                cursor.execute(f"TRUNCATE TABLE {schema}.{table};")
                logger.info("Table %s.%s truncated before reloading", schema, table)

            # 5. Salin data ke PostgreSQL
            DataLoader.copy_dataframe(
                df=df,
                cursor=cursor,
                table=table,
                schema=schema,
                delimiter=delimiter,
                null_rep=null_rep,
            )

            connection.commit()
            logger.info("Successfully loaded %s rows into %s.%s", f"{len(df):,}", schema, table)

        except Exception:
            connection.rollback()
            logger.exception("Failed loading CSV data into %s.%s", schema, table)
            raise
        finally:
            connection.close()

    @staticmethod
    def load_parquet(parquet_path: Path, engine: Engine, table: str = "raw_taxi_trips", schema: str = "bronze", integer_cols: list[str] | None = None,
        batch_size: int = 500_000, delimiter: str = "\t", null_rep: str = "\\N", decode_bytes: bool = True, truncate_first: bool = True,) -> None:
        """
        Memuat file Parquet ke tabel PostgreSQL secara bertahap menggunakan COPY.

        Args:
            parquet_path (Path): Path file Parquet yang akan dimuat.
            engine (Engine): Objek SQLAlchemy Engine untuk koneksi database.
            table (str, optional): Nama tabel tujuan. Default "raw_taxi_trips".
            schema (str, optional): Nama schema database tujuan. Default "bronze".
            integer_cols (list[str] | None, optional): Daftar kolom yang dikonversi ke Int64.
            batch_size (int, optional): Jumlah baris per batch. Default 500_000.
            delimiter (str, optional): Karakter pemisah antar kolom. Default "\t".
            null_rep (str, optional): Representasi string untuk nilai NULL. Default "\\N".
            decode_bytes (bool, optional): Jika True, decode tipe data bytes ke string.
                Default True.
            truncate_first (bool, optional): Jika True, kosongkan tabel sebelum memuat data.
                Default True.

        Raises:
            FileNotFoundError: Jika file Parquet tidak ditemukan.
            Exception: Jika terjadi kesalahan saat proses pemuatan batch data.
        """
        # 1. Pastikan file Parquet tersedia
        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found at: {parquet_path}")

        integer_cols = integer_cols or []

        # 2. Siapkan pembacaan metadata Parquet
        try:
            parquet = pq.ParquetFile(parquet_path)
            total_rows = parquet.metadata.num_rows
        except Exception:
            logger.exception("Failed to inspect Parquet metadata for %s", parquet_path)
            raise

        loaded_rows = 0
        connection = engine.raw_connection()

        try:
            cursor = connection.cursor()

            # 3. Kosongkan tabel jika diperlukan
            if truncate_first:
                cursor.execute(f"TRUNCATE TABLE {schema}.{table};")
                logger.info("Table %s.%s truncated before reloading", schema, table)

            # 4. Muat setiap batch ke PostgreSQL
            for batch in parquet.iter_batches(batch_size=batch_size):
                frame = batch.to_pandas()

                # Decode kolom bytes menjadi string jika ada
                if decode_bytes:
                    for col in frame.select_dtypes(include=["object"]).columns:
                        if len(frame) > 0 and isinstance(frame[col].iloc[0], bytes):
                            frame[col] = frame[col].str.decode("utf-8", errors="replace")

                # Konversi kolom integer nullable
                for col in integer_cols:
                    if col in frame.columns:
                        frame[col] = frame[col].astype("Int64")

                DataLoader.copy_dataframe(
                    df=frame,
                    cursor=cursor,
                    table=table,
                    schema=schema,
                    delimiter=delimiter,
                    null_rep=null_rep,
                )

                loaded_rows += len(frame)
                logger.info(
                    "Progress loading %s.%s: %s/%s rows",
                    schema,
                    table,
                    f"{loaded_rows:,}",
                    f"{total_rows:,}",
                )

            connection.commit()
            logger.info("COPY process completed successfully for %s.%s", schema, table)

        except Exception:
            connection.rollback()
            logger.exception("Failed loading Parquet batch into %s.%s", schema, table)
            raise
        finally:
            connection.close()


def load_postgres_stage(
    engine: Engine | None = None, run_id: uuid.UUID | None = None
) -> None:
    """Membaca file staging dan memuat data ke PostgreSQL layer Bronze dengan Logging Audit."""
    # 1. Inisialisasi koneksi jika belum ada
    if engine is None:
        try:
            db_connection = DatabaseConnection()
            engine = db_connection.get_engine()
        except Exception:
            logger.exception("Failed to initialize database connection instance")
            raise

    # 2. Inisialisasi Audit Logger
    run_id = run_id or uuid.uuid4()
    audit = AuditLogger(engine=engine, logger=logger)

    audit_id = audit.log_start(
        run_id=run_id,
        stage="bronze_load",
        object_name="bronze.raw_taxi_trips",
    )

    try:
        # 3. Load data taxi zone ke Bronze layer
        DataLoader.load_csv(
            csv_path=STAG_TAXI_ZONES,
            engine=engine,
            table="raw_taxi_zones",
            schema="bronze",
            delimiter=",",
            null_rep="",
        )

        # 4. Load data taxi trip ke Bronze layer
        DataLoader.load_parquet(
            parquet_path=STAG_TAXI_TRIPS,
            engine=engine,
            table="raw_taxi_trips",
            schema="bronze",
            batch_size=500_000,
            decode_bytes=True,
            integer_cols=["vendor_id", "passenger_count", "ratecode_id", "payment_type"],
        )

        # Hitung total baris yang berhasil di-load untuk audit
        with engine.connect() as conn:
            loaded_rows = (
                conn.execute(text("SELECT COUNT(*) FROM bronze.raw_taxi_trips")).scalar()
                or 0
            )

        # 5. Catat SUCCESS ke Audit
        audit.log_success(
            audit_id=audit_id,
            rows_affected=loaded_rows,
            message="Staging data loaded successfully into Bronze layer",
        )

    except Exception as e:
        # 6. Catat FAILED jika terjadi kegagalan
        audit.log_failure(audit_id=audit_id, error_message=str(e))
        raise


if __name__ == "__main__":
    load_postgres_stage()