# ============================
# +     Import Libarary      +
# ============================

import re
from pathlib import Path

import pandas as pd
import requests

# Set-up logging 
from utils_helper import setup_logger
logger = setup_logger(__name__)

# ===============================
# +     Path Configuration      +
# ===============================
TAXI_TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
ZONE_TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "data_lake" / "raw"
STAG_PATH = BASE_DIR / "data_lake" / "staging"

RAW_ZONE_TAXI = RAW_PATH / "taxi-zone.csv"
RAW_TRIPS_TAXI = RAW_PATH / "taxi-trips.parquet"

def download(url: str, output_dir: Path, file_name: str) -> Path:

    """
    Mengunduh file dari URL ke direktori tujuan dan memvalidasi ukuran file.

    Args:

        url (str): URL sumber file yang akan diunduh.
        output_dir (Path): Direktori tujuan penyimpanan file.
        file_name (str): Nama file yang akan disimpan.

    Returns:

        Path: Path lengkap menuju file yang berhasil diunduh.

    Raises:

        ValueError: Jika file hasil unduhan berukuran 0 byte.
        requests.RequestException: Jika terjadi kesalahan pada koneksi HTTP.
    """

    # 1. Buat direktori tujuan jika belum ada
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / file_name

    # 2. Lewati proses jika file sudah tersedia
    if destination.exists():
        logger.info("File already exists: %s", destination)
        return destination

    # 3. Unduh file dan simpan ke direktori tujuan
    logger.info("Downloading from %s", url)
    try:
        response = requests.get(url, stream=True, timeout=(10, 300))
        response.raise_for_status()

        with open(destination, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        # 4. Validasi ukuran file tidak 0 byte
        if destination.stat().st_size == 0:
            raise ValueError(f"Downloaded file is empty (0 bytes): {destination}")

        logger.info("Download completed: %s", destination)
        return destination

    except Exception:
        logger.exception("Failed to download file from %s", url)
        raise


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:

    """
    Mengubah nama kolom DataFrame menjadi format snake_case yang konsisten.

    Args:

        df (pd.DataFrame): DataFrame yang akan dibersihkan nama kolomnya.

    Returns:

        pd.DataFrame: DataFrame dengan nama kolom berformat snake_case.
    """

    new_cols = []
    for col in df.columns:
        # 1. Tangani akhiran 'ID' agar berubah jadi '_id' bukan '_i_d'
        c = re.sub(r"(?<=[a-z0-9])ID$", "_id", col)
        c = re.sub(r"(?<=[A-Z])ID$", "_id", c)

        # 2. Tangani transisi PascalCase / CamelCase ke snake_case (misal: LocationID -> location_id)
        c = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", c)

        # 3. Ubah semua ke lowercase dan hilangkan double underscore jika ada
        c = c.lower()
        c = re.sub(r"_+", "_", c)

        new_cols.append(c)

    df.columns = new_cols
    return df


def save_data(df: pd.DataFrame, folder_path: Path, file_name: str) -> None:

    """
    Menyimpan DataFrame ke dalam format CSV atau Parquet.

    Args:

        df (pd.DataFrame): DataFrame yang akan disimpan.
        folder_path (Path): Direktori tujuan penyimpanan.
        file_name (str): Nama file tujuan (harus berakhiran .csv atau .parquet).

    Raises:
    
        ValueError: Jika ekstensi nama file bukan .csv atau .parquet.
    """

    # 1. Buat folder tujuan jika belum ada
    folder_path.mkdir(parents=True, exist_ok=True)
    destination = folder_path / file_name

    # 2. Simpan DataFrame sesuai format file
    try:
        if file_name.endswith(".parquet"):
            df.to_parquet(destination, index=False)
        elif file_name.endswith(".csv"):
            df.to_csv(destination, index=False)
        else:
            raise ValueError("File name must end with .parquet or .csv")

        logger.info("Saved dataset to %s", destination)

    except Exception:
        logger.exception("Failed to save dataset to %s", destination)
        raise


def exctraction_stage() -> None:

    """
    Menjalankan seluruh alur ekstraksi, pembersihan, dan penyiapan staging area.

    Fungsi ini dirancang bersifat idempotent sehingga aman dijalankan berulang kali.

    Raises:

        Exception: Jika terjadi kesalahan pada salah satu tahap ekstraksi.
    """

    logger.info("Starting extraction pipeline execution")

    # 1. Pastikan folder tujuan RAW_PATH dan STAG_PATH sudah dibuat
    try:
        RAW_PATH.mkdir(parents=True, exist_ok=True)
        STAG_PATH.mkdir(parents=True, exist_ok=True)
    except Exception:
        logger.exception("Failed to create raw or staging directories")
        raise

    # 2. Unduh dataset mentah
    try:
        logger.info("Downloading raw datasets")
        download(url=ZONE_TAXI_URL, output_dir=RAW_PATH, file_name="taxi-zone.csv")
        download(url=TAXI_TRIPS_URL, output_dir=RAW_PATH, file_name="taxi-trips.parquet")
    except Exception:
        logger.exception("Failed during raw datasets download step")
        raise

    # 3. Proses dataset taxi zones
    try:
        logger.info("Processing taxi zone dataset")
        df_zone = pd.read_csv(RAW_ZONE_TAXI)
        df_zone_clean = clean_column_names(df_zone)

        # Handling NULL values pada Lookup Zone
        zone_cols = ["zone", "borough", "service_zone"]
        for col in zone_cols:
            if col in df_zone_clean.columns:
                df_zone_clean[col] = df_zone_clean[col].fillna("Unknown")

        # Simpan Zone ke area staging
        logger.info("Saving processed taxi zone dataset to staging")
        save_data(df_zone_clean, STAG_PATH, "stag-zone-taxi.csv")

        # Membebaskan memori yang tidak lagi digunakan
        del df_zone, df_zone_clean

    except Exception:
        logger.exception("Failed during taxi zone dataset processing step")
        raise

    # 4. Proses dataset taxi trips
    try:
        logger.info("Processing taxi trip dataset")
        df_trips = pd.read_parquet(RAW_TRIPS_TAXI)
        df_trips_clean = clean_column_names(df_trips)

        # Simpan Trips ke area staging
        logger.info("Saving processed taxi trip dataset to staging")
        save_data(df_trips_clean, STAG_PATH, "stag-trip-taxi.parquet")

        # Membebaskan memori yang tidak lagi digunakan
        del df_trips, df_trips_clean

    except Exception:
        logger.exception("Failed during taxi trip dataset processing step")
        raise

    logger.info("Extraction pipeline completed successfully")

if __name__ == "__main__":
    exctraction_stage()