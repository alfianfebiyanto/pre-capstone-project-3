# Import Library di extract.py
import os
import re
import gc
import requests
import pandas as pd
from pathlib import Path

# ====================================
# +      Function in exctract.py     +
# ====================================

def download(url, output_dir, file_name):
    os.makedirs(output_dir, exist_ok=True)
    destination = os.path.join(output_dir, file_name)
    if os.path.exists(destination):
        print(f"✔ File sudah ada : {destination}")
        return destination
    print(f"📥 Downloading {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(destination, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
    print(f"✅ Download selesai : {destination}")
    return destination


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Mengubah nama kolom DataFrame menjadi snake_case yang konsisten.
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

def save_data(df, folder_path, file_name):
    os.makedirs(folder_path, exist_ok=True)
    path = os.path.join(folder_path, file_name)

    if file_name.endswith(".parquet"):
        df.to_parquet(path, index=False)

    elif file_name.endswith(".csv"):
        df.to_csv(path, index=False)

    else:
        raise ValueError("Gunakan nama file berakhiran .parquet atau .csv")

    print(f"Saved to: {path}")


# ===============================
# +      Path Configuration     +
# ===============================
TAXI_TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
ZONE_TAXI_URL  = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "data_lake" / "raw"
STAG_PATH = BASE_DIR / "data_lake" / "staging"
RAW_ZONE_TAXI  = RAW_PATH / "taxi-zone.csv"
RAW_TRIPS_TAXI = RAW_PATH / "taxi-trips.parquet"


# ==============================
# +      Exctract Pipeline     +
# ==============================
def run_extraction():
    """
    Menjalankan alur ekstraksi, pembersihan, dan penyiapan staging.
    Aman dijalankan berulang kali (Idempotent).
    """
    print("-------  [1/4] EXTRACTION  -------")

    try:
        # 1. Pastikan folder tujuan RAW_PATH dan STAG_PATH sudah dibuat
        RAW_PATH.mkdir(parents=True, exist_ok=True)
        STAG_PATH.mkdir(parents=True, exist_ok=True)

        
        # 2. Download File Raw (Melakukan caching: skip jika file lokal sudah ada)
        print("-------  DOWNLOADING FILE RAW -------")
        download(
            url=ZONE_TAXI_URL,
            output_dir=RAW_PATH,
            file_name="taxi-zone.csv"
        )

        download(
            url=TAXI_TRIPS_URL,
            output_dir=RAW_PATH,
            file_name="taxi-trips.parquet"
        )

        # 3. Reading & Cleaning Data Raw - Taxi Zones
        print("-------  CLEANING RAW FILE: ZONES  -------")
        df_zone = pd.read_csv(RAW_ZONE_TAXI)
        df_zone_clean = clean_column_names(df_zone)

        # Handling NULL Values pada Lookup Zone
        zone_cols = ['zone', 'borough', 'service_zone']
        for col in zone_cols:
            if col in df_zone_clean.columns:
                df_zone_clean[col] = df_zone_clean[col].fillna('Unknown')

        # Save Zone ke Area Staging
        print("-------  SAVE ZONE TO STAGING  -------")
        save_data(df_zone_clean, STAG_PATH, "stag-zone-taxi.csv")
        
        # Free memory untuk dataframe zone
        del df_zone, df_zone_clean
        gc.collect()

        # 4. Reading & Cleaning Data Raw - Taxi Trips
        print("-------  CLEANING RAW FILE: TRIPS  -------")
        df_trips = pd.read_parquet(RAW_TRIPS_TAXI)
        df_trips_clean = clean_column_names(df_trips)

        # Save Trips ke Area Staging
        print("-------  SAVE TRIPS TO STAGING  -------")
        save_data(df_trips_clean, STAG_PATH, "stag-trip-taxi.parquet")

        # Free memory untuk dataframe trips
        del df_trips, df_trips_clean
        gc.collect()

        print("✅ Extraction & Staging Selesai dengan Lancar!")

    except Exception as e:
        print(f"❌ Terjadi kesalahan saat proses ekstraksi: {e}")
        raise e

if __name__ == "__main__":
    run_extraction()