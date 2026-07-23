# Pre-Capstone Project 3 — NYC Taxi Data Pipeline (Medallion Architecture)

[![Stack - Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Orchestrator - Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Database - PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Infrastructure - Docker](https://img.shields.io/badge/Docker%20Compose-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

Pipeline data lokal end-to-end untuk mengolah dataset **NYC Taxi** menggunakan **Apache Airflow**, **PostgreSQL**, **Python**, dan **Docker Compose**, dengan pendekatan **Medallion Architecture (Bronze → Silver → Gold)** yang dilengkapi **Data Quality Check**, **Business Analytics Logging**, dan **Audit Trail Logging** di setiap tahap.

Project ini merupakan latihan transisi dari Capstone Project 2 menuju Capstone Project 3 (versi cloud), dengan fokus pada orkestrasi pipeline, data lake lokal, data warehouse, transformasi data, dan containerization.

---

## Daftar Isi

- [Pre-Capstone Project 3 — NYC Taxi Data Pipeline (Medallion Architecture)](#pre-capstone-project-3--nyc-taxi-data-pipeline-medallion-architecture)
  - [Daftar Isi](#daftar-isi)
  - [1. Tujuan \& Dataset](#1-tujuan--dataset)
  - [2. Arsitektur Pipeline](#2-arsitektur-pipeline)
    - [Rincian Tahapan (Pipeline Stages)](#rincian-tahapan-pipeline-stages)
      - [1. Extract \& Staging](#1-extract--staging)
      - [2. Bronze Layer (Raw Storage)](#2-bronze-layer-raw-storage)
      - [3. Silver Layer (Cleansing \& Transformation)](#3-silver-layer-cleansing--transformation)
      - [4. Gold Layer (Data Marts \& Analytics)](#4-gold-layer-data-marts--analytics)
      - [5. Pipeline \& Validation Report](#5-pipeline--validation-report)
  - [3. Struktur Folder Project](#3-struktur-folder-project)
  - [4. Cara Menjalankan Project](#4-cara-menjalankan-project)
    - [Langkah 1 — Clone Repository](#langkah-1--clone-repository)
    - [Langkah 2 — Siapkan Environment File](#langkah-2--siapkan-environment-file)
    - [Langkah 3 — Pastikan Docker Desktop Aktif](#langkah-3--pastikan-docker-desktop-aktif)
    - [Langkah 4 — Jalankan Services via Docker Compose](#langkah-4--jalankan-services-via-docker-compose)
    - [Langkah 5 — Trigger DAG](#langkah-5--trigger-dag)
  - [5. Environment Variables](#5-environment-variables)
  - [6. Airflow DAG \& Task Dependency](#6-airflow-dag--task-dependency)
    - [Urutan Task (Grid/Graph View)](#urutan-task-gridgraph-view)
  - [7. Desain Database \& Schema](#7-desain-database--schema)
    - [A. Audit Schema (`audit`)](#a-audit-schema-audit)
    - [B. Bronze Layer (`bronze`)](#b-bronze-layer-bronze)
    - [C. Silver Layer (`silver`)](#c-silver-layer-silver)
    - [D. Gold Layer (`gold`) — Data Mart](#d-gold-layer-gold--data-mart)
  - [8. Data Quality Check](#8-data-quality-check)
  - [9. Idempotency \& Rerun Safety](#9-idempotency--rerun-safety)
  - [10. Monitoring \& Verifikasi Hasil](#10-monitoring--verifikasi-hasil)
    - [Verifikasi via Airflow UI](#verifikasi-via-airflow-ui)
    - [Verifikasi via Query PostgreSQL](#verifikasi-via-query-postgresql)
  - [11. Asumsi \& Batasan Teknis](#11-asumsi--batasan-teknis)

---

## 1. Tujuan & Dataset

| Item | Keterangan |
| :--- | :--- |
| **Tujuan** | Membangun data pipeline lokal end-to-end sebagai latihan sebelum Capstone Project 3 berbasis cloud |
| **Dataset** | NYC Taxi Trip Data (`.parquet`) dan Taxi Zone Lookup (`.csv`) — dataset yang sama dengan Capstone Project 2 |
| **Tools** | Apache Airflow, PostgreSQL, Python, Docker & Docker Compose, SQL |
| **Pendekatan** | Medallion Architecture (Bronze → Silver → Gold) + Audit Logging + Data Quality |
| **Penilaian** | Tidak ada penilaian numerik — dokumen requirement berfungsi sebagai checklist kesiapan |

---

## 2. Arsitektur Pipeline

![Pipeline Architecture](docs/images/architecture-diagram.png)
*Gambar 1: Arsitektur Pipeline Data & Medallion Architecture*

### Rincian Tahapan (Pipeline Stages)

#### 1. Extract & Staging

* **Raw Ingestion**: Mengunduh dataset mentah dari source URL (`raw-taxi-trips.parquet` dan `raw-taxi-zones.csv`) ke direktori `data_lake/raw/`.
* **Data Structuring**: Melakukan pembersihan cepat (*quick clean*) untuk menyelaraskan tipe data dan merapikan struktur data awal.
* **Staging Output**: Menyimpan hasil olahan awal ke `data_lake/staging/` dengan nama `stag-taxi-trips` untuk siap di-load ke database.

#### 2. Bronze Layer (Raw Storage)

* **Schema Initialization**: Menginisialisasi skema database `bronze` beserta DDL tabel utama secara otomatis.
* **Automated Bulk Load**: Memindahkan data dari `data_lake/staging/` dan lookup zona ke PostgreSQL menggunakan mekanisme *bulk loading*.
* **Lineage Preservation**: Mempertahankan format data asli dengan perubahan seminimal mungkin (*preserve source data*) untuk kebutuhan auditabilitas.

#### 3. Silver Layer (Cleansing & Transformation)

* **Data Cleansing**: Memvalidasi dan menyaring data mentah dari anomali bisnis (misal: tarif ≤ 0 atau jarak perjalanan ≤ 0).
* **Feature Enrichment**: Menggenerasi kolom-kolom baru yang siap pakai (*business-friendly columns*) untuk mempermudah query analitik.
* **Quality Quarantine**: Memisahkan record yang tidak valid ke tabel khusus `silver.data_quality_issues` untuk analisis penjaminan kualitas data.

#### 4. Gold Layer (Data Marts & Analytics)

* **Data Mart Aggregation**: Membangun *analytical data marts* teragregasi (performa harian, pola jam sibuk, per zona, dan metode pembayaran).
* **Reporting Views**: Menyediakan *view* khusus yang dioptimalkan untuk kebutuhan *querying*, dashboarding, serta pelaporan bisnis.
* **Business Insights**: Menyajikan metrik bisnis utama (*actionable metrics*) yang siap dikonsumsi untuk mempercepat pengambilan keputusan strategis.

#### 5. Pipeline & Validation Report

* **Execution Summary**: Mengompilasi ringkasan eksekusi pipeline secara menyeluruh, mencakup durasi waktu (*benchmark*) dan total baris terproses.
* **Multi-Layer Validation**: Menjalankan pengujian kualitas data (*data quality checks*) secara otomatis di setiap layer (Bronze, Silver, Gold).
* **Audit Trail Logging**: Mencatat status akhir validasi dan statistik eksekusi ke `audit.load_audit` dengan standar waktu **Asia/Jakarta (WIB)**.

---

## 3. Struktur Folder Project

```text
pre-capstone-project-3/
│
├── dags/
│   └── taxi_pipeline.py            # DAG utama: nyc_taxi_medallion_pipeline
│
├── data_lake/                      # Data_lake lokal
│   ├── raw/                        
│   └── staging/                    
│
├── docs/
│   └── images/                     
│
├── logs/                           # Log eksekusi & business analytics log
│
├── scripts/
│   ├── extract.py                  # Script ingestion data mentah
│   ├── initial_database.py         # Script inisialisasi skema DB
│   ├── load_to_postgres.py         # Script load data ke Bronze
│   ├── quality_check.py            # Engine validasi kualitas data
│   ├── transform.py                # Script transformasi Silver & Gold
│   └── utils_helper.py             # Helper function 
│
├── sql/
│   ├── 01-schema.sql               # Script DDL skema DB
│   ├── 02-bronze.sql               # Script DDL Bronze layer
│   ├── 03-silver.sql               # Script ETL Silver layer
│   └── 04-gold-mart.sql            # Script agregasi Gold Data Mart
│  
├── .env.example                    # Template konfigurasi environment
├── .gitignore
│
├── docker-compose.yml              # Konfigurasi container Airflow & PostgreSQL
├── Dockerfile                      # Custom image Airflow
├── .dockerignore
│
├── requirements.txt
├── README_IDN.md
└── README.md
```

> **Catatan Arsitektur:** Layer `processed/` pada data lake tidak dibuat secara terpisah karena proses cleansing/standardisasi langsung dilakukan di dalam PostgreSQL (Silver Layer), sehingga tidak diperlukan file perantara tambahan di filesystem.

---

## 4. Cara Menjalankan Project

### Langkah 1 — Clone Repository

```bash
git clone https://github.com/alfianfebiyanto/pre-capstone-project-3.git
cd pre-capstone-project-3
```

### Langkah 2 — Siapkan Environment File

```bash
cp .env.example .env
```

Sesuaikan variabel `.env` (kredensial database, port, dsb.) jika diperlukan.

### Langkah 3 — Pastikan Docker Desktop Aktif

Buka **Docker Desktop** dan pastikan status engine sudah **Running**.

### Langkah 4 — Jalankan Services via Docker Compose

```bash
docker compose up -d
```

Perintah ini akan menjalankan container **PostgreSQL** dan **Apache Airflow** (webserver + scheduler), lengkap dengan volume untuk persistensi data dan mount folder `dags/` serta `data_lake/`.

### Langkah 5 — Trigger DAG

DAG dapat dipicu secara manual melalui Airflow UI (`http://localhost:8084`), atau menunggu jadwal `@daily` berjalan otomatis.

---

## 5. Environment Variables

Contoh isi `.env.example`:

```env
# Airflow
AIRFLOW_UID=50000
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD= airflow_pass
AIRFLOW_DB_NAME=airflow

# Pre-Caps3-Project
PRE_CAPS3_USER=your_project_db_user_here
PRE_CAPS3_PASS=your_project_db_password_here
PRE_CAPS3_DB=your_project_db_name_here
```

> Kredensial **tidak** di-hardcode di dalam script maupun `docker-compose.yml`, melainkan diambil dari file `.env` agar konfigurasi tetap aman dan mudah direplikasi.

---

## 6. Airflow DAG & Task Dependency

* **URL Airflow UI:** `http://localhost:8084`
* **Username / Password:** `admin` / `admin` (atau sesuai `.env`)
* **Nama DAG:** `nyc_taxi_medallion_pipeline`

### Urutan Task (Grid/Graph View)

```text
extraction_stage
        ↓
initialize_database_schema
        ↓
load_staging_to_bronze
        ↓
quality_bronze_layer
        ↓
transform_silver
        ↓
quality_silver_layer
        ↓
transform_gold_mart
        ↓
quality_gold_layer
        ↓
business_analytics_execution
        ↓
pipeline_execution_report
```

![Airflow DAG Graph View](docs/images/airflow-dag-graph.png)
*Gambar 2: Tampilan DAG Graph View di Airflow UI (Seluruh Task Berhasil)*

**Kriteria Pipeline Berhasil:**

* Seluruh task berwarna hijau (*success*).
* Status **DAG Run** = **Success**.
* Log task `business_analytics_execution` berhasil mencatat output kueri ke `logs/business_analytics.log`.
* Log task `pipeline_execution_report` mencetak ringkasan durasi & jumlah row per stage dengan zona waktu **Asia/Jakarta (WIB)**.

---

## 7. Desain Database & Schema

```text
Bronze Layer (Raw)  ──▶  Silver Layer (Cleansed)  ──▶  Gold Layer (Data Marts)
                                                               │
                                                        Audit Schema (Logging)
```

![Skema Database PostgreSQL](docs/images/postgres-tables.png)
*Gambar 3: Struktur Skema dan Tabel PostgreSQL*

### A. Audit Schema (`audit`)

| Tabel | Deskripsi |
| --- | --- |
| `audit.load_audit` | Mencatat histori eksekusi tiap tahap pipeline: `run_id` (UUID v5 deterministik dari `run_id` Airflow), `stage`, `object_name`, `rows_affected`, `status` (STARTED/SUCCESS/FAILED), `started_at`, `finished_at`, `error_message` |

### B. Bronze Layer (`bronze`)

| Tabel | Deskripsi |
| --- | --- |
| `bronze.raw_taxi_trips` | Data mentah perjalanan taksi dari file Parquet |
| `bronze.raw_taxi_zones` | Data mentah referensi lokasi/zona dari file CSV |

### C. Silver Layer (`silver`)

| Tabel | Deskripsi |
| --- | --- |
| `silver.taxi_trips_cleaned` | Data trip yang sudah dibersihkan (tarif/jarak ≤ 0 dibuang) dan diperkaya kolom tanggal, jam, dan zona |
| `silver.taxi_zones` | Data zona taksi terverifikasi, tanpa duplikasi `location_id` |
| `silver.data_quality_issues` | Menampung record anomali yang tersaring dari Bronze untuk audit kualitas data |

### D. Gold Layer (`gold`) — Data Mart

| Tabel | Deskripsi |
| --- | --- |
| `gold.daily_trip_summary` | Ringkasan harian: total trip, total pendapatan, rata-rata tarif/jarak/durasi |
| `gold.hourly_demand_summary` | Pola permintaan per jam pickup dan rata-rata pendapatan |
| `gold.zone_performance_summary` | Performa tiap zona: total pickup, dropoff, revenue, rata-rata tip |
| `gold.payment_behavior_summary` | Proporsi & statistik perjalanan per metode pembayaran |
| `gold.route_performance_summary` | Performa rute favorit (pickup–dropoff) dengan ambang batas minimal perjalanan |

> Requirement minimal hanya meminta **1 tabel agregasi/data mart**; project ini menyediakan **5 tabel gold mart** untuk cakupan analisis yang lebih luas (harian, per jam, per zona, per metode pembayaran, dan per rute).

---

## 8. Data Quality Check

Quality check dijalankan **di beberapa titik** (*fail-fast mechanism*), bukan hanya di akhir pipeline, agar pipeline dapat berhenti lebih cepat saat ada masalah:

| Checkpoint | Validasi |
| --- | --- |
| **Bronze** (`quality_bronze_layer`) | Row count > 0, tabel berhasil terbentuk, jumlah row sesuai sumber data |
| **Silver** (`quality_silver_layer`) | Kolom tanggal utama tidak null, `total_amount` ≥ 0, `trip_distance` ≥ 0, tidak ada duplikasi key unik |
| **Gold** (`quality_gold_layer`) | Tabel mart berhasil terbentuk, row count > 0, query agregasi utama dapat dijalankan |

![Log Data Quality Check](docs/images/data-quality-log.png)
*Gambar 4: Tangkapan Layar Log Verification & Quality Check Execution*

Semua hasil pengecekan dicatat ke `audit.load_audit` sehingga histori kegagalan/keberhasilan dapat ditelusuri secara transparan.

---

## 9. Idempotency & Rerun Safety

Pipeline dirancang agar **aman dijalankan ulang** (*rerun*) untuk periode data yang sama tanpa menghasilkan duplikasi:

| Layer | Strategi Idempotency |
| --- | --- |
| **Bronze** | *Delete by period* (berdasarkan `run_date`/`execution_date`) diikuti *insert ulang*, atau *truncate + reload* untuk full load harian |
| **Silver** | Tabel di-*rebuild* (`CREATE OR REPLACE` / truncate-insert) dari Bronze setiap kali DAG dijalankan, sehingga hasil selalu konsisten dengan input terbaru |
| **Gold** | Data mart di-*rebuild* penuh dari Silver setiap run, tidak menggunakan *append* tanpa kontrol |

Parameter periode data (`run_date` / `execution_date`) digunakan agar cakupan data yang diproses selalu jelas. Jika DAG gagal di tengah jalan, rerun akan memperbaiki state tanpa perlu cleanup manual.

---

## 10. Monitoring & Verifikasi Hasil

### Verifikasi via Airflow UI

1. Login ke `http://localhost:8084`.
2. Cari DAG `nyc_taxi_medallion_pipeline` → buka **Grid** / **Graph View**.
3. Pastikan seluruh task berwarna hijau dan DAG Run berstatus **Success**.

### Verifikasi via Query PostgreSQL

```sql
-- Cek jumlah row tiap layer
SELECT COUNT(*) FROM bronze.raw_taxi_trips;
SELECT COUNT(*) FROM silver.taxi_trips_cleaned;
SELECT COUNT(*) FROM gold.daily_trip_summary;

-- Cek tidak ada anomali di Silver
SELECT COUNT(*) FROM silver.taxi_trips_cleaned
WHERE total_amount < 0 OR trip_distance < 0;

-- Cek histori audit run terakhir
SELECT run_id, stage, status, rows_affected, started_at, finished_at
FROM audit.load_audit
ORDER BY started_at DESC
LIMIT 10;
```

---

## 11. Asumsi & Batasan Teknis

* Pipeline berjalan **sepenuhnya lokal**, belum menggunakan storage cloud (GCS/S3/Azure Blob) maupun managed warehouse (BigQuery/Redshift/Snowflake).
* Dataset dapat diperkecil (subset bulan tertentu) agar pipeline ringan dijalankan di laptop.
* Skema waktu pelaporan (`pipeline_execution_report`) menggunakan zona waktu **Asia/Jakarta (WIB)**.
* Fokus utama adalah pemahaman alur pipeline end-to-end, bukan optimasi skala produksi.
* Logic dari Capstone Project 2 (jika ada) dipakai ulang namun dibungkus ulang sebagai task Airflow.