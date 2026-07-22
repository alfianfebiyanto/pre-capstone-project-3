# # Import Library
# import sys
# from pathlib import Path
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator

# from extract import run_extraction
# from initial_database import run_initial
# from load_to_postgre import run_load_to_postgres
# from transform import run_silver_transformation, run_gold_transformation
# from quality_check import check_bronze_table, check_silver_table, check_gold_output

# # path Konfiguration
# BASE_DIR = Path(__file__).resolve().parent.parent
# SCRIPTS_DIR = BASE_DIR / "scripts"
# sys.path.append(str(SCRIPTS_DIR))

# # Default Arguments untuk DAG
# default_args = {
#     'owner': 'fian',
#     'depends_on_past': False,
#     'start_date': datetime(2026, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # DAG
# with DAG(
#     dag_id='nyc_taxi_medallion_pipeline',
#     default_args=default_args,
#     description='ETL Pipeline Data NYC Taxi dengan Medallion Architecture (Bronze -> Silver -> Gold)',
#     schedule_interval='@daily',  
#     catchup=False,
#     tags=['data_engineering', 'nyc_taxi', 'postgres','alfian','pre-cpastone3'],
# ) as dag:

#     # Task 1: Extraction & Staging
#     extraction = PythonOperator(
#         task_id='extraction_stage',
#         python_callable=run_extraction,
#     )

#     # Task 2: Initial Database 
#     initial_db = PythonOperator(
#         task_id='initialize_database_schema',
#         python_callable=run_initial,
#     )

#     # Task 3: Load Data Staging ke Bronze Layer
#     load_staging_to_bronze = PythonOperator(
#         task_id='load_staging_to_bronze',
#         python_callable=run_load_to_postgres,
#     )

#     # --- TASK QUALITY CHECK 1: BRONZE Layer ---
#     quality_bronze_layer = PythonOperator(
#         task_id='quality_bronze_layer',
#         python_callable=check_bronze_table,
#     )

#     # Task 4: Transformation Silver Layer
#     transform_silver = PythonOperator(
#         task_id='transform_silver',
#         python_callable=run_silver_transformation,
#     )
#     # --- TASK QUALITY CHECK 2: SILVER LAYER ---
#     quality_silver_layer = PythonOperator(
#         task_id='quality_silver_layer',
#         python_callable=check_silver_table,
#     )

#     # Task 5: Transformation Gold Layers
#     transform_gold_mart = PythonOperator(
#         task_id='transform_gold_mart',
#         python_callable=run_gold_transformation,
#     )

#     # --- TASK QUALITY CHECK 3: GOLD LAYER ---
#     quality_gold_layer = PythonOperator(
#         task_id='quality_gold_layer',
#         python_callable=check_gold_output,
#     )

#     # 5. Urutan Eksekusi Task
#     extraction >> initial_db >> load_staging_to_bronze >> quality_bronze_layer >> transform_silver >> quality_silver_layer >> transform_gold_mart >> quality_gold_layer


# +++++++ BATAS CODE BARU CUY ++++++ 
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import fungsi ETL dari folder scripts
from extract import exctraction_stage
from initial_database import initial_stage
from load_to_postgres import load_postgres_stage
from transform import silver_transform, goldmart_transform
from quality_check import validate_bronze_layer, validate_silver_layer, validate_goldmart_layer


from utils_helper import setup_logger
logger = setup_logger(__name__)

# ===============================
# +     Path Configuration      +
# ===============================
BASE_DIR = Path(__file__).resolve().parent.parent

# 1. Definisikan zona waktu Jakarta sekali di tingkat modul (efisien & reusable)
JAKARTA_TZ = ZoneInfo("Asia/Jakarta")

def print_pipeline_report(**context) -> None:
    """Mencetak laporan ringkasan status eksekusi pipeline dalam timezone Asia/Jakarta (WIB)."""
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")

    # 2. Ambil waktu UTC dan konversi langsung ke WIB menggunakan JAKARTA_TZ
    end_time = datetime.now(timezone.utc).astimezone(JAKARTA_TZ)
    start_time = (
        dag_run.start_date.astimezone(JAKARTA_TZ)
        if (dag_run and dag_run.start_date)
        else end_time
    )

    # 3. Hitung durasi eksekusi
    execution_duration = end_time - start_time

    # 4. Cetak ringkasan laporan
    logger.info("=" * 60)
    logger.info("       NYC TAXI PIPELINE EXECUTION SUMMARY REPORT      ")
    logger.info("=" * 60)
    logger.info("DAG ID            : %s", task_instance.dag_id if task_instance else "N/A")
    logger.info("Run ID            : %s", dag_run.run_id if dag_run else "N/A")
    logger.info("Execution Date    : %s", context.get("ds"))
    logger.info("Start Time        : %s WIB", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("End Time          : %s WIB", end_time.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("Total Duration    : %s", str(execution_duration).split(".")[0])
    logger.info("Pipeline Status   : SUCCESS")
    logger.info("Architecture      : Medallion (Bronze -> Silver -> Gold)")
    logger.info("=" * 60)

# Default arguments untuk Airflow DAG
default_args = {
    "owner": "fian",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Inisialisasi DAG
with DAG(
    dag_id="nyc_taxi_medallion_pipeline",
    default_args=default_args,
    description="ETL Pipeline Data NYC Taxi dengan Medallion Architecture (Bronze -> Silver -> Gold)",
    schedule_interval="@daily",
    catchup=False,
    tags=["data_engineering", "nyc_taxi", "alfian", "pre-capstone3"],
) as dag:

    # Task 1: Extraction & Staging
    extraction = PythonOperator(
        task_id="extraction_stage",
        python_callable=exctraction_stage,
    )

    # Task 2: Initial Database Setup
    initial_db = PythonOperator(
        task_id="initialize_database_schema",
        python_callable=initial_stage,
    )

    # Task 3: Load Data Staging ke Bronze Layer
    load_staging_to_bronze = PythonOperator(
        task_id="load_staging_to_bronze",
        python_callable=load_postgres_stage,
    )

    # Task 4: Quality Check Bronze Layer
    quality_bronze_layer = PythonOperator(
        task_id="quality_bronze_layer",
        python_callable=validate_bronze_layer,
    )

    # Task 5: Transformation Silver Layer
    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=silver_transform,
    )

    # Task 6: Quality Check Silver Layer
    quality_silver_layer = PythonOperator(
        task_id="quality_silver_layer",
        python_callable=validate_silver_layer,
    )

    # Task 7: Transformation Gold Layer
    transform_gold_mart = PythonOperator(
        task_id="transform_gold_mart",
        python_callable=goldmart_transform,
    )

    # Task 8: Quality Check Gold Layer
    quality_gold_layer = PythonOperator(
        task_id="quality_gold_layer",
        python_callable=validate_goldmart_layer,
    )

    # Task 9: Pipeline Execution Report & Summary
    pipeline_report = PythonOperator(
        task_id="pipeline_execution_report",
        python_callable=print_pipeline_report,
        provide_context=True,
    )

    # Dependency / Urutan Eksekusi Task
    (
        extraction
        >> initial_db
        >> load_staging_to_bronze
        >> quality_bronze_layer
        >> transform_silver
        >> quality_silver_layer
        >> transform_gold_mart
        >> quality_gold_layer
        >> pipeline_report
    )