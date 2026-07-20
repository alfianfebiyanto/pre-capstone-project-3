# dags/test_dag.py

import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Tambahkan path folder 'scripts' ke system path agar Airflow bisa meng-import modulmu
BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
sys.path.append(str(SCRIPTS_DIR))

# 2. Import fungsi-fungsi pipeline yang sudah kamu buat sebelumnya
from extract import run_extraction
from initial_database import run_initial
from load_to_postgre import run_load_to_postgres
from transform import run_silver_transformation, run_gold_transformation
from quality_check import check_bronze_table, check_silver_table, check_gold_output

# 3. Default Arguments untuk DAG
default_args = {
    'owner': 'fian',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# 4. Definisi DAG
with DAG(
    dag_id='nyc_taxi_medallion_pipeline',
    default_args=default_args,
    description='ETL Pipeline Data NYC Taxi dengan Medallion Architecture (Bronze -> Silver -> Gold)',
    schedule_interval=None,  # Atau '0 2 * * *' untuk jadwal tiap jam 2 pagi
    catchup=False,
    tags=['data_engineering', 'nyc_taxi', 'postgres'],
) as dag:

    # Task 1: Extraction & Staging
    task_extraction = PythonOperator(
        task_id='extract_and_stage_data',
        python_callable=run_extraction,
    )

    # Task 2: Initial Database / Re-create Schema
    task_initial_db = PythonOperator(
        task_id='initialize_database_schemas',
        python_callable=run_initial,
    )

    # Task 3: Load Data Staging ke Bronze Layer
    task_load_bronze = PythonOperator(
        task_id='load_staging_to_bronze',
        python_callable=run_load_to_postgres,
    )

    # --- TASK QUALITY CHECK 1: BRONZE ---
    task_check_bronze = PythonOperator(
        task_id='check_bronze_table',
        python_callable=check_bronze_table,
    )

    # Task 4: Transformation (Silver Layer)
    task_transform_silver = PythonOperator(
        task_id='transform_silver',
        python_callable=run_silver_transformation,
    )
    # --- TASK QUALITY CHECK 2: SILVER ---
    task_check_silver = PythonOperator(
        task_id='check_silver_table',
        python_callable=check_silver_table,
    )

    # Task 5: Transformation (Gold Layers)
    task_transform_gold = PythonOperator(
        task_id='transform_gold',
        python_callable=run_gold_transformation,
    )

    # --- TASK QUALITY CHECK 2: GOLD ---
    task_check_gold = PythonOperator(
        task_id='check_gold_output',
        python_callable=check_gold_output,
    )

    # 5. Dependency / Urutan Eksekusi Task
    task_extraction >> task_initial_db >> task_load_bronze >> task_check_bronze >> task_transform_silver >>task_check_silver>> task_transform_gold >> task_check_gold