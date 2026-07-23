# ============================
# +     Import Libarary      +
# ============================

import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import fungsi ETL dari folder scripts
from extract import exctraction_stage
from initial_database import initial_stage
from load_to_postgres import load_postgres_stage
from transform import goldmart_transform, silver_transform
from quality_check import (
    validate_bronze_layer,
    validate_goldmart_layer,
    validate_silver_layer,
)

# Set-up Logging
from utils_helper import setup_logger
logger = setup_logger(__name__)

# Definisikan zona waktu Jakarta
JAKARTA_TZ = ZoneInfo("Asia/Jakarta")


# ===============================
# +      Helper Functions       +
# ===============================

def get_airflow_run_id(**context) -> uuid.UUID:

    """
    Mengonversi `dag_run.run_id` dari Airflow menjadi UUID v5 deterministik
    agar konsisten digunakan sebagai `run_id` di tabel audit.load_audit.
    """

    dag_run = context.get("dag_run")
    raw_run_id = dag_run.run_id if dag_run else str(uuid.uuid4())
    return uuid.uuid5(uuid.NAMESPACE_OID, raw_run_id)


def print_pipeline_report(**context) -> None:

    """Mencetak laporan ringkasan status eksekusi pipeline dalam timezone Asia/Jakarta (WIB)."""

    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")

    end_time = datetime.now(timezone.utc).astimezone(JAKARTA_TZ)
    start_time = (
        dag_run.start_date.astimezone(JAKARTA_TZ)
        if (dag_run and dag_run.start_date)
        else end_time
    )

    execution_duration = end_time - start_time

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


# ===============================
# +    Airflow DAG Definition   +
# ===============================

default_args = {
    "owner": "fian",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

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
        python_callable=lambda **ctx: load_postgres_stage(
            run_id=get_airflow_run_id(**ctx)
        ),
    )

    # Task 4: Quality Check Bronze Layer
    quality_bronze_layer = PythonOperator(
        task_id="quality_bronze_layer",
        python_callable=lambda **ctx: validate_bronze_layer(
            run_id=get_airflow_run_id(**ctx)
        ),
    )

    # Task 5: Transformation Silver Layer
    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=lambda **ctx: silver_transform(
            run_id=get_airflow_run_id(**ctx)
        ),
    )

    # Task 6: Quality Check Silver Layer
    quality_silver_layer = PythonOperator(
        task_id="quality_silver_layer",
        python_callable=lambda **ctx: validate_silver_layer(
            run_id=get_airflow_run_id(**ctx)
        ),
    )

    # Task 7: Transformation Gold Layer
    transform_gold_mart = PythonOperator(
        task_id="transform_gold_mart",
        python_callable=lambda **ctx: goldmart_transform(
            run_id=get_airflow_run_id(**ctx)
        ),
    )

    # Task 8: Quality Check Gold Layer
    quality_gold_layer = PythonOperator(
        task_id="quality_gold_layer",
        python_callable=lambda **ctx: validate_goldmart_layer(
            run_id=get_airflow_run_id(**ctx)
        ),
    )

    # Task 9: Pipeline Execution Report & Summary
    pipeline_report = PythonOperator(
        task_id="pipeline_execution_report",
        python_callable=print_pipeline_report,
    )

    # Flow Task
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