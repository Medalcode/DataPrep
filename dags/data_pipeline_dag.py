"""
dags/data_pipeline_dag.py
--------------------------
Apache Airflow DAG for the DataPrep Pipeline.
Orchestrates the full ETL process on a daily schedule.

Setup:
    1. Install Airflow: pip install apache-airflow
    2. Set AIRFLOW_HOME or copy this file to your $AIRFLOW_HOME/dags directory
    3. airflow db init
    4. airflow webserver --port 8080
    5. airflow scheduler
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path so src/ imports work
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator

from config.settings import (
    DEFAULT_INPUT_FILE,
    DEFAULT_OUTPUT_FILE,
    DEFAULT_REPORT_FILE,
    CLEANING_CONFIG,
    VALIDATION_CONFIG,
    PIPELINE_VERSION,
    AIRFLOW_DAG_ID,
    AIRFLOW_SCHEDULE,
    AIRFLOW_OWNER,
    AIRFLOW_START_DATE_Y,
    AIRFLOW_START_DATE_M,
    AIRFLOW_START_DATE_D,
    LOGS_DIR,
)

# ── Default DAG args ──────────────────────────────────────────────────────────
default_args = {
    "owner": AIRFLOW_OWNER,
    "depends_on_past": False,
    "start_date": datetime(AIRFLOW_START_DATE_Y, AIRFLOW_START_DATE_M, AIRFLOW_START_DATE_D),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── Pipeline task functions ───────────────────────────────────────────────────

def task_ingest(**context):
    """Ingest raw CSV data and push to XCom."""
    from src.ingestion import load_csv
    from src.logger import get_logger
    logger = get_logger("airflow.ingest", log_dir=LOGS_DIR)

    logger.info(f"Ingesting data from {DEFAULT_INPUT_FILE}")
    df = load_csv(DEFAULT_INPUT_FILE)
    logger.info(f"Ingested {len(df)} rows")

    # Serialize to JSON for XCom (partition large datasets differently in production)
    context["ti"].xcom_push(key="df_json", value=df.to_json(orient="split"))
    logger.info("Data pushed to XCom")


def task_validate(**context):
    """Validate raw data and log quality report."""
    import json
    import pandas as pd
    from src.validation import validate_data, DataQualityReport
    from src.logger import get_logger
    logger = get_logger("airflow.validate", log_dir=LOGS_DIR)

    df_json = context["ti"].xcom_pull(task_ids="ingest_data", key="df_json")
    df = pd.read_json(df_json, orient="split")

    report = validate_data(
        df,
        null_threshold_pct=VALIDATION_CONFIG["null_threshold_pct"],
        duplicate_threshold_pct=VALIDATION_CONFIG["duplicate_threshold_pct"],
    )

    summary = report.summary_dict()
    logger.info(f"Validation complete: {summary}")

    if report.alerts:
        for alert in report.alerts:
            logger.warning(alert)

    # Serialize the *full* DataQualityReport so task_load can build an
    # accurate before/after comparison in the HTML report.
    # Only primitive types are stored so XCom (SQLite backend) can handle it.
    before_report_dict = {
        "total_rows":               report.total_rows,
        "total_cols":               report.total_cols,
        "duplicate_rows":           report.duplicate_rows,
        "duplicate_pct":            report.duplicate_pct,
        "null_counts":              report.null_counts,
        "null_pcts":                report.null_pcts,
        "dtypes":                   report.dtypes,
        "outlier_counts":           report.outlier_counts,
        "columns_with_mixed_types": report.columns_with_mixed_types,
        "alerts":                   report.alerts,
    }

    context["ti"].xcom_push(key="df_json", value=df.to_json(orient="split"))
    context["ti"].xcom_push(key="before_report", value=json.dumps(before_report_dict))


def task_clean(**context):
    """Apply data cleaning rules and push cleaned data."""
    import pandas as pd
    from src.cleaning import clean_data
    from src.logger import get_logger
    logger = get_logger("airflow.clean", log_dir=LOGS_DIR)

    df_json = context["ti"].xcom_pull(task_ids="validate_data", key="df_json")
    df = pd.read_json(df_json, orient="split")

    df_clean = clean_data(df, config=CLEANING_CONFIG)
    logger.info(f"Cleaned: {len(df)} → {len(df_clean)} rows")

    context["ti"].xcom_push(key="df_json", value=df_clean.to_json(orient="split"))


def task_transform(**context):
    """Apply data transformations."""
    import pandas as pd
    from src.transformation import transform_data
    from src.logger import get_logger
    logger = get_logger("airflow.transform", log_dir=LOGS_DIR)

    df_json = context["ti"].xcom_pull(task_ids="clean_data", key="df_json")
    df = pd.read_json(df_json, orient="split")

    df_transformed = transform_data(df)
    logger.info(f"Transformed: {len(df_transformed.columns)} output columns")

    context["ti"].xcom_push(key="df_json", value=df_transformed.to_json(orient="split"))


def task_load(**context):
    """Save cleaned/transformed data to CSV and generate quality report."""
    import json
    import pandas as pd
    from src.validation import validate_data, DataQualityReport
    from src.report import generate_quality_report
    from src.logger import get_logger
    logger = get_logger("airflow.load", log_dir=LOGS_DIR)

    df_json = context["ti"].xcom_pull(task_ids="transform_data", key="df_json")
    df = pd.read_json(df_json, orient="split")

    # Save clean dataset
    DEFAULT_OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DEFAULT_OUTPUT_FILE, index=False, encoding="utf-8")
    logger.info(f"Saved {len(df)} clean rows to {DEFAULT_OUTPUT_FILE}")

    # Validate after-pipeline data
    after_report = validate_data(df)

    # Reconstruct the full before-pipeline DataQualityReport from XCom.
    # This gives the HTML report accurate "before vs after" metrics.
    before_report_json = context["ti"].xcom_pull(task_ids="validate_data", key="before_report")
    before_report_dict = json.loads(before_report_json)

    before_report = DataQualityReport(
        total_rows=before_report_dict["total_rows"],
        total_cols=before_report_dict["total_cols"],
        duplicate_rows=before_report_dict["duplicate_rows"],
        duplicate_pct=before_report_dict["duplicate_pct"],
        null_counts=before_report_dict["null_counts"],
        null_pcts=before_report_dict["null_pcts"],
        dtypes=before_report_dict["dtypes"],
        outlier_counts={k: int(v) for k, v in before_report_dict["outlier_counts"].items()},
        columns_with_mixed_types=before_report_dict["columns_with_mixed_types"],
        alerts=before_report_dict["alerts"],
    )
    logger.info(
        f"Before report restored from XCom — "
        f"{before_report.total_rows} rows, {len(before_report.alerts)} alerts"
    )

    # Generate report with real before/after comparison
    DEFAULT_REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    generate_quality_report(
        before_report=before_report,
        after_report=after_report,
        report_path=DEFAULT_REPORT_FILE,
        version=PIPELINE_VERSION,
    )
    logger.info(f"Quality report saved to {DEFAULT_REPORT_FILE}")


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id=AIRFLOW_DAG_ID,
    default_args=default_args,
    description="DataPrep Pipeline — Automated daily data cleaning and transformation",
    schedule_interval=AIRFLOW_SCHEDULE,
    catchup=False,
    tags=["dataprep", "etl", "data-engineering"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=task_ingest,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=task_validate,
    )

    clean = PythonOperator(
        task_id="clean_data",
        python_callable=task_clean,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=task_load,
    )

    # ── DAG flow ──────────────────────────────────────────────────────────────
    ingest >> validate >> clean >> transform >> load
