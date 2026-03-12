"""
config/settings.py
------------------
Global configuration for the DataPrep Pipeline.
"""
import os
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_RAW_DIR       = BASE_DIR / "data" / "raw"
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"
REPORTS_DIR        = BASE_DIR / "reports"
LOGS_DIR           = BASE_DIR / "logs"

# Default input file
DEFAULT_INPUT_FILE  = DATA_RAW_DIR / "ventas.csv"
DEFAULT_OUTPUT_FILE = DATA_PROCESSED_DIR / "ventas_cleaned.csv"
DEFAULT_REPORT_FILE = REPORTS_DIR / "quality_report.html"

# ── Pipeline version ─────────────────────────────────────────────────────────
PIPELINE_VERSION = "1.0.0"
PIPELINE_NAME    = "DataPrep Pipeline"

# ── Cleaning configuration ────────────────────────────────────────────────────
CLEANING_CONFIG = {
    # Strategy for filling numeric nulls: "mean" | "median" | "zero"
    "numeric_imputation": "mean",
    # Strategy for filling categorical nulls: "mode" | "unknown"
    "categorical_imputation": "mode",
    # Remove rows where ALL values are null
    "drop_all_null_rows": True,
    # Remove exact duplicate rows
    "drop_duplicates": True,
    # Strip whitespace from string columns
    "strip_strings": True,
    # Lowercase string columns
    "lowercase_strings": False,
    # Normalize column names (lowercase, replace spaces with _)
    "normalize_column_names": True,
}

# ── Validation thresholds ─────────────────────────────────────────────────────
VALIDATION_CONFIG = {
    # Alert if null % exceeds this value per column
    "null_threshold_pct": 20.0,
    # Alert if duplicate rows exceed this % of total
    "duplicate_threshold_pct": 5.0,
    # IQR multiplier for outlier detection
    "outlier_iqr_factor": 1.5,
}

# ── Airflow ───────────────────────────────────────────────────────────────────
AIRFLOW_DAG_ID       = "dataprep_pipeline"
AIRFLOW_SCHEDULE     = "@daily"
AIRFLOW_OWNER        = "dataprep"
AIRFLOW_START_DATE_Y = 2024
AIRFLOW_START_DATE_M = 1
AIRFLOW_START_DATE_D = 1
