"""
main.py
-------
Standalone pipeline runner for DataPrep Pipeline.
Executes the full ETL process: Ingest → Validate → Clean → Transform → Load → Report.

Usage:
    python main.py
    python main.py --input data/raw/ventas.csv --output data/processed/out.csv
"""
import argparse
import sys
import time
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import (
    DEFAULT_INPUT_FILE,
    DEFAULT_OUTPUT_FILE,
    DEFAULT_REPORT_FILE,
    CLEANING_CONFIG,
    VALIDATION_CONFIG,
    PIPELINE_NAME,
    PIPELINE_VERSION,
    LOGS_DIR,
)
from src.logger import get_logger
from src.ingestion import load_csv
from src.validation import validate_data
from src.cleaning import clean_data
from src.transformation import transform_data
from src.report import generate_quality_report

logger = get_logger("main", log_dir=LOGS_DIR)


def run_pipeline(input_path: Path, output_path: Path, report_path: Path) -> bool:
    """
    Execute the full DataPrep pipeline.

    Returns:
        True if pipeline completed successfully, False otherwise.
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info(f"{PIPELINE_NAME} v{PIPELINE_VERSION} — Pipeline Started")
    logger.info("=" * 60)

    # ── STEP 1: INGEST ──────────────────────────────────────────────────────
    logger.info("[1/5] INGESTION")
    try:
        df_raw = load_csv(input_path)
    except FileNotFoundError as e:
        logger.error(f"Input file not found: {e}")
        logger.error("Run 'python generate_dataset.py' first to create test data.")
        return False

    # ── STEP 2: VALIDATE (before) ───────────────────────────────────────────
    logger.info("[2/5] VALIDATION (before cleaning)")
    before_report = validate_data(
        df_raw,
        null_threshold_pct=VALIDATION_CONFIG["null_threshold_pct"],
        duplicate_threshold_pct=VALIDATION_CONFIG["duplicate_threshold_pct"],
        iqr_factor=VALIDATION_CONFIG["outlier_iqr_factor"],
    )
    logger.info(f"  → {before_report.total_rows} rows | {before_report.duplicate_rows} dups "
                f"| {sum(before_report.null_counts.values())} nulls "
                f"| {len(before_report.alerts)} alerts")
    if before_report.alerts:
        for alert in before_report.alerts:
            logger.warning(f"  {alert}")

    # ── STEP 3: CLEAN ───────────────────────────────────────────────────────
    logger.info("[3/5] CLEANING")
    df_clean = clean_data(df_raw, config=CLEANING_CONFIG)

    # ── STEP 4: TRANSFORM ───────────────────────────────────────────────────
    logger.info("[4/5] TRANSFORMATION")
    df_transformed = transform_data(df_clean)

    # ── STEP 5: VALIDATE (after) ────────────────────────────────────────────
    after_report = validate_data(df_transformed)
    logger.info(f"  → {after_report.total_rows} rows | {after_report.duplicate_rows} dups "
                f"| {sum(after_report.null_counts.values())} nulls")

    # ── SAVE OUTPUT ─────────────────────────────────────────────────────────
    logger.info("[5/5] LOAD — Saving results")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_transformed.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"  → Clean dataset saved: {output_path}")

    # ── GENERATE REPORT ─────────────────────────────────────────────────────
    report_path.parent.mkdir(parents=True, exist_ok=True)
    generate_quality_report(
        before_report=before_report,
        after_report=after_report,
        report_path=report_path,
        version=PIPELINE_VERSION,
    )

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Pipeline completed successfully in {elapsed:.2f}s")
    logger.info(f"  Clean dataset : {output_path}")
    logger.info(f"  Quality report: {report_path}")
    logger.info("=" * 60)
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="DataPrep Pipeline",
        description="Automated data cleaning and transformation pipeline",
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=DEFAULT_INPUT_FILE,
        help=f"Input CSV file path (default: {DEFAULT_INPUT_FILE})",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help=f"Output CSV file path (default: {DEFAULT_OUTPUT_FILE})",
    )
    parser.add_argument(
        "--report", "-r",
        type=Path,
        default=DEFAULT_REPORT_FILE,
        help=f"HTML report output path (default: {DEFAULT_REPORT_FILE})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args  = parse_args()
    success = run_pipeline(
        input_path=args.input,
        output_path=args.output,
        report_path=args.report,
    )
    sys.exit(0 if success else 1)
