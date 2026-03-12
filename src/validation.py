"""
src/validation.py
-----------------
Data validation module for DataPrep Pipeline.
Detects data quality issues and produces a structured report.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Dict

from src.logger import get_logger

logger = get_logger("validation")


@dataclass
class DataQualityReport:
    """Structured summary of data quality metrics."""
    total_rows: int = 0
    total_cols: int = 0
    duplicate_rows: int = 0
    duplicate_pct: float = 0.0

    # Per-column stats
    null_counts: Dict[str, int]   = field(default_factory=dict)
    null_pcts: Dict[str, float]   = field(default_factory=dict)
    dtypes: Dict[str, str]        = field(default_factory=dict)
    outlier_counts: Dict[str, int] = field(default_factory=dict)
    columns_with_mixed_types: list = field(default_factory=list)

    # Alert flags
    alerts: list = field(default_factory=list)

    def summary_dict(self) -> dict:
        """Return a flat dict suitable for display."""
        return {
            "total_rows": self.total_rows,
            "total_cols": self.total_cols,
            "duplicate_rows": self.duplicate_rows,
            "duplicate_pct": round(self.duplicate_pct, 2),
            "columns_with_nulls": sum(1 for v in self.null_counts.values() if v > 0),
            "total_nulls": sum(self.null_counts.values()),
            "alerts": len(self.alerts),
        }


def detect_mixed_type_columns(df: pd.DataFrame) -> list[str]:
    """
    Identify columns that contain mixed data types (e.g., numbers stored as strings).
    """
    mixed = []
    for col in df.select_dtypes(include="object").columns:
        sample = df[col].dropna()
        numeric_count = pd.to_numeric(sample, errors="coerce").notna().sum()
        if 0 < numeric_count < len(sample):
            mixed.append(col)
    return mixed


def detect_outliers_iqr(series: pd.Series, iqr_factor: float = 1.5) -> int:
    """
    Count outliers in a numeric series using the IQR method.
    """
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - iqr_factor * iqr
    upper = q3 + iqr_factor * iqr
    return int(((series < lower) | (series > upper)).sum())


def validate_data(
    df: pd.DataFrame,
    null_threshold_pct: float = 20.0,
    duplicate_threshold_pct: float = 5.0,
    iqr_factor: float = 1.5,
) -> DataQualityReport:
    """
    Run data quality checks on a DataFrame.

    Args:
        df:                      Input DataFrame to validate.
        null_threshold_pct:      Alert if null % for any column exceeds this.
        duplicate_threshold_pct: Alert if duplicate % exceeds this.
        iqr_factor:              Multiplier for IQR-based outlier detection.

    Returns:
        DataQualityReport with all metrics.
    """
    logger.info("Running data validation...")
    report = DataQualityReport()

    report.total_rows = len(df)
    report.total_cols = len(df.columns)

    # ── Duplicates ────────────────────────────────────────────────────────────
    report.duplicate_rows = int(df.duplicated().sum())
    report.duplicate_pct  = (report.duplicate_rows / max(report.total_rows, 1)) * 100
    if report.duplicate_pct > duplicate_threshold_pct:
        report.alerts.append(
            f"⚠ High duplicates: {report.duplicate_rows} rows ({report.duplicate_pct:.1f}%)"
        )

    # ── Null values ───────────────────────────────────────────────────────────
    null_series = df.isnull().sum()
    for col in df.columns:
        count = int(null_series[col])
        pct   = (count / max(report.total_rows, 1)) * 100
        report.null_counts[col] = count
        report.null_pcts[col]   = round(pct, 2)
        report.dtypes[col]      = str(df[col].dtype)
        if pct > null_threshold_pct:
            report.alerts.append(f"⚠ High nulls in '{col}': {count} ({pct:.1f}%)")

    # ── Outliers ──────────────────────────────────────────────────────────────
    for col in df.select_dtypes(include=[np.number]).columns:
        n_out = detect_outliers_iqr(df[col].dropna(), iqr_factor)
        if n_out > 0:
            report.outlier_counts[col] = n_out

    # ── Mixed types ───────────────────────────────────────────────────────────
    report.columns_with_mixed_types = detect_mixed_type_columns(df)
    for col in report.columns_with_mixed_types:
        report.alerts.append(f"⚠ Mixed data types in column '{col}'")

    logger.info(
        f"Validation complete — rows: {report.total_rows}, "
        f"nulls: {sum(report.null_counts.values())}, "
        f"duplicates: {report.duplicate_rows}, "
        f"alerts: {len(report.alerts)}"
    )
    return report
