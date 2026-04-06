"""
src/cleaning.py
---------------
Data cleaning module for DataPrep Pipeline.
Applies a configurable set of automatic cleaning rules to a DataFrame.
"""
from __future__ import annotations

import re
import warnings
import pandas as pd
import numpy as np
from typing import Any, Dict

from src.logger import get_logger

logger = get_logger("cleaning")

DEFAULT_CONFIG: Dict[str, Any] = {
    "numeric_imputation": "mean",       # "mean" | "median" | "zero"
    "categorical_imputation": "mode",    # "mode" | "unknown"
    "drop_all_null_rows": True,
    "drop_duplicates": True,
    "strip_strings": True,
    "lowercase_strings": False,
    "normalize_column_names": True,
}


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names:
    - Strip whitespace
    - Lowercase
    - Replace spaces and special characters with underscores
    """
    df = df.copy()
    df.columns = [
        re.sub(r"[^\w]", "_", col.strip().lower()).strip("_")
        for col in df.columns
    ]
    return df


def impute_numeric(df: pd.DataFrame, strategy: str = "mean") -> pd.DataFrame:
    """Fill null values in numeric columns."""
    df = df.copy()
    num_cols = df.select_dtypes(include=[np.number]).columns
    for col in num_cols:
        if df[col].isnull().any():
            if strategy == "mean":
                fill_val = df[col].mean()
            elif strategy == "median":
                fill_val = df[col].median()
            else:
                fill_val = 0
            df[col] = df[col].fillna(round(fill_val, 4))
            logger.debug(f"Imputed numeric null in '{col}' with {strategy}={fill_val:.4f}")
    return df


def _is_date_column(col_name: str) -> bool:
    """Check if a column looks like a date (handled by transform step)."""
    return any(k in col_name.lower() for k in ["fecha", "date", "time", "hora", "timestamp"])


def _cat_cols(df: pd.DataFrame) -> list:
    """Get categorical/string column names, compatible with pandas 2 and 3."""
    cols = []
    for col in df.columns:
        dtype = df[col].dtype
        if isinstance(dtype, pd.CategoricalDtype):
            cols.append(col)
        elif isinstance(dtype, pd.StringDtype):
            cols.append(col)
        elif dtype == "object":
            cols.append(col)
    return cols


def impute_categorical(df: pd.DataFrame, strategy: str = "mode") -> pd.DataFrame:
    """Fill null values in categorical/object columns."""
    df = df.copy()
    cat_cols = [c for c in _cat_cols(df) if not _is_date_column(c)]
    for col in cat_cols:
        if df[col].isnull().any():
            if strategy == "mode" and not df[col].mode().empty:
                fill_val = df[col].mode()[0]
            else:
                fill_val = "Unknown"
            df[col] = df[col].fillna(fill_val)
            logger.debug(f"Imputed categorical null in '{col}' with '{fill_val}'")
    return df


def clean_data(
    df: pd.DataFrame,
    config: Dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Apply all cleaning rules to a DataFrame based on the provided config.

    Steps:
    1. Normalize column names
    2. Drop rows where ALL values are null
    3. Strip whitespace from string columns
    4. Optionally lowercase string columns
    5. Drop exact duplicate rows
    6. Impute numeric nulls
    7. Impute categorical nulls

    Args:
        df:     Raw input DataFrame.
        config: Cleaning parameters dict. Falls back to DEFAULT_CONFIG if None.

    Returns:
        Cleaned pd.DataFrame.
    """
    if config is None:
        config = DEFAULT_CONFIG

    df = df.copy()
    rows_before = len(df)
    logger.info(f"Starting cleaning — {rows_before:,} rows")

    # 1. Normalize column names
    if config.get("normalize_column_names", True):
        df = normalize_column_names(df)
        logger.info("Column names normalized")

    # 2. Drop rows where ALL values are null
    if config.get("drop_all_null_rows", True):
        df = df.dropna(how="all")
        logger.info(f"Dropped all-null rows → {len(df):,} rows remaining")

    # 3. Strip whitespace from strings
    # NOTE: We use a value-level map instead of col.str.strip() to avoid
    # silently converting non-string values (e.g. ints in object columns)
    # to NaN. Only actual str instances are stripped; all other types are
    # left untouched.
    if config.get("strip_strings", True):
        obj_cols = _cat_cols(df)
        df[obj_cols] = df[obj_cols].apply(
            lambda col: col.map(lambda v: v.strip() if isinstance(v, str) else v)
        )
        logger.debug(f"Stripped whitespace from {len(obj_cols)} string columns")

    # 4. Lowercase string columns
    # Same rationale: apply lower() only to actual str values.
    if config.get("lowercase_strings", False):
        obj_cols = _cat_cols(df)
        df[obj_cols] = df[obj_cols].apply(
            lambda col: col.map(lambda v: v.lower() if isinstance(v, str) else v)
        )

    # 5. Drop duplicates
    if config.get("drop_duplicates", True):
        before = len(df)
        df = df.drop_duplicates()
        dropped = before - len(df)
        logger.info(f"Dropped {dropped} duplicate rows → {len(df):,} rows remaining")

    # 6. Impute numeric nulls
    df = impute_numeric(df, strategy=config.get("numeric_imputation", "mean"))

    # 7. Impute categorical nulls
    df = impute_categorical(df, strategy=config.get("categorical_imputation", "mode"))

    rows_after = len(df)
    logger.info(
        f"Cleaning complete — {rows_before:,} → {rows_after:,} rows "
        f"({rows_before - rows_after} removed)"
    )
    return df
