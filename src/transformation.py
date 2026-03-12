"""
src/transformation.py
---------------------
Data transformation module for DataPrep Pipeline.
Applies domain-aware transformations to produce analysis-ready data.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from src.logger import get_logger

logger = get_logger("transformation")


def parse_date_columns(df: pd.DataFrame, date_cols: list[str] | None = None) -> pd.DataFrame:
    """
    Parse date columns to datetime64 and extract year, month, day sub-columns.

    Args:
        df:        Input DataFrame.
        date_cols: List of column names to parse. If None, auto-detects columns
                   named 'fecha', 'date', 'fecha_venta', 'order_date', etc.
    Returns:
        DataFrame with parsed datetime columns and extracted components.
    """
    df = df.copy()
    if date_cols is None:
        # Auto-detect likely date column names
        candidates = [
            c for c in df.columns
            if any(k in c.lower() for k in ["fecha", "date", "time", "hora", "timestamp"])
        ]
    else:
        candidates = date_cols

    for col in candidates:
        if col not in df.columns:
            continue
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            null_dates = df[col].isnull().sum()
            if null_dates > 0:
                logger.warning(f"Column '{col}': {null_dates} dates could not be parsed")

            # Extract components
            df[f"{col}_year"]  = df[col].dt.year
            df[f"{col}_month"] = df[col].dt.month
            df[f"{col}_day"]   = df[col].dt.day
            logger.info(f"Parsed date column '{col}' → extracted year/month/day")
        except Exception as e:
            logger.warning(f"Could not parse date column '{col}': {e}")

    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add domain-specific derived columns.

    Rules applied:
    - If 'precio' and 'cantidad' exist → add 'total' = precio * cantidad
    - If 'total' exists → add 'total_con_iva' = total * 1.19 (IVA 19%)
    - If 'total' exists → add revenue category label
    """
    df = df.copy()

    if "precio" in df.columns and "cantidad" in df.columns:
        df["total"] = (df["precio"] * df["cantidad"]).round(2)
        logger.info("Derived column 'total' = precio × cantidad")

    if "total" in df.columns:
        df["total_con_iva"] = (df["total"] * 1.19).round(2)
        logger.info("Derived column 'total_con_iva' = total × 1.19")

        bins   = [0, 100, 500, 1000, float("inf")]
        labels = ["Bajo", "Medio", "Alto", "Premium"]
        df["categoria_venta"] = pd.cut(
            df["total"].clip(lower=0),
            bins=bins,
            labels=labels,
            right=False,
        )
        logger.info("Derived column 'categoria_venta' (Bajo/Medio/Alto/Premium)")

    return df


def normalize_numeric_columns(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    method: str = "minmax",
) -> pd.DataFrame:
    """
    Normalize numeric columns using min-max or z-score scaling.

    Args:
        df:      Input DataFrame.
        columns: Column names to normalize. If None, normalizes all numeric columns.
        method:  'minmax' (0-1) or 'zscore'.

    Returns:
        DataFrame with normalized values in new columns (original preserved).
        New columns are named `{col}_norm`.
    """
    df = df.copy()
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
        # Exclude year/month/day derived columns
        columns = [c for c in columns if not any(s in c for s in ["_year", "_month", "_day"])]

    for col in columns:
        if col not in df.columns:
            continue
        series = df[col].dropna()
        if method == "minmax":
            col_min, col_max = series.min(), series.max()
            if col_max != col_min:
                df[f"{col}_norm"] = ((df[col] - col_min) / (col_max - col_min)).round(4)
            else:
                df[f"{col}_norm"] = 0.0
        elif method == "zscore":
            mean, std = series.mean(), series.std()
            if std != 0:
                df[f"{col}_norm"] = ((df[col] - mean) / std).round(4)
            else:
                df[f"{col}_norm"] = 0.0
        logger.debug(f"Normalized '{col}' → '{col}_norm' ({method})")

    return df


def transform_data(df: pd.DataFrame, normalize: bool = False) -> pd.DataFrame:
    """
    Run the full transformation pipeline on a cleaned DataFrame.

    Steps:
    1. Parse date columns
    2. Add derived columns
    3. (Optional) Normalize numeric columns

    Args:
        df:        Cleaned DataFrame.
        normalize: If True, apply min-max normalization to numeric columns.

    Returns:
        Transformed DataFrame ready for analysis.
    """
    logger.info("Starting transformation pipeline...")

    df = parse_date_columns(df)
    df = add_derived_columns(df)

    if normalize:
        df = normalize_numeric_columns(df, method="minmax")

    logger.info(f"Transformation complete — {len(df.columns)} columns in output")
    return df
