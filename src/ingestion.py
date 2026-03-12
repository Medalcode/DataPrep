"""
src/ingestion.py
----------------
Data ingestion module for DataPrep Pipeline.
Supports loading data from CSV, Excel, and JSON REST APIs.
"""
import pandas as pd
import requests
from pathlib import Path
from src.logger import get_logger

logger = get_logger("ingestion")


def load_csv(path: str | Path, encoding: str = "utf-8", **kwargs) -> pd.DataFrame:
    """
    Load a CSV file into a DataFrame.

    Args:
        path:     Path to the CSV file.
        encoding: File encoding (default: utf-8).
        **kwargs: Extra kwargs forwarded to pd.read_csv.

    Returns:
        pd.DataFrame with the raw data.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    logger.info(f"Loading CSV: {path}")
    df = pd.read_csv(path, encoding=encoding, **kwargs)
    logger.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns from {path.name}")
    return df


def load_excel(path: str | Path, sheet_name: int | str = 0, **kwargs) -> pd.DataFrame:
    """
    Load an Excel file into a DataFrame.

    Args:
        path:       Path to the .xlsx / .xls file.
        sheet_name: Sheet to read (default: first sheet).
        **kwargs:   Extra kwargs forwarded to pd.read_excel.

    Returns:
        pd.DataFrame with the raw data.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")

    logger.info(f"Loading Excel: {path} (sheet={sheet_name})")
    df = pd.read_excel(path, sheet_name=sheet_name, **kwargs)
    logger.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns from {path.name}")
    return df


def load_json_api(url: str, timeout: int = 10, record_path=None, **kwargs) -> pd.DataFrame:
    """
    Fetch JSON data from a REST API and parse it into a DataFrame.

    Args:
        url:         API endpoint URL.
        timeout:     Request timeout in seconds.
        record_path: Nested key to normalize (passed to pd.json_normalize).
        **kwargs:    Extra kwargs forwarded to pd.json_normalize.

    Returns:
        pd.DataFrame with the API response as rows.
    """
    logger.info(f"Fetching API data from: {url}")
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    data = response.json()

    if isinstance(data, list):
        df = pd.json_normalize(data, **kwargs)
    elif isinstance(data, dict) and record_path:
        df = pd.json_normalize(data, record_path=record_path, **kwargs)
    else:
        df = pd.json_normalize(data, **kwargs)

    logger.info(f"API returned {len(df):,} records")
    return df
