"""
tests/test_validation.py
------------------------
Unit tests for the validation module.
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.validation import validate_data, detect_mixed_type_columns, detect_outliers_iqr


@pytest.fixture
def dirty_df():
    """DataFrame with known quality issues."""
    return pd.DataFrame({
        "id":       [1, 2, 3, 4, 5, 2],
        "nombre":   ["Laptop", "Mouse", "Mouse", "Teclado", None, "Mouse"],
        "precio":   [999.99, 29.99, None, 59.99, 1500.0, 29.99],
        "cantidad": [1, 5, 3, None, 2, 5],
    })



@pytest.fixture
def clean_df():
    return pd.DataFrame({
        "id":       [1, 2, 3],
        "nombre":   ["Laptop", "Mouse", "Teclado"],
        "precio":   [999.99, 29.99, 59.99],
        "cantidad": [1, 5, 3],
    })


class TestValidateData:
    def test_returns_report(self, dirty_df):
        report = validate_data(dirty_df)
        assert report is not None

    def test_detects_duplicates(self, dirty_df):
        report = validate_data(dirty_df)
        assert report.duplicate_rows >= 1

    def test_detects_nulls(self, dirty_df):
        report = validate_data(dirty_df)
        assert report.null_counts["nombre"] >= 1
        assert report.null_counts["precio"] >= 1

    def test_correct_row_count(self, dirty_df):
        report = validate_data(dirty_df)
        assert report.total_rows == len(dirty_df)

    def test_correct_col_count(self, dirty_df):
        report = validate_data(dirty_df)
        assert report.total_cols == len(dirty_df.columns)

    def test_no_alerts_on_clean_data(self, clean_df):
        report = validate_data(clean_df, null_threshold_pct=5.0, duplicate_threshold_pct=1.0)
        assert report.duplicate_rows == 0
        assert sum(report.null_counts.values()) == 0

    def test_null_pct_calculated_correctly(self, dirty_df):
        report = validate_data(dirty_df)
        expected = round((dirty_df["nombre"].isnull().sum() / len(dirty_df)) * 100, 2)
        assert report.null_pcts["nombre"] == pytest.approx(expected, abs=0.01)


class TestDetectMixedTypeColumns:
    def test_detects_mixed(self):
        df = pd.DataFrame({"col": ["123", "abc", "456", "789"]})
        result = detect_mixed_type_columns(df)
        assert "col" in result

    def test_no_mixed_in_clean(self):
        df = pd.DataFrame({"col": ["abc", "def", "ghi"]})
        result = detect_mixed_type_columns(df)
        assert "col" not in result


class TestDetectOutliersIQR:
    def test_detects_outlier(self):
        s = pd.Series([1, 2, 2, 3, 2, 1, 2, 3, 1000])  # 1000 is an outlier
        n = detect_outliers_iqr(s)
        assert n >= 1

    def test_no_outliers_in_uniform(self):
        s = pd.Series([10, 11, 10, 12, 11, 10, 11, 12])
        n = detect_outliers_iqr(s)
        assert n == 0
