"""
tests/test_transformation.py
-----------------------------
Unit tests for the transformation module.
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.transformation import (
    parse_date_columns,
    add_derived_columns,
    normalize_numeric_columns,
    transform_data,
)


@pytest.fixture
def base_df():
    return pd.DataFrame({
        "fecha":    ["2023-01-15", "2023-06-20", "2024-03-10"],
        "producto": ["Laptop", "Mouse", "Teclado"],
        "precio":   [999.99, 29.99, 59.99],
        "cantidad": [1, 5, 3],
    })


class TestParseDateColumns:
    def test_converts_to_datetime(self, base_df):
        result = parse_date_columns(base_df)
        assert pd.api.types.is_datetime64_any_dtype(result["fecha"])

    def test_extracts_year(self, base_df):
        result = parse_date_columns(base_df)
        assert "fecha_year" in result.columns
        assert result["fecha_year"].iloc[0] == 2023

    def test_extracts_month(self, base_df):
        result = parse_date_columns(base_df)
        assert "fecha_month" in result.columns
        assert result["fecha_month"].iloc[0] == 1

    def test_extracts_day(self, base_df):
        result = parse_date_columns(base_df)
        assert "fecha_day" in result.columns
        assert result["fecha_day"].iloc[0] == 15

    def test_handles_bad_date_gracefully(self):
        df = pd.DataFrame({"fecha": ["2023-01-15", "not-a-date", "2024-12-01"]})
        result = parse_date_columns(df, date_cols=["fecha"])
        assert pd.api.types.is_datetime64_any_dtype(result["fecha"])
        assert result["fecha"].isna().sum() == 1  # bad date becomes NaT


class TestAddDerivedColumns:
    def test_creates_total_column(self, base_df):
        result = add_derived_columns(base_df)
        assert "total" in result.columns

    def test_total_is_precio_times_cantidad(self, base_df):
        result = add_derived_columns(base_df)
        assert result["total"].iloc[0] == pytest.approx(999.99 * 1, abs=0.01)

    def test_creates_iva_column(self, base_df):
        result = add_derived_columns(base_df)
        assert "total_con_iva" in result.columns

    def test_total_con_iva_correct(self, base_df):
        result = add_derived_columns(base_df)
        expected = result["total"].iloc[0] * 1.19
        assert result["total_con_iva"].iloc[0] == pytest.approx(expected, abs=0.01)

    def test_creates_category_column(self, base_df):
        result = add_derived_columns(base_df)
        assert "categoria_venta" in result.columns


class TestNormalizeNumericColumns:
    def test_creates_norm_column(self, base_df):
        result = normalize_numeric_columns(base_df, columns=["precio"])
        assert "precio_norm" in result.columns

    def test_norm_values_between_0_and_1(self, base_df):
        result = normalize_numeric_columns(base_df, columns=["precio"])
        assert result["precio_norm"].between(0, 1).all()

    def test_zscore_normalization(self, base_df):
        result = normalize_numeric_columns(base_df, columns=["precio"], method="zscore")
        assert "precio_norm" in result.columns


class TestTransformData:
    def test_returns_dataframe(self, base_df):
        result = transform_data(base_df)
        assert isinstance(result, pd.DataFrame)

    def test_has_more_columns_than_input(self, base_df):
        result = transform_data(base_df)
        assert len(result.columns) > len(base_df.columns)

    def test_runs_without_error(self, base_df):
        # Should not raise
        transform_data(base_df, normalize=True)
