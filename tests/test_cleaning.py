"""
tests/test_cleaning.py
----------------------
Unit tests for the cleaning module.
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.cleaning import clean_data, normalize_column_names, impute_numeric, impute_categorical


@pytest.fixture
def dirty_df():
    # Row 4 is an exact duplicate of row 1 (same values in all columns)
    return pd.DataFrame({
        "ID Venta":  [1, 2, 3, 4, 2],
        "Producto":  ["Laptop", "Mouse", "Teclado", "Auriculares", "Mouse"],
        "Precio":    [999.99, 29.99, 59.99, None, 29.99],
        "Cantidad":  [1, 5, 3, None, 5],
    })



class TestNormalizeColumnNames:
    def test_strips_spaces(self):
        df = pd.DataFrame(columns=["Nombre ", " Precio"])
        df = normalize_column_names(df)
        assert "nombre" in df.columns
        assert "precio" in df.columns

    def test_replaces_spaces_with_underscore(self):
        df = pd.DataFrame(columns=["ID Venta", "Fecha Registro"])
        df = normalize_column_names(df)
        assert "id_venta" in df.columns
        assert "fecha_registro" in df.columns

    def test_lowercases(self):
        df = pd.DataFrame(columns=["NOMBRE", "PRECIO"])
        df = normalize_column_names(df)
        assert "nombre" in df.columns
        assert "precio" in df.columns


class TestImputeNumeric:
    def test_fills_nulls_with_mean(self):
        df = pd.DataFrame({"precio": [10.0, 20.0, None, 30.0]})
        result = impute_numeric(df, strategy="mean")
        assert result["precio"].isnull().sum() == 0
        assert result["precio"].iloc[2] == pytest.approx(20.0)

    def test_fills_nulls_with_median(self):
        df = pd.DataFrame({"precio": [10.0, 20.0, None, 30.0]})
        result = impute_numeric(df, strategy="median")
        assert result["precio"].isnull().sum() == 0

    def test_fills_nulls_with_zero(self):
        df = pd.DataFrame({"precio": [10.0, None]})
        result = impute_numeric(df, strategy="zero")
        assert result["precio"].iloc[1] == 0.0


class TestImputeCategorical:
    def test_fills_with_mode(self):
        df = pd.DataFrame({"cat": ["A", "A", "B", None]})
        result = impute_categorical(df, strategy="mode")
        assert result["cat"].isnull().sum() == 0
        assert result["cat"].iloc[3] == "A"  # mode

    def test_fills_with_unknown(self):
        df = pd.DataFrame({"cat": ["A", "B", None]})
        result = impute_categorical(df, strategy="unknown")
        assert result["cat"].iloc[2] == "Unknown"


class TestCleanData:
    def test_removes_duplicates(self, dirty_df):
        result = clean_data(dirty_df)
        assert result.duplicated().sum() == 0

    def test_no_nulls_after_cleaning(self, dirty_df):
        result = clean_data(dirty_df)
        assert result.isnull().sum().sum() == 0

    def test_strips_whitespace(self, dirty_df):
        result = clean_data(dirty_df)
        str_cols = result.select_dtypes(include="object").columns
        for col in str_cols:
            for val in result[col].dropna():
                assert val == val.strip()

    def test_returns_dataframe(self, dirty_df):
        result = clean_data(dirty_df)
        assert isinstance(result, pd.DataFrame)

    def test_row_count_reduced(self, dirty_df):
        result = clean_data(dirty_df)
        # Row 4 is an exact duplicate of row 2 (same ID, product, price, quantity)
        assert len(result) < len(dirty_df)
