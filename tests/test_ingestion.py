"""
tests/test_ingestion.py
-----------------------
Unit tests for the ingestion module.
"""
import pytest
import pandas as pd
from pathlib import Path
import tempfile
import os

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingestion import load_csv, load_excel


@pytest.fixture
def sample_csv(tmp_path):
    """Create a temporary CSV file for testing."""
    data = "id,nombre,precio\n1,Laptop,999.99\n2,Mouse,29.99\n3,Teclado,59.99"
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(data, encoding="utf-8")
    return csv_file


@pytest.fixture
def sample_excel(tmp_path):
    """Create a temporary Excel file for testing."""
    df = pd.DataFrame({
        "id":      [1, 2, 3],
        "nombre":  ["Laptop", "Mouse", "Teclado"],
        "precio":  [999.99, 29.99, 59.99],
    })
    excel_file = tmp_path / "test.xlsx"
    df.to_excel(excel_file, index=False)
    return excel_file


class TestLoadCSV:
    def test_loads_correct_shape(self, sample_csv):
        df = load_csv(sample_csv)
        assert df.shape == (3, 3)

    def test_returns_dataframe(self, sample_csv):
        df = load_csv(sample_csv)
        assert isinstance(df, pd.DataFrame)

    def test_correct_columns(self, sample_csv):
        df = load_csv(sample_csv)
        assert list(df.columns) == ["id", "nombre", "precio"]

    def test_raises_if_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_csv("non_existent_file.csv")

    def test_correct_values(self, sample_csv):
        df = load_csv(sample_csv)
        assert df["precio"].iloc[0] == pytest.approx(999.99)


class TestLoadExcel:
    def test_loads_correct_shape(self, sample_excel):
        df = load_excel(sample_excel)
        assert df.shape == (3, 3)

    def test_returns_dataframe(self, sample_excel):
        df = load_excel(sample_excel)
        assert isinstance(df, pd.DataFrame)

    def test_raises_if_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_excel("non_existent.xlsx")
