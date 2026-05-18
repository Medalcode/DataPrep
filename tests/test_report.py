import pytest
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.validation import validate_data
from src.report import generate_quality_report


class TestGenerateQualityReport:
    def test_creates_html_file(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2], "nombre": ["A", "B"], "precio": [10.0, 20.0]})
        before = validate_data(df)
        after = validate_data(df)
        report_path = tmp_path / "report.html"
        result = generate_quality_report(before, after, report_path)
        assert result.exists()
        assert result.suffix == ".html"

    def test_html_contains_expected_content(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2], "nombre": ["A", None], "precio": [10.0, 20.0]})
        before = validate_data(df)
        after = validate_data(df)
        report_path = tmp_path / "report.html"
        generate_quality_report(before, after, report_path)
        html = report_path.read_text(encoding="utf-8")
        assert "DataPrep" in html
        assert "Pipeline" in html

    def test_empty_report_does_not_crash(self, tmp_path):
        df = pd.DataFrame()
        before = validate_data(df)
        after = validate_data(df)
        report_path = tmp_path / "report.html"
        result = generate_quality_report(before, after, report_path)
        assert result.exists()
