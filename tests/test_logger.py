import pytest
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.logger import get_logger


class TestGetLogger:
    def test_returns_logger_instance(self):
        logger = get_logger("test")
        assert isinstance(logger, logging.Logger)

    def test_logger_name(self):
        logger = get_logger("test_name")
        assert logger.name == "test_name"

    def test_console_handler_added(self):
        logger = get_logger("console_test")
        handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(handlers) >= 1

    def test_file_handler_added_with_log_dir(self, tmp_path):
        logger = get_logger("file_test", log_dir=tmp_path)
        handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(handlers) >= 1

    def test_multiple_calls_return_same_logger(self):
        logger1 = get_logger("shared")
        logger2 = get_logger("shared")
        assert logger1 is logger2
