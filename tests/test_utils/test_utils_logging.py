"""Tests for logging utility functions."""

import pytest
import tempfile
import os
from unittest.mock import patch
from pathlib import Path

from fsspeckit.common.logging import setup_logging, get_logger


class TestSetupLogging:
    """Test setup_logging function."""

    def test_basic_setup(self):
        """Test basic logging setup."""
        setup_logging(level="INFO", disable=False)

        # Test passes if no exception is raised

    def test_disable_logging(self):
        """Test disabling logging."""
        setup_logging(level="INFO", disable=True)

        # Test passes if no exception is raised

    def test_invalid_log_level(self):
        """Test invalid log level handling."""
        # Loguru will accept any string as a level, so this should not raise
        setup_logging(level="INVALID_LEVEL", disable=False)

        # Test passes if no exception is raised

    def test_custom_format(self):
        """Test custom format string."""
        custom_format = "{time} | {level} | {message}"
        setup_logging(level="INFO", disable=False, format_string=custom_format)

        # Test passes if no exception is raised

    def test_environment_variable(self):
        """Test environment variable usage."""
        os.environ["fsspeckit_LOG_LEVEL"] = "DEBUG"

        try:
            setup_logging(disable=False)
            # Should use DEBUG from environment
        finally:
            os.environ.pop("fsspeckit_LOG_LEVEL", None)

    def test_multiple_calls(self):
        """Test multiple calls to setup_logging."""
        # First setup
        setup_logging(level="INFO", disable=False)

        # Second setup with different level
        setup_logging(level="DEBUG", disable=False)

        # Test passes if no exception is raised


class TestGetLogger:
    """Test get_logger function."""

    def test_get_logger_default(self):
        """Test getting logger with default name."""
        logger = get_logger()
        # Loguru logger should be returned
        assert logger is not None

    def test_get_logger_custom_name(self):
        """Test getting logger with custom name."""
        logger = get_logger("custom_logger")
        assert logger is not None

    def test_get_logger_multiple_calls(self):
        """Test multiple calls to get_logger."""
        logger1 = get_logger("test_logger")
        logger2 = get_logger("test_logger")
        # Should return same logger instance (though Loguru works differently)
        assert logger1 is not None
        assert logger2 is not None


class TestLoggingIntegration:
    """Test logging integration scenarios."""

    def test_logging_context_manager(self):
        """Test logging within context manager."""
        setup_logging(level="INFO", disable=False)

        # Test logging from different modules
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")

        # Should not raise exceptions
        logger1.info("Message from module1")
        logger2.info("Message from module2")

    def test_logging_with_exception(self):
        """Test logging with exception information."""
        setup_logging(level="INFO", disable=False)

        logger = get_logger(__name__)

        try:
            raise ValueError("Test exception")
        except ValueError:
            # Should not raise exception
            logger.exception("An error occurred")

    @patch("loguru.logger.add")
    def test_loguru_integration(self, mock_add):
        """Test Loguru integration."""
        setup_logging(level="INFO", disable=False)
        mock_add.assert_called()

    def test_log_filtering(self):
        """Test log filtering."""
        setup_logging(level="WARNING", disable=False)

        logger = get_logger(__name__)
        # These should not raise exceptions even if filtered
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
