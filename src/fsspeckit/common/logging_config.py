"""Logging configuration utilities for fsspeckit using Python's standard logging module."""

import logging
import os
import sys
from typing import Optional

# Global registry to prevent duplicate configuration
_configured = False


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    include_timestamp: bool = True,
    enable_console: bool = True,
    enable_file: bool = False,
    file_path: Optional[str] = None
) -> None:
    """
    Configure logging for the fsspeckit package.

    This should be called once at application startup.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom log format string
        include_timestamp: Whether to include timestamp in logs
        enable_console: Whether to output to console
        enable_file: Whether to output to file
        file_path: Path for log file output
    """
    global _configured

    if _configured:
        return

    # Parse level from environment if not provided
    if not level:
        level = os.getenv('FSSPECKIT_LOG_LEVEL', 'INFO')

    # Set default format
    if not format_string:
        timestamp_part = "%(asctime)s - " if include_timestamp else ""
        format_string = f"{timestamp_part}%(name)s - %(levelname)s - %(message)s"

    # Configure root logger
    root_logger = logging.getLogger('fsspeckit')
    root_logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(format_string))
        root_logger.addHandler(console_handler)

    # File handler
    if enable_file and file_path:
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(logging.Formatter(format_string))
        root_logger.addHandler(file_handler)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """
    Get a properly configured logger for a module.

    Args:
        name: Module name (typically __name__)

    Returns:
        Configured logger instance
    """
    # Auto-configure if not already done
    if not _configured:
        setup_logging()

    return logging.getLogger(f"fsspeckit.{name}")
