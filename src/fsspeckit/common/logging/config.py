"""Logging configuration utilities for fsspeckit."""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from loguru import Logger

if TYPE_CHECKING:
    pass


def setup_logging(
    level: str | None = None,
    disable: bool = False,
    format_string: str | None = None,
) -> None:
    """Configure the Loguru logger for fsspeckit.

    Removes the default handler and adds a new one targeting stderr
    with customizable level and format.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               If None, uses FSSPECKIT_LOG_LEVEL environment variable
               or defaults to "INFO".
        disable: Whether to disable logging for fsspeckit package.
        format_string: Custom format string for log messages.
                      If None, uses a default comprehensive format.

    Example:
        ```python
        # Basic setup
        setup_logging()

        # Custom level and format
        setup_logging(level="DEBUG", format_string="{time} | {level} | {message}")

        # Disable logging
        setup_logging(disable=True)
        ```
    """
    # Determine log level
    if level is None:
        level = os.getenv("FSSPECKIT_LOG_LEVEL", "INFO")

    # Default format if none provided
    if format_string is None:
        format_string = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        )

    # Remove the default handler added by Loguru
    logger.remove()

    # Add new handler with custom configuration
    logger.add(
        sys.stderr,
        level=level.upper(),
        format=format_string,
    )

    # Optionally disable logging for this package
    if disable:
        logger.disable("fsspeckit")


def get_logger(name: str = "fsspeckit") -> Logger:
    """Get a logger instance for the given name.

    Args:
        name: Logger name, typically the module name.

    Returns:
        Configured logger instance.

    Example:
        ```python
        logger = get_logger(__name__)
        logger.info("This is a log message")
        ```
    """
    return logger.bind(name=name)
