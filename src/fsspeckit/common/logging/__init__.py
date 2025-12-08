"""Common logging utilities for fsspeckit.

This package contains focused submodules for logging functionality:
- config: Logging configuration and logger utilities

All public APIs are re-exported here for convenient access.
"""

from .config import (
    setup_logging,
    get_logger,
)

__all__ = [
    "setup_logging",
    "get_logger",
]
