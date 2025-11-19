"""SQL-to-filter utilities for fsspeckit.

This package contains SQL parsing and filter conversion utilities:
- SQL to PyArrow filter expression conversion
- SQL to Polars filter expression conversion
- SQL query parsing utilities
"""

from .filters import sql2pyarrow_filter, sql2polars_filter, get_table_names

__all__ = [
    "sql2pyarrow_filter",
    "sql2polars_filter",
    "get_table_names",
]