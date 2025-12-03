"""Polars utilities façade.

DEPRECATED: This module exists only for backwards compatibility.
New code should import from fsspeckit.common.polars.
"""

# Re-export from canonical location
from fsspeckit.common.polars import (
    opt_dtype as opt_dtype_pl,
    pl,
    explode_all,
    drop_null_columns,
)

__all__ = [
    "opt_dtype_pl",
    "pl",
    "explode_all",
    "drop_null_columns",
]
