"""SQL utilities façade.

DEPRECATED: This module exists only for backwards compatibility.
New code should import from fsspeckit.sql.
"""

# Re-export from canonical location
from fsspeckit.sql import get_table_names

__all__ = [
    "get_table_names",
]
