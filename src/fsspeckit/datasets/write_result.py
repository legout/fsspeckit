"""
Canonical result types for dataset write operations.

This module provides shared result dataclasses for dataset writes,
capturing per-file metadata that can be used for downstream operations
like merge planning, auditing, and compaction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FileWriteMetadata:
    """Metadata for a single written parquet file.

    Attributes:
        path: Absolute or relative path to the written file
        row_count: Number of rows written to this file
        size_bytes: Size of the file in bytes (optional)
        metadata: Additional backend-specific metadata (optional)
    """

    path: str
    row_count: int
    size_bytes: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.row_count < 0:
            raise ValueError("row_count must be >= 0")
        if self.size_bytes is not None and self.size_bytes < 0:
            raise ValueError("size_bytes must be >= 0")


@dataclass
class WriteDatasetResult:
    """Result of a write_dataset operation.

    This captures metadata about all files written during a dataset write,
    providing consistent information across both PyArrow and DuckDB backends.

    Attributes:
        files: List of metadata for each written file
        total_rows: Total number of rows written across all files
        mode: Write mode used ('append' or 'overwrite')
        backend: Backend that performed the write ('pyarrow' or 'duckdb')
    """

    files: list[FileWriteMetadata]
    total_rows: int
    mode: str
    backend: str

    def __post_init__(self) -> None:
        if self.total_rows < 0:
            raise ValueError("total_rows must be >= 0")
        if self.mode not in ("append", "overwrite"):
            raise ValueError(f"mode must be 'append' or 'overwrite', got: {self.mode}")
        if self.backend not in ("pyarrow", "duckdb"):
            raise ValueError(
                f"backend must be 'pyarrow' or 'duckdb', got: {self.backend}"
            )

        # Validate that sum of file row counts matches total
        computed_total = sum(f.row_count for f in self.files)
        if computed_total != self.total_rows:
            raise ValueError(
                f"Sum of file row counts ({computed_total}) does not match "
                f"total_rows ({self.total_rows})"
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format for serialization."""
        return {
            "files": [
                {
                    "path": f.path,
                    "row_count": f.row_count,
                    "size_bytes": f.size_bytes,
                    "metadata": f.metadata,
                }
                for f in self.files
            ],
            "total_rows": self.total_rows,
            "mode": self.mode,
            "backend": self.backend,
        }
