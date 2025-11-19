# Design: Backend-Neutral Parquet Maintenance Layer

## Context

Parquet maintenance in `fsspeckit` currently spans:

- DuckDB:
  - `DuckDBParquetHandler._collect_dataset_stats`
  - `DuckDBParquetHandler.compact_parquet_dataset`
  - `DuckDBParquetHandler.optimize_parquet_dataset`
- PyArrow:
  - `collect_dataset_stats_pyarrow`
  - `compact_parquet_dataset_pyarrow`
  - `optimize_parquet_dataset_pyarrow`

Both code paths:

- Walk a directory tree (via `fsspec`) and discover `.parquet` files.
- Compute per-file stats (size, row count).
- Decide which files are “candidates” and group them into compaction/optimization groups according to thresholds.
- Rewrite groups into new files and delete originals.
- Return stats objects with before/after file counts, bytes, and rewrite counts.

The logic for stats and grouping is highly similar across DuckDB and PyArrow. Only the *execution* (how groups are read, ordered, and written) should differ between backends.

## Goals

- Implement dataset discovery, statistics, and grouping once and share across backends.
- Provide a clear, canonical stats structure for both compaction and optimization.
- Preserve streaming constraints from the specs (no full-dataset materialization) as much as possible.
- Keep backend-specific code focused on IO and ordering rather than planning.

## Non-Goals

- Introduce a generic query planner or cost model for maintenance.
- Change public method signatures or high-level capabilities (compaction / optimization remain the same from a user perspective).
- Implement advanced maintenance operations beyond compaction and z-order-ish optimization.

## High-Level Approach

Introduce `src/fsspeckit/core/maintenance.py` with three core responsibilities:

1. **Dataset discovery and statistics**
   - A single helper (e.g. `collect_dataset_stats`) that:
     - Uses `fsspec` to recursively discover `.parquet` files under a root path.
     - Applies `partition_filter` by matching relative paths to prefix strings.
     - Computes `size_bytes` and `num_rows` per file (using PyArrow’s parquet metadata).
     - Returns a list of file descriptors plus total bytes and rows.
   - This helper can be backed by the current `collect_dataset_stats_pyarrow` implementation, but moved into a neutral module.

2. **Compaction grouping**
   - A helper (e.g. `plan_compaction_groups`) that:
     - Accepts file descriptors and thresholds (`target_mb_per_file`, `target_rows_per_file`).
     - Returns:
       - Groups of files to compact together (only multi-file groups).
       - A set of “untouched” files (large ones and singletons).
       - A `planned_stats` object with keys:
         - `before_file_count`, `after_file_count`,
         - `before_total_bytes`, `after_total_bytes`,
         - `compacted_file_count`, `rewritten_bytes`,
         - `compression_codec`, `dry_run`,
         - `planned_groups` (list of lists of paths).
   - The grouping algorithm can closely resemble the current implementations:
     - Sort candidate files by size.
     - Fill a current group until adding another file would exceed thresholds, then flush.

3. **Optimization planning**
   - A helper (e.g. `plan_optimize_groups`) that:
     - Accepts file descriptors, `zorder_columns`, thresholds, and a sample schema.
     - Validates that all `zorder_columns` exist in the schema.
     - For dry-run, returns groupings and stats similar to compaction.
     - For live runs, may simply return “all files” as a single group or more granular groups based on thresholds.
   - Backend code is responsible for how rows are ordered within each group (DuckDB SQL vs PyArrow table sort).

Backends then:

- Call into this maintenance core to obtain a plan and baseline stats.
- Execute the plan group-by-group using their preferred IO/compute engine.
- Recalculate final stats via the same helper or a simple follow-up stats call.

## API Sketch

```python
# core/maintenance.py (sketch – names tentative)

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from fsspec import AbstractFileSystem


@dataclass
class FileInfo:
    path: str
    size_bytes: int
    num_rows: int


def collect_dataset_stats(
    path: str,
    filesystem: AbstractFileSystem,
    partition_filter: list[str] | None = None,
) -> dict[str, Any]:
    """Return canonical stats and a list of FileInfo."""
    ...


def plan_compaction_groups(
    files: list[FileInfo],
    target_mb_per_file: int | None,
    target_rows_per_file: int | None,
) -> dict[str, Any]:
    """Return compaction groups and dry-run style stats."""
    ...


def plan_optimize_groups(
    files: list[FileInfo],
    zorder_columns: list[str],
    sample_schema: Any,  # PyArrow schema, but kept loose at core boundary
    target_mb_per_file: int | None,
    target_rows_per_file: int | None,
) -> dict[str, Any]:
    """Return optimization group plan and stats metadata."""
    ...
```

Backends can build on these helpers for both dry-run and live operations.

## DuckDB Integration Sketch

### Compaction

1. `DuckDBParquetHandler.compact_parquet_dataset`:
   - Obtain an `AbstractFileSystem` from the handler.
   - Call `collect_dataset_stats` to get `files` and baseline stats.
   - Call `plan_compaction_groups` for grouping and dry-run stats.
   - If `dry_run=True` or no groups, return the stats as-is.
   - Otherwise, for each group:
     - Use DuckDB `parquet_scan` or `read_parquet` to read group files into a relation or Arrow table.
     - Write a new `compact-*.parquet` file via `write_parquet`.
     - Delete original group files via `fsspec`.
   - Re-run `collect_dataset_stats` to compute final stats for the path.

### Optimization

1. `DuckDBParquetHandler.optimize_parquet_dataset`:
   - Use `collect_dataset_stats` to discover files.
   - Read a small sample table (e.g. first file) to obtain the schema.
   - Call `plan_optimize_groups` to validate `zorder_columns` and compute a plan.
   - For dry-run: return the plan.
   - For live runs:
     - For each group, use DuckDB SQL to:
       - `SELECT * FROM parquet_scan([...]) ORDER BY <zorder_columns>` (with NULL-last ordering).
       - Write the ordered result into `optimized-*.parquet` files, chunked by row or size thresholds.
     - Delete original input files in the group.
   - Recompute stats with `collect_dataset_stats`.

This keeps grouping and stats in one place and leaves the SQL ordering and IO to the DuckDB backend.

## PyArrow Integration Sketch

### Compaction

1. `compact_parquet_dataset_pyarrow`:
   - Use `collect_dataset_stats` from the core maintenance layer.
   - Use `plan_compaction_groups` for grouping and dry-run stats.
   - For dry-run, return stats/plan directly.
   - For live runs:
     - For each group, stream files via `fsspec` and `pq.read_table`, concat tables, and write new `compact-*.parquet` files.
     - Delete original group files.
   - Recompute stats via `collect_dataset_stats`.

### Optimization

1. `optimize_parquet_dataset_pyarrow`:
   - Use `collect_dataset_stats` and `plan_optimize_groups`.
   - For dry-run, return the plan.
   - For live runs:
     - For each group, stream files into memory one group at a time.
     - Concatenate and sort each group’s table by `zorder_columns`.
     - Write `optimized-*.parquet` files for that group, then delete originals.
   - This avoids “read all files → sort → split” and respects the streaming intent at the group level.

## Stats Structure

Both DuckDB and PyArrow backends will share a consistent stats structure, e.g.:

```python
{
    "before_file_count": int,
    "after_file_count": int,
    "before_total_bytes": int,
    "after_total_bytes": int,
    "compacted_file_count": int,
    "rewritten_bytes": int,
    "compression_codec": str | None,
    "dry_run": bool,
    # Optional, for planning:
    "zorder_columns": list[str] | None,
    "planned_groups": list[list[str]] | None,
}
```

Specs and tests will reference this canonical shape so both backends are held to the same expectations.

## Edge Cases

The shared maintenance layer should define behavior for:

- No parquet files under path (with or without partition filters) → `FileNotFoundError` with a consistent message.
- Thresholds ≤ 0 → `ValueError` with clear wording.
- Datasets that already meet thresholds → no groups, no rewrites, stats indicating zero `compacted_file_count`.

These behaviors will be reflected in the updated specs and verified via tests for both backends.

## Open Questions

- How aggressively should optimization avoid full dataset materialization?
  - Group-level streaming is a good compromise, but some very large groups may still be expensive.
- Should we surface the maintenance core as public API (e.g. for other backends) or keep it internal?
- Do we want to support more advanced grouping strategies (e.g. based on statistics such as column cardinality) in the future?

These can be revisited after the initial backend-neutral layer is in place and validated.

