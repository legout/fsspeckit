# Change Proposal: Refactor Parquet Maintenance to Backend-Neutral Layer

## Why

Parquet dataset maintenance is currently implemented twice:

- DuckDB-backed maintenance in `DuckDBParquetHandler.compact_parquet_dataset` and `DuckDBParquetHandler.optimize_parquet_dataset`.
- PyArrow-backed maintenance in `compact_parquet_dataset_pyarrow` and `optimize_parquet_dataset_pyarrow` within `utils.pyarrow`.

Both sets of helpers aim to satisfy similar requirements:

- Discover parquet files recursively under a dataset path with optional `partition_filter`.
- Compute file-level statistics (path, size_bytes, num_rows, totals).
- Group files into compaction/optimization groups based on target size and/or row thresholds.
- Rewrite groups into fewer files with consistent compression, optionally reordering rows by clustering/z-order columns.
- Support dry-run plans and structured statistics objects.

However:

- DuckDB and PyArrow implementations carry very similar discovery and grouping logic in two places (`_collect_dataset_stats` vs `collect_dataset_stats_pyarrow`, separate group-builders for compaction and optimize).
- `optimize_parquet_dataset_pyarrow` and `DuckDBParquetHandler.optimize_parquet_dataset` both materialize the full filtered dataset into a single `pa.Table` before sorting and chunking, which conflicts with the “streaming / avoid full materialization” guidance in the PyArrow maintenance spec and is not ideal for large datasets.
- Stats structures are similar but not guaranteed to be identical across backends, which complicates documentation and testing.
- Adding a third backend (or new variants of maintenance operations) would require re-implementing the same planning logic again.

We want a single, backend-neutral maintenance layer that handles discovery, statistics, and grouping, while letting DuckDB and PyArrow focus on the execution (reading/writing groups and applying ordering).

## What Changes

- Introduce a backend-neutral maintenance module under `src/fsspeckit/core/` (e.g. `core/maintenance.py`) that:
  - Implements dataset discovery and file-level statistics once (reusing the existing PyArrow `collect_dataset_stats_pyarrow` behavior as the canonical version).
  - Provides a shared grouping algorithm for compaction (`target_mb_per_file`, `target_rows_per_file`) that returns groups of file descriptors.
  - Provides a shared grouping algorithm / plan for optimization that:
    - Validates presence of `zorder_columns`.
    - Returns groupings and intended ordering behavior without forcing full dataset materialization.
  - Produces a consistent statistics object for both compaction and optimization (before/after counts, total bytes, compacted_file_count, rewritten_bytes, compression_codec, dry_run, optional `planned_groups`).
- Refactor DuckDB-backed maintenance:
  - `DuckDBParquetHandler.compact_parquet_dataset`:
    - Delegate dataset discovery and grouping to the backend-neutral maintenance module.
    - Focus on executing group rewrites via DuckDB (e.g. using `parquet_scan` or `read_parquet` and `write_parquet`) rather than re-implementing grouping logic.
    - Ensure dry-run behavior and stats match the shared maintenance spec.
  - `DuckDBParquetHandler.optimize_parquet_dataset`:
    - Use the backend-neutral optimization planner to determine which files to rewrite and approximate grouping.
    - Avoid full materialization into a single Arrow table where possible; instead, process per group, ordering rows by the provided clustering columns.
- Refactor PyArrow-backed maintenance:
  - `collect_dataset_stats_pyarrow` becomes the authoritative implementation of dataset stats; DuckDB code paths that need stats can call into this core (via the new maintenance module).
  - `compact_parquet_dataset_pyarrow`:
    - Reuse the shared grouping plan, then execute compaction per group using PyArrow and `fsspec`.
    - Maintain the existing “group streaming” behavior and stats, but via shared logic.
  - `optimize_parquet_dataset_pyarrow`:
    - Replace the “read all files → concat → sort → split” approach with a plan that operates per group and avoids full dataset materialization where feasible.
    - Use the shared stats structure.
- Update `openspec/specs/utils-duckdb/spec.md` and `openspec/specs/utils-pyarrow/spec.md` with MODIFIED requirements that:
  - Emphasize the streaming constraints for both compaction and optimization.
  - Clarify which statistics are guaranteed and their semantics.
  - Note that grouping and statistics are shared across backends, while the execution engine (DuckDB vs PyArrow) may differ.

## Impact

- **Affected specs**
  - `utils-duckdb` (compaction / optimization requirements and stats).
  - `utils-pyarrow` (compaction / optimization requirements and streaming constraints).
- **Affected code**
  - New: `src/fsspeckit/core/maintenance.py` (or similar) for shared stats and grouping.
  - Modified: `src/fsspeckit/utils/duckdb.py` (`_collect_dataset_stats`, `compact_parquet_dataset`, `optimize_parquet_dataset`).
  - Modified: `src/fsspeckit/utils/pyarrow.py` (`collect_dataset_stats_pyarrow`, `compact_parquet_dataset_pyarrow`, `optimize_parquet_dataset_pyarrow`).
  - Modified tests: `tests/test_utils/test_duckdb.py` (maintenance sections) and `tests/test_utils/test_utils_pyarrow.py` (maintenance tests).
- **Behavioral impact**
  - External method signatures and high-level capabilities remain, but:
    - Stats objects will follow a single canonical structure across backends.
    - Memory characteristics for optimize operations improve (or are at least explicitly documented).
    - Dry-run behavior and grouping become easier to reason about and test.
- **Risk / rollout**
  - Medium: changes touch non-trivial, performance-sensitive code paths.
  - Mitigated by strong existing tests for both DuckDB and PyArrow maintenance helpers.

