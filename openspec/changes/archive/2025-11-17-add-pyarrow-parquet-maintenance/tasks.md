## 1. Implementation

- [x] 1.1 Add a `collect_dataset_stats_pyarrow` helper to
        `src/fsspeckit/utils/pyarrow.py` that:
        - Uses fsspec to list files under a dataset path.
        - Optionally filters files by a list of partition prefixes.
        - Computes `size_bytes` from file metadata and `num_rows` using
          `pyarrow.parquet.ParquetFile` metadata or a minimal table scan.
        - Returns a dict with keys `files`, `total_bytes`, `total_rows`.
- [x] 1.2 Add `compact_parquet_dataset_pyarrow(path, target_mb_per_file=None,
        target_rows_per_file=None, partition_filter=None, compression=None,
        dry_run=False, filesystem=None)` that:
        - Validates thresholds (`> 0`) and raises on invalid inputs.
        - Computes compaction groups based on file sizes and/or row counts.
        - In dry-run mode, returns `before_file_count`, `after_file_count`,
          `before_total_bytes`, `after_total_bytes`, `compacted_file_count`,
          `rewritten_bytes`, `compression_codec`, and `planned_groups` without
          reading or writing any data.
        - In live mode, reads each group’s files incrementally, concatenates
          them into a new table, writes new `compact-*.parquet` files, and then
          deletes the originals via fsspec.
- [x] 1.3 Add `optimize_parquet_dataset_pyarrow(path, zorder_columns,
        target_mb_per_file=None, target_rows_per_file=None,
        partition_filter=None, compression=None, dry_run=False,
        filesystem=None)` that:
        - Validates that all `zorder_columns` exist in the dataset schema.
        - In dry-run mode, reuses the grouping logic from compaction and returns
          a plan with the same statistics plus the configured `zorder_columns`.
        - In live mode, reads data in a streaming fashion, sorts rows by
          `zorder_columns` (with NULLs last) for the scope of each group, and
          writes `optimized-*.parquet` files while deleting original group
          files.
- [x] 1.4 Ensure both helpers avoid naive full-dataset materialization:
        - Use per-file or per-group reads (e.g. `pq.read_table` on individual
          files) instead of `dataset.to_table()` on the entire dataset.
        - Use partition filtering where possible to minimize the number of files
          and row groups scanned.

## 2. Testing

- [x] 2.1 Add tests to `tests/test_utils/test_utils_pyarrow.py` (or a new
        `test_pyarrow_dataset_maintenance.py`) to cover:
        - Compaction reduces file count while preserving row count and schema.
        - Compaction with both `target_mb_per_file` and
          `target_rows_per_file`.
        - Dry-run behavior for compaction and optimization (no files changed,
          valid plan structure).
        - Optimization rewrites data so that files are ordered by
          `zorder_columns` within each output file.
        - Partition filters restrict the scope of maintenance to matching
          prefixes.
- [x] 2.2 Add tests that simulate larger datasets by many small files and
        verify that:
        - Maintenance operations complete successfully.
        - Peak memory usage stays bounded by group sizes (implicitly tested by
          building many-file scenarios).

## 3. Documentation

- [x] 3.1 Extend `docs/utils.md` with a “PyArrow Parquet Maintenance” section:
        - Describe compaction and optimization use cases.
        - Show examples of dry-run and live execution.
        - Call out memory characteristics (group-based processing).
- [x] 3.2 Optionally add small example scripts under `examples/pyarrow/` to
        mirror the DuckDB maintenance examples but using PyArrow.

- [x] 4.1 Run targeted tests:
        `pytest tests/test_utils/test_utils_pyarrow.py -k \"compact or optimize\"`.
- [x] 4.2 Run `ruff check src/fsspeckit/utils/pyarrow.py`.
- [x] 4.3 Run `mypy --ignore-missing-imports src/fsspeckit/utils/pyarrow.py`.
- [x] 4.4 Validate OpenSpec:
        `openspec validate add-pyarrow-parquet-maintenance --strict`.
