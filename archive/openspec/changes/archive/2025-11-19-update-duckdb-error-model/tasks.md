# Implementation Tasks: Align DuckDB Error Model

## 1. Spec Review and Deltas

- [x] 1.1 Review `openspec/specs/utils-duckdb/spec.md` for all error-related scenarios (read/write, dataset write, maintenance).
- [x] 1.2 Add MODIFIED requirements clarifying:
  - `FileNotFoundError` for missing parquet paths or empty datasets under a given root.
  - `ValueError` (or related path-type error) when a dataset path is an existing file.
  - Error behavior for invalid modes and thresholds (already partially specified).
- [x] 1.3 Run `openspec validate update-duckdb-error-model --strict` and fix any formatting/consistency issues.

## 2. Read/Write Error Modeling

- [x] 2.1 Update `DuckDBParquetHandler.read_parquet` to:
  - Use the handler's filesystem (when available) to check if the path exists before executing DuckDB queries.
  - Raise `FileNotFoundError` with a clear message when the path does not exist.
  - Avoid over-wrapping DuckDB errors; preserve original error type or wrap with preserved message.
- [x] 2.2 Update `DuckDBParquetHandler.write_parquet` to:
  - Detect when the parent directory path is an existing file and raise a clear error.
  - Preserve or clarify error handling for permission/authentication failures from `fsspec` or DuckDB.

## 3. Dataset Write Path Validation

- [x] 3.1 Update `DuckDBParquetHandler.write_parquet_dataset` to:
  - Detect when `path` is an existing regular file and raise `ValueError` (or a more precise built-in) with a clear message that dataset paths must be directories.
  - Keep existing `ValueError` checks for invalid `mode` and `max_rows_per_file` and align messages with spec wording.
- [x] 3.2 Add tests in `tests/test_utils/test_duckdb.py` to cover:
  - `write_parquet_dataset` on an existing file path (not directory).
  - Invalid modes and thresholds (confirm messages and types).

## 4. Maintenance Error Consistency

- [x] 4.1 Refine `_collect_dataset_stats` (or its successor if moved by other changes) to:
  - Use `FileNotFoundError` for non-existent dataset paths and "no parquet files found" cases.
  - Avoid wrapping these conditions in generic `Exception`.
- [x] 4.2 Update `compact_parquet_dataset` and `optimize_parquet_dataset` to:
  - Let `FileNotFoundError` propagate when there is nothing to operate on.
  - Avoid catching and re-wrapping these in ways that obscure the type.
- [x] 4.3 Add tests for maintenance error behavior in `tests/test_utils/test_duckdb.py`:
  - Compaction/optimization on non-existent paths.
  - Compaction/optimization when `partition_filter` excludes all files.

## 5. Validation

- [x] 5.1 Run `pytest tests/test_utils/test_duckdb.py -v` and ensure all pass.
- [x] 5.2 Run a subset of integration tests involving DuckDB to spot unexpected behavior changes.
- [x] 5.3 Run `openspec validate update-duckdb-error-model --strict`.

