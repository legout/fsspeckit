# Change Proposal: Align DuckDB Handler Error Model with Spec

## Why

The `utils-duckdb` spec describes clear error behavior for common failure modes:

- `FileNotFoundError` when reading non-existent parquet paths (files or datasets).
- `ValueError` for invalid modes and thresholds (e.g. `mode` not in `{"overwrite", "append"}`, `max_rows_per_file <= 0`).
- Clear errors when the dataset path is a file instead of a directory for dataset operations.
- Clear surface of underlying authentication/permission failures for remote filesystems.

The current `DuckDBParquetHandler` implementation only partially matches this:

- `read_parquet` delegates to DuckDB and wraps any failure in a generic `Exception("Failed to read parquet from ...")`, obscuring the underlying `FileNotFoundError` vs SQL error vs permission issue.
- `write_parquet_dataset` assumes `path` is a directory and calls `makedirs` without explicitly handling “path is an existing file” as a separate, clear error case.
- Maintenance helpers use a mixture of `FileNotFoundError`, `Exception`, and bare `print` statements for warnings, making it harder for callers to catch specific error types.

This makes error handling less predictable and deviates from the scenarios described in the spec. We want the DuckDB handler to surface precise, consistent exception types so callers can reliably distinguish “missing dataset”, “bad parameters”, and “backend failure”.

## What Changes

- Update `DuckDBParquetHandler.read_parquet` to:
  - Use the configured filesystem (when available) to pre-check existence for local and fsspec-backed paths, raising `FileNotFoundError` with a clear message when the path does not exist.
  - When DuckDB raises errors for existing paths (e.g. invalid parquet, SQL syntax), avoid re-wrapping as a bare `Exception` and either:
    - propagate the original DuckDB error type, or
    - wrap it in a domain-specific error while preserving the root cause in the message.
- Update `DuckDBParquetHandler.write_parquet` and `write_parquet_dataset` to:
  - Detect when the given `path` (for datasets) or parent directory (for single files) is an existing regular file and raise a `ValueError` (or `IsADirectoryError` / `NotADirectoryError` where appropriate) with a clear message, instead of letting `fsspec.makedirs` or DuckDB fail with less explicit errors.
  - Preserve existing `ValueError` behavior for invalid compression or modes and ensure messages match the spec.
- Update dataset maintenance helpers (`_collect_dataset_stats`, `compact_parquet_dataset`, `optimize_parquet_dataset`) to:
  - Use `FileNotFoundError` consistently when the root dataset path does not exist or when no parquet files match a given `partition_filter`.
  - Avoid masking these as generic `Exception` in outer layers.
- Clarify in the `utils-duckdb` spec that:
  - Read operations use `FileNotFoundError` for missing paths.
  - Dataset writes raise a clear error when `path` is an existing file.
  - Maintenance operations raise `FileNotFoundError` when there is nothing to operate on.

## Impact

- **Affected specs**
  - `utils-duckdb` (error handling sections for read, write, dataset write, and maintenance).
- **Affected code**
  - Modified: `src/fsspeckit/utils/duckdb.py` (`read_parquet`, `write_parquet`, `write_parquet_dataset`, maintenance helpers).
  - Tests: `tests/test_utils/test_duckdb.py` (add/adjust tests around missing paths, file-vs-directory errors, and maintenance error cases).
- **Behavioral impact**
  - Method signatures remain unchanged.
  - The types and messages of raised exceptions will become more specific and aligned with the spec; callers relying on generic `Exception` may now wish to catch `FileNotFoundError` or `ValueError` instead.
- **Risk / rollout**
  - Low to medium: more precise exceptions are usually easier to handle but may surface previously masked error conditions in tests or calling code.

