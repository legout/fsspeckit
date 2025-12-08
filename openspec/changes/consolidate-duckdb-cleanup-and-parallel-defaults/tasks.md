## 1. Implementation

- [x] 1.1 Move `_unregister_duckdb_table_safely` into the canonical DuckDB helpers module
      (for example `fsspeckit.datasets.duckdb.helpers`) defined by the package-based layout.
      **Status**: Function already exists in canonical location (`fsspeckit.datasets.duckdb.helpers`).
- [x] 1.2 Update the DuckDB connection and dataset implementation modules under `fsspeckit.datasets.duckdb`
      to import and use the canonical helper (target the package modules, not the legacy shim files).
      **Status**:
      - Removed duplicate from `connection.py`
      - Added import in `connection.py` and `dataset.py`
      - Replaced all cleanup calls with canonical helper (3 locations in dataset.py)
- [x] 1.3 Review CSV/Parquet helpers for `use_threads` defaults and joblib usage:
  - [x] 1.3.1 Ensure base behaviour does not require joblib when `use_threads=False`.
      **Status**: Changed all defaults from `True` to `False` in 14 functions across 5 files.
  - [x] 1.3.2 Ensure that requesting parallel execution without joblib yields a clear `ImportError` with guidance.
      **Status**: Error handling already exists in `common/misc.py` with proper guidance.

## 2. Testing

- [x] 2.1 Add or extend tests for DuckDB cleanup helpers to verify:
  - [x] 2.1.1 Cleanup failures are logged once and do not interrupt other cleanup steps.
      **Status**: Added `TestUnregisterDuckDBTableSafely` class with 5 test methods covering successful unregistration, CatalogException logging, ConnectionException logging, and cleanup continuation.
  - [x] 2.1.2 All DuckDB modules use the same helper.
      **Status**: Added test `test_all_duckdb_modules_use_canonical_helper` verifying both connection.py and dataset.py import the canonical helper.
- [x] 2.2 Add or extend tests for CSV/Parquet read helpers to cover:
  - [x] 2.2.1 Behaviour with and without joblib installed.
      **Status**: Added `TestJoblibAvailability` class with tests for lazy import behavior and run_parallel error handling without joblib.
  - [x] 2.2.2 Behaviour with `use_threads=True` and `use_threads=False`.
      **Status**: Added test `test_all_helpers_use_threads_false_by_default` verifying all 11 helper functions default to `use_threads=False`.

## 3. Documentation

- [x] 3.1 Update performance/parallelism documentation to explain:
  - [x] 3.1.1 That joblib is only required for threaded execution.
      **Status**: Added "Parallel Execution Configuration" section in `docs/how-to/optimize-performance.md` with detailed explanation of serial vs parallel execution.
  - [x] 3.1.2 How to enable threaded execution via the appropriate extras.
      **Status**: Documented installation instructions using `pip install "fsspeckit[datasets]"` and best practices for optional parallel execution.
