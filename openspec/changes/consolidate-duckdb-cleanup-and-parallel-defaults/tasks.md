## 1. Implementation

- [ ] 1.1 Move `_unregister_duckdb_table_safely` into the canonical DuckDB helpers module
      (for example `fsspeckit.datasets.duckdb.helpers`) defined by the package-based layout.
- [ ] 1.2 Update the DuckDB connection and dataset implementation modules under `fsspeckit.datasets.duckdb`
      to import and use the canonical helper (target the package modules, not the legacy shim files).
- [ ] 1.3 Review CSV/Parquet helpers for `use_threads` defaults and joblib usage:
  - [ ] 1.3.1 Ensure base behaviour does not require joblib when `use_threads=False`.
  - [ ] 1.3.2 Ensure that requesting parallel execution without joblib yields a clear `ImportError` with guidance.

## 2. Testing

- [ ] 2.1 Add or extend tests for DuckDB cleanup helpers to verify:
  - [ ] 2.1.1 Cleanup failures are logged once and do not interrupt other cleanup steps.
  - [ ] 2.1.2 All DuckDB modules use the same helper.
- [ ] 2.2 Add or extend tests for CSV/Parquet read helpers to cover:
  - [ ] 2.2.1 Behaviour with and without joblib installed.
  - [ ] 2.2.2 Behaviour with `use_threads=True` and `use_threads=False`.

## 3. Documentation

- [ ] 3.1 Update performance/parallelism documentation to explain:
  - [ ] 3.1.1 That joblib is only required for threaded execution.
  - [ ] 3.1.2 How to enable threaded execution via the appropriate extras.
