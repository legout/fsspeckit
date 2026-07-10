# Change: Add merge-aware write support to DuckDB dataset writes

## Why
DuckDB already supports merge strategies via `merge_parquet_dataset`, but `write_parquet_dataset` cannot apply those
strategies directly when writing from in-memory data. Users must stage data separately and then call the merge helper,
hurting ergonomics and parity with the planned PyArrow path.

## What Changes
- Extend `write_parquet_dataset` in `DuckDBDatasetIO` to accept optional `strategy` and `key_columns`; when provided,
  delegate to the existing merge machinery instead of a plain write.
- Add convenience helpers (`insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset`) on
  `DuckDBDatasetIO` and re-export via `DuckDBParquetHandler`.
- Preserve current behaviour when `strategy` is omitted; signatures remain backward compatible.

## Impact
- UX improvement: one-step merge-aware writes with DuckDB.
- Public API surface grows with a small set of convenience helpers.
- No change to default behaviour without `strategy`.
