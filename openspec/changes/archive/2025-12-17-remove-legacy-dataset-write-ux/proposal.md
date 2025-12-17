# Change: Remove legacy `mode/strategy/rewrite_mode` dataset write UX and broken helpers

## Why
After introducing `write_dataset(mode=...)` and `merge(strategy=...)`, the old API surface becomes redundant and actively
harmful:
- it encodes confusing precedence rules (`strategy` ignores `mode`)
- it contains incorrect semantics (DuckDB `insert`/`update` can drop unaffected rows)
- it contains broken implementations (incremental stubs; PyArrow merge helper writing with invalid args)

Backward compatibility is not required.

## What Changes
- Remove or deprecate legacy APIs:
  - `write_parquet_dataset(..., mode, strategy, rewrite_mode, ...)`
  - incremental rewrite knobs (`rewrite_mode`)
- Remove or replace broken helpers:
  - function-based `merge_parquet_dataset_pyarrow` implementation that writes invalidly for modern PyArrow
  - any placeholder incremental merge stubs and debug prints
- Update docs and tests to use the new explicit APIs:
  - `write_dataset(mode=...)`
  - `merge(strategy=...)`

## Impact
This is the **breaking cleanup** step that makes the new UX the only supported one.

## Implementation Notes (Refactor)
This change is large and touches many call sites. To keep it reviewable and reduce churn, implement it in phases:
1) Migrate internal + test call sites to `write_dataset(...)` / `merge(...)` and delete legacy-only tests.
2) Update/replace the `DatasetHandler` protocol to match the new UX (drop `write_parquet_dataset`, `rewrite_mode`).
3) Remove legacy handler methods (`write_parquet_dataset`, convenience `*_dataset` wrappers) and legacy helpers (`merge_parquet_dataset_pyarrow`, `_perform_merge_in_memory`, etc).
