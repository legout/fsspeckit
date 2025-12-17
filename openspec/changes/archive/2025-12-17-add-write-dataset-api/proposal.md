# Change: Add `write_dataset` API with file metadata results

## Why
Dataset writes currently spread across `write_parquet_dataset` variants and backends, with inconsistent behavior and no
reliable way to capture per-file metadata (paths, row counts, stats). This makes downstream operations (merge planning,
auditing, compaction) harder and contributes to the confusing UX.

## What Changes
- Introduce a single dataset write API: `write_dataset(..., mode="append"|"overwrite", ...)`.
- Define canonical result types for write outputs (per-file paths + row counts + basic stats).
- Implement metadata capture during writes:
  - PyArrow: `pyarrow.dataset.write_dataset(..., file_visitor=...)`
  - DuckDB: `COPY ... (RETURN_STATS TRUE)`

## Scope
This change covers **writes only** (append/overwrite). Merge operations are explicitly out of scope and will be added in
follow-up changes.

## Impact
- Adds new public APIs and result types (breaking change will happen later when removing legacy APIs).
- Touches both backends’ dataset write implementations and tests.

