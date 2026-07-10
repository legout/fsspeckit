# Change: Implement `merge(strategy=...)` for PyArrow datasets

## Why
The PyArrow handler currently merges by loading the full dataset in memory and often writes the merged result as *new*
files without removing old ones, producing duplicate/mixed datasets. “Incremental” rewrite is not incremental.

## What Changes
- Add `merge(data, path, strategy="insert"|"update"|"upsert", key_columns=..., ...)` to the PyArrow dataset IO.
- Implement incremental behavior using the shared merge core:
  - `insert`: append only new keys as new parquet file(s)
  - `update`: rewrite only affected files; no inserts
  - `upsert`: rewrite affected files + write inserted rows as new files
- Enforce invariants:
  - Full-row replacement for matching keys
  - Partition columns cannot change for existing keys
- Return per-file metadata for rewritten and newly written files.

## Scope
PyArrow backend only. DuckDB implementation will follow in a separate change.

