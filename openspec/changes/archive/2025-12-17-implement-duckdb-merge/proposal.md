# Change: Implement `merge(strategy=...)` for DuckDB datasets (incremental, file-level)

## Why
DuckDB’s merge-aware write currently computes strategy queries correctly but then replaces the dataset with the query
result, which breaks `insert` and `update` semantics by dropping unaffected rows. The incremental path is incomplete.

## What Changes
- Add `merge(data, path, strategy="insert"|"update"|"upsert", key_columns=..., ...)` to the DuckDB dataset IO.
- Implement incremental behavior using the shared merge core (`src/fsspeckit/core/incremental.py`):
  - prune candidate files via partition parsing + parquet footer stats (conservative)
  - confirm affected files by scanning only `key_columns`
  - rewrite only affected files for `update`/`upsert`
  - append inserted rows as new files for `insert`/`upsert`
- Perform per-file rewrites without full dataset rewrites:
  - compute matched vs inserted keys (composite keys supported)
  - rewrite only files that are actually affected
- Use fsspec-safe staging + replacement:
  - staging directory created under the dataset root via `IncrementalFileManager.create_staging_directory(..., filesystem=fs)`
  - replace only rewritten files via `IncrementalFileManager.atomic_replace_files(...)`
- Return `MergeResult` / `MergeFileMetadata` including `rewritten_files`, `inserted_files`, `preserved_files`.
- Collect per-file metadata for written files (at minimum `row_count` and `size_bytes`; richer parquet stats are optional).
- Consolidate incremental merge implementations so there is only one canonical path (`DuckDBDatasetIO.merge(...)`).

## Scope
DuckDB backend only. Legacy APIs are removed in a follow-up cleanup change.
