## 1. Specs
- [x] Update `datasets-parquet-io` delta for DuckDB `merge(strategy=...)`:
  - composite key support
  - conservative pruning + key-scan confirmation
  - per-file metadata collection policy (parquet metadata preferred; RETURN_STATS optional)
  - no parallel “incremental merge” implementation paths

## 2. DuckDB implementation
- [x] Add `merge(...) -> MergeResult` method to `DuckDBDatasetIO` with parity to PyArrow API (strategy, key_columns, partition_columns).
- [x] Validate invariants using shared core helpers:
  - reject NULL keys in source
  - enforce partition column immutability for existing keys (no moves)
- [x] Discover target parquet files via `list_dataset_files(..., filesystem=fs)` and handle empty target semantics:
  - `update`: error if no target dataset
  - `insert`/`upsert`: initial write as inserted files only
- [x] Implement file-level planning + confirmation via shared core:
  - `plan_incremental_rewrite(...)` (partition pruning when partition_columns provided)
  - `confirm_affected_files(...)` (read only `key_columns`)
- [x] Implement per-strategy behavior (incremental, file-level):
  - `insert`: compute anti-join (source keys not in target); write only inserted rows as new file(s); preserve all existing files
  - `update`: compute matched keys; rewrite only confirmed affected files; do not write inserts
  - `upsert`: rewrite confirmed affected files + write inserted rows as new file(s)
- [x] Support composite keys and per-file source subsets (implementation may use PyArrow or DuckDB SQL).
- [x] Write rewritten files into a staging dir under the dataset root and replace only affected files via `IncrementalFileManager.atomic_replace_files(...)`.
- [x] Return `MergeResult` with accurate file lists and `MergeFileMetadata` (row_count/size where possible).
- [x] Keep a single canonical incremental path (`DuckDBDatasetIO.merge(...)`).

Optional follow-ups (performance / metadata depth):
- [x] **DEFERRED** Use DuckDB SQL to compute matched/inserted key sets and per-file subsets (avoid materializing full files in Python where possible).
- [x] **DEFERRED** Collect richer per-file metadata for newly written files via `duckdb.parquet_metadata(...)` (or `COPY ... RETURN_STATS`).

**Note:** These are performance optimization enhancements deferred for future optimization cycles. Core merge functionality is complete and tested.

## 3. Tests
- [x] Align/extend `tests/test_duckdb_merge.py` to match the PyArrow merge contract:
  - composite keys (multi-column key_columns) scenarios
  - NULL key rejection
  - partition immutability rejection
- [x] Strengthen file preservation assertions (multi-file dataset, only one file rewritten when keys are isolated).
- [x] Assert returned `MergeResult` includes `files` entries for rewritten + inserted files and that `*_files` lists are consistent.
