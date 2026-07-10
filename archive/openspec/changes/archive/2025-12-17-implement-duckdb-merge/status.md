# Status: ✅ IMPLEMENTED

## What Exists
- Incremental merge API: `DuckDBDatasetIO.merge(...)` in `src/fsspeckit/datasets/duckdb/dataset.py`.
- Merge semantics:
  - `insert`: appends new keys as new files only
  - `update`: rewrites only files containing matching keys (confirmed by key scan)
  - `upsert`: rewrites affected files + appends inserted keys as new files
- Invariants:
  - Full-row replacement for matched keys
  - NULL keys rejected
  - Partition column immutability enforced
- Per-file result metadata is returned via `MergeResult` / `MergeFileMetadata`.

## Notes
- The canonical behavior lives in `DuckDBDatasetIO.merge(...)`; legacy dataset-write surfaces are removed/disabled by `remove-legacy-dataset-write-ux`.

## Tests
- `tests/test_duckdb_merge.py` passes (includes file preservation, NULL key rejection, partition immutability, composite keys, and file metadata assertions).
