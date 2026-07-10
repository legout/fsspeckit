# Status: ✅ IMPLEMENTED

## What Exists
- Incremental merge API: `PyarrowDatasetIO.merge(...)` in `src/fsspeckit/datasets/pyarrow/io.py`.
- Core utilities + result dataclasses in `src/fsspeckit/core/incremental.py`.
- Merge semantics:
  - `insert`: appends new keys as new files only
  - `update`: rewrites only files containing matching keys (confirmed by key scan)
  - `upsert`: rewrites affected files + appends inserted keys as new files
- Invariants:
  - Full-row replacement for matched keys
  - NULL keys rejected
  - Partition column immutability enforced
- Per-file result metadata is returned via `MergeResult` / `MergeFileMetadata`.

## Tests
- `tests/test_utils/test_pyarrow_merge_method.py` passes (includes file preservation + file metadata assertions).
