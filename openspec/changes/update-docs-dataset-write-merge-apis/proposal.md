# Change: Update documentation to use current write_dataset() and merge() APIs

## Why
Documentation currently references APIs that no longer exist in the codebase:
- `write_parquet_dataset(..., strategy=...)` - raises NotImplementedError
- `insert_dataset()`, `upsert_dataset()`, `update_dataset()` - raise NotImplementedError
- `deduplicate_dataset()` - raises NotImplementedError
- `merge_parquet_dataset()` - raises NotImplementedError

All documentation must be updated to reflect the current API surface:
- `write_dataset(mode='append'|'overwrite')` for simple writes returning `WriteDatasetResult`
- `merge(strategy='insert'|'update'|'upsert', key_columns=...)` for merge operations returning `MergeResult`

This is pre-release documentation cleanup with no backward compatibility concerns.

## What Changes
- Rewrite `docs/how-to/read-and-write-datasets.md` to use `write_dataset()` and `merge()`
- Rewrite `docs/how-to/merge-datasets.md` completely for new `merge()` API
- Fix `docs/tutorials/getting-started.md` PyArrow example section
- Update `docs/reference/api-guide.md` capability descriptions
- Document `WriteDatasetResult` and `MergeResult` return types
- Update `docs/explanation/concepts.md` code examples
- Ensure all code examples work with current codebase

## Impact
- Affected docs:
  - `docs/how-to/read-and-write-datasets.md`
  - `docs/how-to/merge-datasets.md`
  - `docs/tutorials/getting-started.md`
  - `docs/reference/api-guide.md`
  - `docs/explanation/concepts.md`
- Affected specs: `project-docs`
