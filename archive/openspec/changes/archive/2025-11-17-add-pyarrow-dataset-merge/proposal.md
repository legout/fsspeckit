# Change Proposal: Add PyArrow-Based Parquet Dataset Merge

## Why

`DuckDBParquetHandler.merge_parquet_dataset` provides rich merge semantics for parquet
datasets (UPSERT/INSERT/UPDATE/FULL_MERGE/DEDUPLICATE). However, some users run
in environments where introducing DuckDB is undesirable or impossible, and they
already rely heavily on PyArrow datasets and compute.

We want a pure-PyArrow, fsspec-aware merge helper that:

- Implements the same high-level strategies and statistics as the DuckDB helper.
- Operates directly on parquet datasets (directory of files on an fsspec
  filesystem).
- Avoids full in-memory materialization of large datasets by using
  `pyarrow.dataset` scanners and `pyarrow.compute` filters to read only the
  relevant subsets of the target into memory.

This will:

- Make merge behavior available even when DuckDB is not installed or allowed.
- Improve portability of merge-focused workflows (e.g. running inside Arrow-only
  engines or constrained environments).
- Keep merge semantics aligned across DuckDB and PyArrow backends.

## What Changes

- Add a `merge_parquet_dataset_pyarrow` helper under the `utils-pyarrow`
  capability (implemented in `src/fsspeckit/utils/pyarrow.py`).
- Support the following merge strategies with the same behavior as
  `DuckDBParquetHandler.merge_parquet_dataset`:
  - `upsert`
  - `insert`
  - `update`
  - `full_merge`
  - `deduplicate` (deduplicate source before performing an UPSERT-style merge)
- Accept the following inputs:
  - `source`: PyArrow `Table` or path to a parquet dataset.
  - `target_path`: directory path for the target parquet dataset on an fsspec
    filesystem.
  - `key_columns`: str or list[str] identifying the merge keys.
  - `strategy`, `dedup_order_by`, `compression`, and optional fsspec filesystem.
- Implement merge execution using:
  - `pyarrow.dataset.Dataset` + `Scanner` to read the target dataset.
  - `pyarrow.compute` to build key-based filters such as
    `pc.field("id").is_in(batch_keys)` so that only matching rows are loaded.
  - A batch-oriented algorithm that never calls unfiltered `dataset.to_table()`
    on the full target.
- Return merge statistics identical in shape to the DuckDB helper:
  `{"inserted": int, "updated": int, "deleted": int, "total": int}`.

## Impact

- New change affects the `utils-pyarrow` capability only.
- New helper is additive; no breaking changes to existing PyArrow utilities.
- New tests will be added under `tests/test_utils` to validate merge behavior.
- Documentation will be extended in `docs/utils.md` to describe PyArrow merge
  usage and its memory characteristics.

