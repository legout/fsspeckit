# Proposal: Dataset Deduplication Maintenance API

## Why
`strategy="deduplicate"` inside `write_parquet_dataset` is useful for ingestion-time hygiene but is easy to misinterpret as “deduplicate an existing dataset in place”.

Users need a dedicated maintenance entry point that:
- Operates on an existing parquet dataset directory.
- Can be run independently of ingestion.
- Can be composed with `optimize_parquet_dataset` to produce a cleaner, compacted dataset.

## What Changes
- Add a dedicated maintenance method to dataset handlers:
  - `deduplicate_parquet_dataset(path, *, key_columns=None, dedup_order_by=None, partition_filter=None, compression=None, dry_run=False, verbose=False)`
  - If `key_columns` is provided: keep one row per key, selecting the preferred record by `dedup_order_by` (backend-specific implementation).
  - If `key_columns` is not provided: remove exact duplicate rows across all columns.
- Extend `optimize_parquet_dataset` with an optional deduplication step:
  - Add optional parameters (e.g. `deduplicate_key_columns`, `dedup_order_by`).
  - If provided, optimize SHALL perform deduplication as part of the rewrite pipeline.

## Impact
- New API surface; no behavior changes for existing calls unless the new method/parameters are used.
- Deduplication can be expensive on large datasets; the API should make the rewrite nature explicit and support `dry_run`.

## Out of Scope
- Changing merge strategy semantics in `write_parquet_dataset`.
- Adding new storage backends beyond DuckDB and PyArrow.

