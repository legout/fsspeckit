# Status: ✅ IMPLEMENTED

## What Exists
- Backend-neutral incremental rewrite utilities in `src/fsspeckit/core/incremental.py`:
  - Dataset file listing: `list_dataset_files(...)` (local + fsspec)
  - Hive partition parsing: `parse_hive_partition_path(...)`
  - Parquet stats extraction: `ParquetMetadataAnalyzer` via `pyarrow.parquet.read_metadata`
  - Candidate pruning:
    - Partition pruning via explicit `partition_columns` + `source_partition_values`
    - Conservative stats pruning via min/max when available
  - Affected file confirmation by scanning only `key_columns`: `confirm_affected_files(...)`
  - Staging + replacement helpers: `IncrementalFileManager.create_staging_directory(...)`, `atomic_replace_files(...)`, `cleanup_staging_files(...)`
  - Invariants helpers:
    - `validate_no_null_keys(...)`
    - `validate_partition_column_immutability(...)`

## Notes
- Pruning is intentionally conservative: when metadata is missing or unreadable, files remain candidates and are confirmed via key scans.
