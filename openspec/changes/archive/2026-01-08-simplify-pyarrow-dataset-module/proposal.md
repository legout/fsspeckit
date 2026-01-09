# Change: Simplify PyArrow Dataset Module

## Why
The `src/fsspeckit/datasets/pyarrow/` module contains significant technical debt, including:
- Legacy method stubs that raise `NotImplementedError` and confuse users.
- Redundant helper functions for key handling and merging.
- Merging logic scattered between `io.py` and `dataset.py`.
- Unused internal utilities.

Simplifying this module will improve maintainability, clarify the public API, and ensure that all PyArrow-based dataset operations use a consistent, high-performance execution path.

## What Changes
- **REMOVED**: Legacy stubs in `PyarrowDatasetIO` (`write_parquet_dataset`, `merge_parquet_dataset`, `insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset`).
- **REMOVED**: Obsolete `merge_parquet_dataset_pyarrow` function in `dataset.py`.
- **REMOVED**: Unused `_normalize_datetime_string` in `schema.py`.
- **REMOVED**: Redundant key handling helpers (`_create_string_key_array`, `_extract_key_tuples`, `_ensure_no_null_keys_dataset`).
- **REFACTORED**: Moved `_merge_upsert_pyarrow` and `_merge_update_pyarrow` from `io.py` to `dataset.py`.
- **REFACTORED**: Unified key handling into a single `_ensure_key_array` utility in `dataset.py`.
- **REFACTORED**: Standardized all key-based filtering on `AdaptiveKeyTracker`.

## Impact
- Affected specs: `utils-pyarrow`, `datasets-parquet-io`.
- Affected code: `src/fsspeckit/datasets/pyarrow/`.
- **BREAKING**: Removal of deprecated/stubbed methods in `PyarrowDatasetIO`. Users should use `merge()` for incremental operations and `write_dataset()` for append/overwrite.
