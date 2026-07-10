## 1. Cleanup
- [x] 1.1 Remove legacy stubs from `src/fsspeckit/datasets/pyarrow/io.py`
- [x] 1.2 Remove obsolete `merge_parquet_dataset_pyarrow` from `src/fsspeckit/datasets/pyarrow/dataset.py`
- [x] 1.3 Remove unused `_normalize_datetime_string` from `src/fsspeckit/datasets/pyarrow/schema.py`
- [x] 1.4 Remove redundant `_perform_merge_in_memory` and legacy `_write_parquet_dataset_incremental` from `io.py`

## 2. Refactor and Unification
- [x] 2.1 Move `_merge_upsert_pyarrow` and `_merge_update_pyarrow` to `dataset.py`
- [x] 2.2 Implement `_ensure_key_array` in `dataset.py` to unify key extraction
- [x] 2.3 Refactor `dataset.py` to remove redundant key helpers (`_create_string_key_array`, `_extract_key_tuples`)
- [x] 2.4 Update `PyarrowDatasetIO.merge` to use the refactored helpers in `dataset.py`

## 3. Verification
- [x] 3.1 Run `pytest tests/test_utils/test_pyarrow_dataset_handler.py`
- [x] 3.2 Run `pytest tests/test_pyarrow_merge.py` (if exists, or similar merge tests)
- [x] 3.3 Validate OpenSpec with `openspec validate simplify-pyarrow-dataset-module --strict`
