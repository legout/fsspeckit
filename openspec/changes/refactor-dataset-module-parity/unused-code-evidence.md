# Unused Code Evidence (Phase 4)

## Removal Candidates (with evidence)

### datasets/duckdb_cleanup_helpers.py
- `rg -n "_cleanup_duckdb_tables|_unregister_duckdb_table" src/fsspeckit -S` shows no imports or call sites.
- Superseded by `datasets/duckdb/helpers._unregister_duckdb_table_safely`.

### DuckDBDatasetIO legacy helpers
- `_merge_with_sql` (`datasets/duckdb/dataset.py:1031`): `rg -n "_merge_with_sql"` shows only definition.
- `_write_parquet_dataset_standard` (`datasets/duckdb/dataset.py:1184`): definition only.
- `_write_parquet_dataset_incremental` (`datasets/duckdb/dataset.py:1281`): definition only.
- `_perform_incremental_rewrite` (`datasets/duckdb/dataset.py:1508`): definition only.
- `_clear_dataset` (`datasets/duckdb/dataset.py:2046`): definition only.
- `_generate_unique_filename` (`datasets/duckdb/dataset.py:2026`): definition only.

### DuckDB legacy public methods
- `merge_parquet_dataset`, `insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset` all raise `NotImplementedError` and have no call sites outside docstring examples.

### PyArrow dataset helpers
- `_create_string_key_array` (`datasets/pyarrow/dataset.py:317`) is deprecated and unused.

### Base dataset helpers
- `_list_parquet_files`, `_get_file_row_count`, `_get_file_size`, `_extract_keys_from_table` in `datasets/base.py` show no call sites in `src/`.

### core/ext/dataset legacy write API
- `write_pyarrow_dataset`, `_write_pyarrow_dataset_standard`, `insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset` raise `NotImplementedError` and are not registered in `core/ext/register.py`. Only referenced in docs.
