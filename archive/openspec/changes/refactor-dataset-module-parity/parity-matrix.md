# Dataset Module Parity Matrix (Baseline)

## Public API Inventory

### fsspeckit.datasets exports (`src/fsspeckit/datasets/__init__.py`)
- `PyarrowDatasetIO`
- `cast_schema`, `convert_large_types_to_normal`, `opt_dtype_pa`, `unify_schemas_pa`
- `collect_dataset_stats_pyarrow`, `compact_parquet_dataset_pyarrow`, `optimize_parquet_dataset_pyarrow`
- Path + error helpers: `normalize_path`, `validate_dataset_path`, `Dataset*Error` classes
- DuckDB handlers are exposed only via deprecated imports (`__getattr__`).

### PyArrow module exports (`src/fsspeckit/datasets/pyarrow/__init__.py`)
- `PyarrowDatasetIO`
- Dataset ops: `collect_dataset_stats_pyarrow`, `compact_parquet_dataset_pyarrow`, `optimize_parquet_dataset_pyarrow`
- Dataset creation: `pyarrow_dataset`, `pyarrow_parquet_dataset`
- Schema utils + memory tracking types

### DuckDB module exports (`src/fsspeckit/datasets/duckdb/__init__.py`)
- `DuckDBConnection`, `create_duckdb_connection`
- `DuckDBDatasetIO`
- Dataset ops: `collect_dataset_stats_duckdb`, `compact_parquet_dataset_duckdb`

### Class-based handler signatures

#### PyarrowDatasetIO (`src/fsspeckit/datasets/pyarrow/io.py`)
- `read_parquet(path: str, columns: list[str] | None = None, filters: Any | None = None, use_threads: bool = True) -> pa.Table`
- `write_parquet(data: pa.Table | list[pa.Table], path: str, compression: str | None = "snappy", row_group_size: int | None = None) -> None`
- `write_dataset(data: pa.Table | list[pa.Table], path: str, *, mode: Literal["append","overwrite"] = "append", basename_template: str | None = None, schema: pa.Schema | None = None, partition_by: str | list[str] | None = None, compression: str | None = "snappy", max_rows_per_file: int | None = 5_000_000, row_group_size: int | None = 500_000) -> WriteDatasetResult`
- `merge(data: pa.Table | list[pa.Table], path: str, strategy: Literal["insert","update","upsert"], key_columns: list[str] | str, *, partition_columns: list[str] | str | None = None, schema: pa.Schema | None = None, compression: str | None = "snappy", max_rows_per_file: int | None = 5_000_000, row_group_size: int | None = 500_000, merge_chunk_size_rows: int = 100_000, enable_streaming_merge: bool = True, merge_max_memory_mb: int = 1024, merge_max_process_memory_mb: int | None = None, merge_min_system_available_mb: int = 512, merge_progress_callback: Callable[[int, int], None] | None = None) -> MergeResult`
- `compact_parquet_dataset(path: str, target_mb_per_file: int | None = None, target_rows_per_file: int | None = None, partition_filter: list[str] | None = None, compression: str | None = None, dry_run: bool = False, verbose: bool = False) -> dict[str, Any]`
- `optimize_parquet_dataset(path: str, target_mb_per_file: int | None = None, target_rows_per_file: int | None = None, partition_filter: list[str] | None = None, compression: str | None = None, verbose: bool = False) -> dict[str, Any]`

#### DuckDBDatasetIO (`src/fsspeckit/datasets/duckdb/dataset.py`)
- `read_parquet(path: str, columns: list[str] | None = None, filters: str | None = None, use_threads: bool = True) -> pa.Table`
- `write_parquet(data: pa.Table | list[pa.Table], path: str, compression: str | None = "snappy", row_group_size: int | None = None, use_threads: bool = False) -> None`
- `write_dataset(data: pa.Table | list[pa.Table], path: str, *, mode: Literal["append","overwrite"] = "append", compression: str | None = "snappy", max_rows_per_file: int | None = 5_000_000, row_group_size: int | None = 500_000) -> WriteDatasetResult`
- `merge(data: pa.Table | list[pa.Table], path: str, strategy: Literal["insert","update","upsert"], key_columns: list[str] | str, *, partition_columns: list[str] | str | None = None, schema: pa.Schema | None = None, compression: str | None = "snappy", max_rows_per_file: int | None = 5_000_000, row_group_size: int | None = 500_000, use_merge: bool | None = None) -> MergeResult`
- `compact_parquet_dataset(path: str, target_mb_per_file: int | None = None, target_rows_per_file: int | None = None, partition_filter: list[str] | None = None, compression: str | None = None, dry_run: bool = False, verbose: bool = False) -> dict[str, Any]`
- `optimize_parquet_dataset(path: str, target_mb_per_file: int | None = None, target_rows_per_file: int | None = None, partition_filter: list[str] | None = None, compression: str | None = None, deduplicate_key_columns: list[str] | str | None = None, dedup_order_by: list[str] | str | None = None, verbose: bool = False) -> dict[str, Any]`

### Module-level dataset operations

#### PyArrow (`src/fsspeckit/datasets/pyarrow/dataset.py`)
- `collect_dataset_stats_pyarrow(path: str, filesystem: AbstractFileSystem | None = None, partition_filter: list[str] | None = None) -> dict[str, Any]`
- `compact_parquet_dataset_pyarrow(path: str, target_mb_per_file: int | None = None, target_rows_per_file: int | None = None, partition_filter: list[str] | None = None, compression: str | None = None, dry_run: bool = False, filesystem: AbstractFileSystem | None = None) -> dict[str, Any]`
- `optimize_parquet_dataset_pyarrow(path: str, target_mb_per_file: int | None = None, target_rows_per_file: int | None = None, partition_filter: list[str] | None = None, compression: str | None = None, deduplicate_key_columns: list[str] | str | None = None, dedup_order_by: list[str] | str | None = None, filesystem: AbstractFileSystem | None = None, verbose: bool = False) -> dict[str, Any]`
- `deduplicate_parquet_dataset_pyarrow(path: str, *, key_columns: list[str] | str | None = None, dedup_order_by: list[str] | str | None = None, partition_filter: list[str] | None = None, compression: str | None = None, dry_run: bool = False, filesystem: AbstractFileSystem | None = None, verbose: bool = False, chunk_size_rows: int = 1_000_000, max_memory_mb: int = 2048, enable_progress: bool = True, progress_callback: Callable[[int, int], None] | None = None) -> dict[str, Any]`

#### DuckDB (`src/fsspeckit/datasets/duckdb/dataset.py`)
- `collect_dataset_stats_duckdb(path: str, filesystem: AbstractFileSystem | None = None, partition_filter: list[str] | None = None) -> dict[str, Any]`
- `compact_parquet_dataset_duckdb(path: str, target_mb_per_file: int | None = None, target_rows_per_file: int | None = None, partition_filter: list[str] | None = None, compression: str | None = None, dry_run: bool = False, filesystem: AbstractFileSystem | None = None) -> dict[str, Any]`
- `DuckDBDatasetIO.deduplicate_parquet_dataset(...) -> dict[str, Any]` (method only; no module-level export)

## Parity Matrix (Core Operations)

| Operation | PyArrow handler | DuckDB handler | Parity gaps |
| --- | --- | --- | --- |
| `read_parquet` | Filters accept PyArrow expression/DNF/SQL strings (no normalization call in method) | Filters accept SQL string only | Filter type mismatch; PyArrow `filters` not normalized in `read_parquet` (`_normalize_filters` unused) |
| `write_parquet` | `compression`, `row_group_size` | `compression`, `row_group_size`, `use_threads` | Extra `use_threads` in DuckDB; thread param ignored in PyArrow |
| `write_dataset` | Supports `basename_template`, `schema`, `partition_by` | No `basename_template`, `schema`, `partition_by` | Parameter surface mismatch; partitioning only in PyArrow |
| `merge` | Streaming controls (`merge_chunk_size_rows`, `enable_streaming_merge`, memory knobs) | `use_merge` (DuckDB MERGE vs UNION ALL) | Extra parameters differ by backend |
| `compact_parquet_dataset` | Same signature | Same signature | Parity OK at method level |
| `optimize_parquet_dataset` | No dedup params | `deduplicate_key_columns`, `dedup_order_by` | DuckDB exposes dedup controls; PyArrow hides them in class API |

## Behavioral Differences / Defaults
- `DatasetHandler` protocol defines `max_rows_per_file`/`row_group_size` defaults as `None`, but both handlers default to `5_000_000` and `500_000`.
- PyArrow `read_parquet` advertises SQL-like filters but does not call `_normalize_filters` (unused method). DuckDB requires SQL WHERE clause strings.
- DuckDB `optimize_parquet_dataset` optionally deduplicates first; PyArrow class method does not expose dedup controls (only module-level function does).
- PyArrow `write_dataset` uses `pyarrow.dataset.write_dataset` with `existing_data_behavior="overwrite_or_ignore"`; DuckDB uses staging + atomic move and preserves non-parquet files.
- PyArrow `merge` returns `MergeResult` with populated `metrics`; DuckDB merge returns `MergeResult` without metrics (defaults to `None`).
- DuckDB handler construction does not explicitly validate dependency availability; PyArrow handler raises ImportError in `__init__` if pyarrow missing.

## Candidate Unused / Legacy Code (with evidence)

> Evidence: `rg` in `src/` shows only definitions (no call sites), or only docstring examples.

- `src/fsspeckit/datasets/duckdb_cleanup_helpers.py` (`_unregister_duckdb_table`, `_cleanup_duckdb_tables`) — no references in `src/` (only in archived OpenSpec tasks).
- `DuckDBDatasetIO._merge_with_sql` (`datasets/duckdb/dataset.py:1031`) — `rg -n "_merge_with_sql"` finds only definition.
- `DuckDBDatasetIO._write_parquet_dataset_standard`, `_write_parquet_dataset_incremental`, `_perform_incremental_rewrite` — `rg` shows only definitions; no call sites.
- `DuckDBDatasetIO._clear_dataset`, `_generate_unique_filename` — definitions only, no call sites.
- DuckDB legacy methods raising `NotImplementedError`: `merge_parquet_dataset`, `insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset` — only referenced in their own docstrings.
- `PyarrowDatasetIO._normalize_filters` — `rg -n "_normalize_filters"` finds only definition; `read_parquet` does not call it.
- `datasets/pyarrow/dataset.py:_create_string_key_array` — deprecated alias with no references.
- `datasets/base.py` helper methods (`_combine_tables`, `_generate_unique_filename`, `_clear_parquet_files`, `_list_parquet_files`, `_get_file_row_count`, `_get_file_size`, `_dedupe_source_last_wins`, `_select_rows_by_keys`, `_extract_keys_from_table`) — definitions only; no call sites in `src/`.
- `core/ext/dataset.py` legacy dataset-write API (`write_pyarrow_dataset`, `insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset`, `_write_pyarrow_dataset_standard`) — all raise `NotImplementedError` and are not registered in `core/ext/register.py`; only referenced in documentation examples.
