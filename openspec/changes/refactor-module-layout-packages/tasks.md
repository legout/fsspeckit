## 1. Implementation

### 1.1 core.ext package
- [x] 1.1.1 Create `fsspeckit/core/ext/` package with:
  - `__init__.py` re-exporting the public API.
  - `csv.py`, `json.py`, `parquet.py`, `dataset.py`, `io.py`, `register.py` containing content from existing `ext_*` modules.
- [x] 1.1.2 Convert `ext_json.py`, `ext_csv.py`, `ext_parquet.py`, `ext_dataset.py`, `ext_io.py`, `ext_register.py`, `ext.py`
  into shims that import from `fsspeckit.core.ext` and re-export, optionally emitting `DeprecationWarning`.
- [x] 1.1.3 Update internal imports to the new package layout.

### 1.2 core.filesystem package
- [x] 1.2.1 Create `fsspeckit/core/filesystem/` package with:
  - `__init__.py` owning factories and high-level filesystem APIs.
  - `paths.py` with content from `filesystem_paths.py`.
  - `cache.py` with content from `filesystem_cache.py`.
  - `gitlab.py` GitLab filesystem implementation.
- [x] 1.2.2 Convert `filesystem.py`, `filesystem_paths.py`, and `filesystem_cache.py` into shims that import from the new package and re-export.
- [x] 1.2.3 Update internal imports accordingly.

### 1.3 datasets.duckdb package
- [x] 1.3.1 Create `fsspeckit/datasets/duckdb/` package with:
  - `__init__.py` re-exporting `DuckDBDatasetIO`, `DuckDBParquetHandler`, `create_duckdb_connection`, `MergeStrategy`, helpers.
  - `dataset.py` from `duckdb_dataset.py`.
  - `connection.py` from `duckdb_connection.py`.
  - `helpers.py` from `_duckdb_helpers.py` (and any remaining DuckDB helpers).
- [x] 1.3.2 Convert `duckdb.py`, `duckdb_dataset.py`, `duckdb_connection.py`, `_duckdb_helpers.py` into shims.
- [x] 1.3.3 Update internal imports.

### 1.4 datasets.pyarrow package
- [x] 1.4.1 Create `fsspeckit/datasets/pyarrow/` package with:
  - `__init__.py` re-exporting public PyArrow helpers (e.g. `merge_parquet_dataset_pyarrow`).
  - `dataset.py` from `pyarrow_dataset.py`.
  - `schema.py` from `pyarrow_schema.py`.
- [x] 1.4.2 Convert `pyarrow_dataset.py`, `pyarrow_schema.py`, and `pyarrow.py` into shims.
- [x] 1.4.3 Update internal imports.

### 1.5 common.logging package
- [x] 1.5.1 Create `fsspeckit/common/logging/` package with:
  - `__init__.py` owning `get_logger` and core logging helpers.
  - `config.py` with content from `logging_config.py`.
- [x] 1.5.2 Convert `logging.py` and `logging_config.py` into shims.
- [x] 1.5.3 Update internal imports.

## 2. Testing

- [x] 2.1 Run the full test suite and fix any import issues.
- [x] 2.2 Add targeted tests (if needed) to ensure shim modules import successfully and expose the same public API.

## 3. Documentation

- [x] 3.1 Update documentation and API reference pages to use the new package-based import paths.
- [x] 3.2 Add a short “legacy imports and deprecation” section documenting shim modules and the migration path.

