## 1. Specs
- [x] Add `datasets-parquet-io` requirements for `write_dataset(mode=append|overwrite)` and file metadata returns.

## 2. Public API surface
- [x] Add `write_dataset(data, path, mode="append"|"overwrite", ...)` to both backends' dataset IO classes.
- [x] Add canonical result dataclasses for written file metadata.

## 3. PyArrow backend
- [x] Implement `write_dataset` via `pyarrow.dataset.write_dataset` with `file_visitor` metadata capture.
- [x] Implement `mode="overwrite"` semantics as "remove existing parquet data files, preserve non-parquet files".

## 4. DuckDB backend
- [x] Implement `write_dataset` via `COPY ... (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE, RETURN_STATS TRUE)`.
- [x] Map DuckDB RETURN_STATS output into the canonical result dataclasses.

## 5. Tests
- [x] Add tests for append (no collisions, preserves existing parquet files).
- [x] Add tests for overwrite (removes only parquet files, preserves non-parquet files).
- [x] Add tests asserting file metadata is returned for all newly written files.

