## 1. Implementation

- [x] 1.1 Fix `_read_json` control flow so `use_threads=True` uses `run_parallel` and `use_threads=False` uses a sequential comprehension without double work.
- [x] 1.2 Fix `_read_csv` control flow with the same pattern as `_read_json`.
- [x] 1.3 Refactor Parquet helpers to import `pyarrow`/`pyarrow.parquet` via the optional-dependency helpers.
- [x] 1.4 Change `read_parquet_file(..., include_file_path=True)` to add a `file_path` column using a PyArrow array, not Polars.
- [x] 1.5 Update `write_json` to obtain `orjson` via `_import_orjson` and to fail with a clear, guided `ImportError` when the `sql`/JSON extra is missing.

## 2. Testing

- [x] 2.1 Add/extend tests to verify that `use_threads=True` and `use_threads=False` both produce the same data but exercise different code paths.
- [x] 2.2 Add tests for Parquet `include_file_path=True` that assert a PyArrow `Table` with a string `file_path` column is returned.
- [x] 2.3 Add tests that exercise `write_json` in environments with and without `orjson` installed, asserting the error message matches the optional-extra guidance.

## 3. Documentation

- [x] 3.1 Update any relevant docstrings or README snippets that describe threading behaviour or `include_file_path` semantics to match the corrected implementation.

