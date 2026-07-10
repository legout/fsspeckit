## Why

Core JSON/CSV/Parquet helpers in `fsspeckit.core.ext` have subtle but user-visible bugs and inconsistencies:

- `use_threads=True` in JSON/CSV readers is ignored because a sequential path overwrites parallel results.
- Parquet helpers rely on `pyarrow` imports that are not guaranteed to exist at runtime.
- `read_parquet_file(..., include_file_path=True)` constructs a `file_path` column using Polars types on a PyArrow `Table`.
- `write_json` calls `orjson.dumps` without using the existing optional-dependency pattern.

These issues break expectations around performance, type stability, and optional dependencies and are called out explicitly in multiple code reviews.

## What Changes

- Fix control flow in `_read_json` and `_read_csv` so `use_threads` truly controls parallel vs sequential execution.
- Ensure Parquet helpers (`_read_parquet_file`, `_read_parquet`, `_read_parquet_batches`) import `pyarrow`/`pyarrow.parquet` lazily via the optional-dependency helpers.
- Ensure `include_file_path=True` on Parquet reads adds a `file_path` column using native PyArrow arrays, not Polars.
- Update `write_json` to obtain `orjson` via the lazy `_import_orjson` helper and to fail with a clear error when the `sql`/JSON extra is missing.

## Impact

- **Behaviour:**  
  - JSON/CSV readers will honour `use_threads` and no longer perform redundant sequential work when threads are requested.  
  - Parquet readers will produce consistent PyArrow tables when `include_file_path=True`.  
  - JSON writing will either work correctly with `orjson` or raise a clear, guided `ImportError`.

- **Optional dependencies:**  
  - `pyarrow` and `orjson` remain optional and are only required when the corresponding functionality is used.

- **Specs affected:**  
  - `core-lazy-imports` (core filesystem and extension layer use of lazy imports).  
  - `utils-pyarrow` (PyArrow-based table utilities and schema handling).

- **Code affected (non-exhaustive):**
  - `src/fsspeckit/core/ext.py` JSON/CSV/Parquet helpers.

