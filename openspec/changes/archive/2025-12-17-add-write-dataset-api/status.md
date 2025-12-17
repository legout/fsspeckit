# Status: ✅ IMPLEMENTED

## What Exists
- Canonical result dataclasses: `src/fsspeckit/datasets/write_result.py` (`FileWriteMetadata`, `WriteDatasetResult`).
- Backend APIs:
  - `PyarrowDatasetIO.write_dataset(...)` in `src/fsspeckit/datasets/pyarrow/io.py`
  - `DuckDBDatasetIO.write_dataset(...)` in `src/fsspeckit/datasets/duckdb/dataset.py`
- Semantics:
  - `mode="append"` writes new parquet files without touching existing ones (unique filenames)
  - `mode="overwrite"` removes existing parquet data files and preserves non-parquet files
- Metadata capture:
  - PyArrow: uses `pyarrow.dataset.write_dataset(..., file_visitor=...)` (fallback to `pq.read_metadata`/`fs.size` when needed)
  - DuckDB: writes into a staging directory then moves files into the dataset, collecting per-file row counts via Parquet metadata

## Tests
- `tests/test_write_dataset_api.py` passes.
