# Change: Add PyarrowDatasetIO/Handler for UX parity with DuckDB

## Why
DuckDB has a `DuckDBDatasetIO` class and `DuckDBParquetHandler` façade. PyArrow exposes functions but no symmetric
handler class. Providing a `PyarrowDatasetIO` plus a thin `PyarrowDatasetHandler` wrapper would align the user
experience across backends and make it easier to switch between PyArrow and DuckDB while using similar method names and
ergonomics.

## What Changes
- Introduce `PyarrowDatasetIO` class under the `fsspeckit.datasets.pyarrow` package that encapsulates PyArrow dataset
  operations (read, write, merge, compact, optimize) using existing functions like `merge_parquet_dataset_pyarrow`.
- Add `PyarrowDatasetHandler` wrapper for convenience/backward compatibility, re-exported from `fsspeckit.datasets`.
- Align public method surface with DuckDB’s handler where applicable (`read_parquet`, `write_parquet_dataset`,
  `merge_parquet_dataset`, `compact_parquet_dataset`, `optimize_parquet_dataset`), omitting operations that are not
  supported in PyArrow.
- Ensure optional dependency handling remains lazy (only requires PyArrow when used).

## Impact
- Improves UX symmetry between PyArrow and DuckDB.
- Adds new public classes; does not break existing APIs.
- Requires doc updates and minimal wiring in `fsspeckit.datasets` to re-export the handler.
