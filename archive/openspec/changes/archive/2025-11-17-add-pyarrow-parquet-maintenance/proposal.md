# Change Proposal: Add PyArrow-Based Parquet Dataset Maintenance

## Why

`DuckDBParquetHandler` currently provides `compact_parquet_dataset` and
`optimize_parquet_dataset` methods for file-level maintenance of parquet
datasets. These operations are extremely useful, but they rely on DuckDB.

Some deployments:

- Cannot ship DuckDB for operational, packaging, or policy reasons.
- Already have strong PyArrow-based pipelines and prefer to stay within that
  dependency set.

We want parity maintenance operations implemented purely with PyArrow and
fsspec:

- Compaction of many small files into fewer larger files.
- Z-order-style clustering and optional compaction in a single pass.
- Validation and dry-run planning for safe operation on large datasets.
- Streaming, chunked processing to avoid loading the entire dataset into memory.

## What Changes

- Add `collect_dataset_stats_pyarrow` and two helpers under the `utils-pyarrow`
  capability:
  - `compact_parquet_dataset_pyarrow`
  - `optimize_parquet_dataset_pyarrow`
- Implement:
  - Dataset statistics collection using file-level metadata and per-file row
    counts.
  - Grouping algorithms for file compaction based on target MB per file and/or
    target rows per file.
  - Dry-run modes that return a structured plan plus before/after metrics
    without touching the underlying files.
  - Live rewrite modes that:
    - Read groups of files via PyArrow in a streaming fashion.
    - Write combined/clustered files back to the dataset directory via fsspec.
    - Delete original files only after successful writes.
- Avoid naive `dataset.to_table()` over the entire dataset; rely on per-file
  scans and partition filters instead.

## Impact

- New change affects the `utils-pyarrow` capability only.
- No breaking changes; DuckDB-based maintenance remains available.
- New tests and examples will be added to demonstrate PyArrow-only maintenance
  workflows.
- Docs will gain a “PyArrow Parquet Maintenance” section alongside the existing
  DuckDB maintenance docs.

