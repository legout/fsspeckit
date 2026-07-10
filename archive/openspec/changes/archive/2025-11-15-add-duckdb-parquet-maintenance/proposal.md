# Change Proposal: Add Parquet Dataset Maintenance (Compaction & Z-Ordering) to DuckDB Handler

## Why

Current parquet dataset support allows writing, appending, and merging but lacks lifecycle maintenance operations. Real-world analytical workloads accumulate many small parquet files ("small files problem") degrading scan performance and create suboptimal data layout for selective queries. Without compaction and data layout optimization (z-order style clustering), users face:

- Increased query latency due to excessive file listing & metadata overhead
- Poor data skipping and predicate pushdown effectiveness
- Higher storage costs from redundant tiny files
- Manual, error-prone scripts for reorganizing datasets

Providing native maintenance operations improves performance, reduces operational toil, and completes the dataset management lifecycle (write → merge → optimize).

## What Changes

- Add `compact_parquet_dataset` method to `DuckDBParquetHandler`
  - Consolidate many small files into fewer large files based on target size or row count thresholds
  - Optional partition-pruning / selective compaction
  - Preserve schema and compression settings; support recompression
- Add `optimize_parquet_dataset` method with z-order style clustering
  - Re-write dataset ordering rows by multi-column interleaving key list ("zorder_columns") to improve locality
  - Optionally applies compaction simultaneously
  - Statistics return (pre/post file counts, total bytes, compaction ratio)
- Provide dry-run mode for both methods (returns planned actions without modifying data)
- Provide file size / row count metadata collection utility `_collect_dataset_stats`
- Add requirements under `utils-duckdb` capability (new ADDED requirements)
- Tests for compaction & optimization scenarios (small files, already optimized, mixed compression)
- Examples: `duckdb_compact_example.py`, `duckdb_optimize_example.py`
- Non-breaking additive feature.

## Impact

- Affected specs: `utils-duckdb` (add maintenance capability)
- Affected code:
  - Modified: `src/fsspeckit/utils/duckdb.py` (new methods + helpers)
  - Tests: `tests/test_utils/test_duckdb.py` (new test cases)
  - Docs: `docs/utils.md` (maintenance section)
  - Examples: `examples/duckdb/` (new scripts)
- Dependencies: None (leverages DuckDB + PyArrow)
- Performance: Reduces query cost on large fragmented datasets; improves data skipping
- Risk: Rewriting data is I/O intensive; addressed with dry-run + partition filtering
