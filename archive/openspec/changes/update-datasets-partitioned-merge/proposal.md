# Change: Update partitioned merge behavior and dataset robustness

## Why
Partitioned merge inserts currently land in dataset roots because partition columns are not propagated to dataset writes. This violates hive-partition expectations and causes incorrect layouts in both DuckDB and PyArrow backends. Additionally, partition columns are inconsistently handled (some rewritten files inject partition columns, others do not), which can create mixed schemas. There is also a DuckDB compaction helper path that never executes due to indentation, and unused/duplicate code paths that increase maintenance cost.

## What Changes
- Propagate `partition_columns` from merge operations into dataset writes for DuckDB and PyArrow.
- Standardize partition column policy for hive-style datasets to prevent mixed schemas during rewrites.
- Enforce partition immutability validation in PyArrow merges.
- Fix the DuckDB compaction helper execution path.
- Remove unused merge implementations and helper functions that are not referenced.
- Add regression tests for partitioned merge inserts, immutability, and compaction behavior.
- Update docs/changelog where behavior is clarified.

## Impact
- Affected specs: `datasets-parquet-io`, `utils-pyarrow`
- Affected code: `src/fsspeckit/datasets/duckdb/dataset.py`, `src/fsspeckit/datasets/pyarrow/io.py`, `src/fsspeckit/datasets/pyarrow/dataset.py`, `src/fsspeckit/core/incremental.py`, tests/docs
