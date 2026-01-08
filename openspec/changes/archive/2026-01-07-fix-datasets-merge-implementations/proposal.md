# Change: Fix Missing Dataset Merge Implementations

## Why
The datasets module has critical merge functionality that is not implemented, causing `NotImplementedError` when users try to perform essential operations like upsert, update, and merge operations. The methods `_merge_upsert()` and `_merge_update()` in DuckDBDatasetIO are just `pass` statements, making the core functionality unusable.

## What Changes
- Implement `_merge_upsert()` method in DuckDBDatasetIO with proper UPSERT semantics
- Implement `_merge_update()` method in DuckDBDatasetIO with proper UPDATE semantics  
- Implement `_extract_inserted_rows()` method for identifying new records
- Fix PyArrow merge implementations to use vectorized operations instead of Python loops
- Ensure all merge strategies work correctly across both backends

## Impact
- **Affected specs**: datasets-duckdb, datasets-pyarrow
- **Affected code**: 
  - `src/fsspeckit/datasets/duckdb/dataset.py` (lines 1370-1400)
  - `src/fsspeckit/datasets/pyarrow/io.py` (lines 696-759)
- **User impact**: Core merge functionality becomes usable
- **Breaking changes**: None
