# Change: Refactor Duplicate Code Between Dataset Backends

## Why
The datasets module has massive code duplication between PyArrow and DuckDB backends, violating the DRY principle and making maintenance extremely difficult. Both backends have nearly identical implementations of merge logic, compaction, optimization, and error handling.

## What Changes
- Extract common merge logic into shared utilities in `fsspeckit.core.merge`
- Create abstract base class `BaseDatasetHandler` with shared implementations
- Refactor duplicate compaction and optimization logic into shared functions
- Standardize error handling patterns across both backends
- Eliminate code duplication in deduplication implementations

## Impact
- **Affected specs**: datasets-pyarrow, datasets-duckdb, core-merge
- **Affected code**: 
  - Multiple files in `src/fsspeckit/datasets/pyarrow/`
  - Multiple files in `src/fsspeckit/datasets/duckdb/`
  - `src/fsspeckit/core/merge.py`
- **Maintenance impact**: Dramatically reduced code duplication and easier bug fixes
- **Breaking changes**: API remains the same, internal refactoring only
