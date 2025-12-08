# Change: Add comprehensive documentation for DuckDB merge-aware writes

## Why
The `add-duckdb-merge-aware-write` feature has been implemented but lacks comprehensive documentation. Users cannot discover new merge-aware write functionality, understand different strategies, or find practical examples. Despite OpenSpec tasks being marked complete, substantial documentation gaps exist that make the feature difficult to use.

## What Changes
- Update API reference to include new `strategy` and `key_columns` parameters for `write_parquet_dataset`
- Document convenience helpers (`insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset`) on DuckDB backends
- Add merge-aware write examples to existing how-to guides
- Integrate existing comprehensive examples into documentation structure
- Provide guidance on when to use DuckDB vs PyArrow for merge operations

## Impact
- Improves feature discoverability and usability for DuckDB merge-aware writes
- Provides clear guidance on when to use each merge strategy
- Enables users to leverage merge-aware writes effectively without trial-and-error
- Maintains documentation consistency between DuckDB and PyArrow merge functionality
- Reduces support burden by providing comprehensive self-service documentation

## Files Changed
- `docs/api/fsspeckit.datasets.md` - Updated with DuckDB merge-aware write parameters and functions
- `docs/how-to/read-and-write-datasets.md` - Added DuckDB merge-aware write section
- `docs/how-to/merge-datasets.md` - Unified guide covering DuckDB and PyArrow approaches
- `docs/how-to/index.md` - Added link to merge guide
- `examples/README.md` - Updated to reference existing merge examples
- Cross-references added between documentation and existing examples