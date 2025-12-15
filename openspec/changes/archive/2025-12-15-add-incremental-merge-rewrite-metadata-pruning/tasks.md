## 1. Specification
- [x] 1.1 Add incremental rewrite requirements to `utils-duckdb`
- [x] 1.2 Add incremental rewrite requirements to `utils-pyarrow`

## 2. Public API
- [x] 2.1 Add `rewrite_mode` kwarg to `write_parquet_dataset` and `merge_parquet_dataset`
- [x] 2.2 Update `DatasetHandler` protocol accordingly

## 3. DuckDB Backend (Incremental Upsert/Update)
- [x] 3.1 Implement candidate file discovery (partition pruning first, then parquet metadata pruning)
- [x] 3.2 For affected files: read affected rows + apply merge semantics
- [x] 3.3 Write replacement parquet files with unique names; delete only affected old parquet files
- [x] 3.4 For UPSERT: write inserted rows to new parquet files (unique names)
- [x] 3.5 Keep behavior atomic (staging + commit) and safe on failure

## 4. PyArrow Backend (Incremental Upsert/Update)
- [x] 4.1 Implement candidate file discovery using PyArrow dataset fragments and parquet metadata
- [x] 4.2 Apply merge semantics without full dataset materialization where feasible
- [x] 4.3 Rewrite only affected files; preserve unaffected files
- [x] 4.4 Ensure unique filenames for any new/replacement files

## 5. Tests + Docs
- [x] 5.1 Add tests proving unaffected files remain untouched for incremental mode
- [x] 5.2 Add tests proving correctness fallback (metadata not helpful ⇒ rewrites more files, not fewer)
- [x] 5.3 Document when incremental mode helps (partitioned datasets, bloom filters) and when it may not

## Additional Implementation Details

### Core Infrastructure Created
- **Shared Utilities**: `src/fsspeckit/core/incremental.py` with metadata analysis, partition pruning, and file management
- **Validation Logic**: Enhanced `src/fsspeckit/core/merge.py` with rewrite_mode compatibility validation
- **Interface Updates**: Complete protocol updates in `src/fsspeckit/datasets/interfaces.py`

### Backend-Specific Implementations
- **DuckDB**: `_write_parquet_dataset_incremental()` with staging + commit pattern
- **PyArrow**: `_write_parquet_dataset_incremental()` with dataset API integration

### Testing & Documentation
- **Test Suite**: `tests/test_incremental_rewrite.py` with comprehensive validation and integration tests
- **Implementation Guide**: `INCREMENTAL_REWRITE_IMPLEMENTATION.md` with usage examples and architecture details

## Implementation Status: ✅ COMPLETE

All specified tasks have been successfully implemented with full functionality for both DuckDB and PyArrow backends.

