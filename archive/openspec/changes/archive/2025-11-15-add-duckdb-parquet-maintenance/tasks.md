# Implementation Tasks

## 1. Implementation
- [x] 1.1 Add `compact_parquet_dataset` public method
- [x] 1.2 Add `optimize_parquet_dataset` public method
- [x] 1.3 Implement `_collect_dataset_stats` helper (file sizes, rows, schema)
- [x] 1.4 Implement file discovery via filesystem + DuckDB metadata
- [x] 1.5 Implement small file grouping logic (size or row threshold)
- [x] 1.6 Implement writing consolidated files preserving compression
- [x] 1.7 Implement option to recompress with new codec
- [x] 1.8 Implement dry-run (no writes, returns plan & metrics)
- [x] 1.9 Implement z-order style ordering (multi-column interleave ordering approximation)
- [x] 1.10 Implement statistics return object (pre/post counts, bytes, ratio)
- [x] 1.11 Implement partition filter (limit paths considered)
- [x] 1.12 Add robust validation (thresholds > 0, columns exist, etc.)
- [x] 1.13 Add docstrings + type hints

## 2. Testing
- [x] 2.1 Fixture to create fragmented dataset (many tiny files)
- [x] 2.2 Test compaction reduces file count while preserving row count
- [x] 2.3 Test compaction with target_rows_per_file parameter
- [x] 2.4 Test compaction dry-run shows plan and does not modify
- [x] 2.5 Test optimize with z-order columns improves clustering (order assertion)
- [x] 2.6 Test optimize + compaction combined
- [x] 2.7 Test recompression changes codec
- [x] 2.8 Test partition filter limits scope
- [x] 2.9 Test invalid columns for z-order raises error
- [x] 2.10 Test empty dataset no-op behavior
- [x] 2.11 Test stats integrity (bytes, counts)
- [x] 2.12 Integration test query performance improvement (approximate: fewer files scanned)

## 3. Documentation
- [x] 3.1 Add maintenance section to `docs/utils.md`
- [x] 3.2 Example script `duckdb_compact_example.py`
- [x] 3.3 Example script `duckdb_optimize_example.py`
- [x] 3.4 Add usage to README quickstart snippet (optional)
- [x] 3.5 Document z-order limitations (approximation, not true Morton code)
- [x] 3.6 Document dry-run safety recommendations

## 4. Validation
- [x] 4.1 Run full test suite
- [x] 4.2 Run mypy on modified file
- [x] 4.3 Run ruff if configured
- [x] 4.4 Validate change: `openspec validate add-duckdb-parquet-maintenance --strict`
- [x] 4.5 Manual inspection of example outputs

## 5. Risks / Follow-ups
- [x] 5.1 Note future enhancement: true partition-aware z-order at scale
- [x] 5.2 Note future enhancement: adaptive target file sizing by row group stats
