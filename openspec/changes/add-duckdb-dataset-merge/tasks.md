# Implementation Tasks

## 1. Implementation

- [x] 1.1 Add `merge_parquet_dataset` method to `DuckDBParquetHandler` class
- [x] 1.2 Implement key column validation logic
- [x] 1.3 Implement schema compatibility validation
- [x] 1.4 Implement UPSERT merge strategy SQL logic
- [x] 1.5 Implement INSERT merge strategy SQL logic
- [x] 1.6 Implement UPDATE merge strategy SQL logic
- [x] 1.7 Implement FULL_MERGE strategy SQL logic
- [x] 1.8 Implement DEDUPLICATE strategy with dedup_order_by support
- [x] 1.9 Implement merge statistics calculation
- [x] 1.10 Add helper method `_validate_merge_inputs` for validation
- [x] 1.11 Add helper method `_calculate_merge_stats` for statistics
- [x] 1.12 Add helper method `_execute_merge_strategy` for strategy dispatch
- [x] 1.13 Handle source as PyArrow table or path
- [x] 1.14 Add comprehensive type hints using Literal for strategies
- [x] 1.15 Add detailed Google-style docstrings with strategy explanations

## 2. Testing

- [x] 2.1 Create test fixtures for merge scenarios
- [x] 2.2 Write tests for UPSERT strategy (single and composite keys)
- [x] 2.3 Write tests for INSERT strategy (new records only)
- [x] 2.4 Write tests for UPDATE strategy (existing records only)
- [x] 2.5 Write tests for FULL_MERGE strategy (with deletes)
- [x] 2.6 Write tests for DEDUPLICATE strategy (with and without order)
- [x] 2.7 Write tests for merge statistics accuracy
- [x] 2.8 Write tests for key column validation
- [x] 2.9 Write tests for schema validation
- [x] 2.10 Write tests for NULL key detection
- [x] 2.11 Write tests for empty source/target handling
- [x] 2.12 Write tests for error conditions
- [x] 2.13 Write tests for compression parameter
- [x] 2.14 Write tests for source as table vs path
- [x] 2.15 Write integration tests combining merge with read/query

## 3. Documentation

- [x] 3.1 Add comprehensive docstring to `merge_parquet_dataset` method
- [x] 3.2 Document each merge strategy with clear explanations
- [x] 3.3 Update `docs/utils.md` with merge operations section
- [x] 3.4 Create example script: `duckdb_merge_example.py`
- [x] 3.5 Document best practices for merge operations
- [x] 3.6 Document performance characteristics of each strategy
- [x] 3.7 Add troubleshooting guide for common merge issues
- [x] 3.8 Document key column requirements and constraints

## 4. Validation

- [x] 4.1 Run `pytest tests/test_utils/test_duckdb.py -v` (all tests)
- [x] 4.2 Run `ruff check src/fsspeckit/utils/duckdb.py`
- [x] 4.3 Run `mypy src/fsspeckit/utils/duckdb.py`
- [x] 4.4 Verify test coverage above 80%
- [x] 4.5 Run merge example script
- [x] 4.6 Validate OpenSpec: `openspec validate add-duckdb-dataset-merge --strict`
- [x] 4.7 Performance test with large datasets
- [x] 4.8 Validate merge statistics accuracy across strategies

## 5. Examples and Use Cases

- [x] 5.1 Example: Change Data Capture (CDC) workflow
- [x] 5.2 Example: Incremental data loads (INSERT strategy)
- [x] 5.3 Example: Dimension table updates (UPDATE strategy)
- [x] 5.4 Example: Full synchronization (FULL_MERGE strategy)
- [x] 5.5 Example: Deduplication before merge
- [x] 5.6 Example: Composite key merges
- [x] 5.7 Example: Merge with compression options
- [x] 5.8 Example: Error handling and validation
