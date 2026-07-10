## 1. Implementation
- [x] 1.1 Implement `_merge_upsert()` method with proper UPSERT semantics
- [x] 1.2 Implement `_merge_update()` method with proper UPDATE semantics
- [x] 1.3 Implement `_extract_inserted_rows()` for identifying new records
- [x] 1.4 Fix PyArrow merge implementations to use vectorized operations
- [x] 1.5 Test all merge strategies across both backends
- [x] 1.6 Add comprehensive test coverage for merge operations

## 2. Validation
- [x] 2.1 Run existing tests to ensure no regressions
- [x] 2.2 Test with sample datasets to verify functionality
- [obsolete] 2.3 Performance test with large datasets (out of scope for this proposal)

## 3. Documentation
- [x] 3.1 Update docstrings for implemented methods
- [x] 3.2 Add usage examples for merge operations
