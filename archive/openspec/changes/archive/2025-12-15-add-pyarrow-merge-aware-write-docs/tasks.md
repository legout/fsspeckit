## 1. API Documentation Updates

- [x] 1.1 Update `docs/api/fsspeckit.core.ext.md` to include new `strategy` and `key_columns` parameters for `write_pyarrow_dataset`
- [x] 1.2 Add API documentation for convenience helpers:
  - [x] 1.2.1 `insert_dataset` function documentation
  - [x] 1.2.2 `upsert_dataset` function documentation  
  - [x] 1.2.3 `update_dataset` function documentation
  - [x] 1.2.4 `deduplicate_dataset` function documentation
- [x] 1.3 Ensure parameter types, defaults, and requirements are accurately documented
- [x] 1.4 Document return value differences (metadata vs None) for merge vs standard writes

## 2. How-to Guide Updates

- [x] 2.1 Add "Merge-Aware Dataset Writes" section to `docs/how-to/read-and-write-datasets.md`
- [x] 2.2 Document basic merge-aware write syntax with examples for each strategy
- [x] 2.3 Add convenience helper examples showing equivalent functionality
- [x] 2.4 Include advanced examples (composite keys, custom ordering, error handling)
- [x] 2.5 Update `docs/how-to/index.md` to include new merge guide link

## 3. Comprehensive Merge Guide

- [x] 3.1 Create new `docs/how-to/merge-datasets.md` guide
- [x] 3.2 Document each merge strategy with:
  - [x] 3.2.1 INSERT strategy use cases and examples
  - [x] 3.2.2 UPSERT strategy use cases and examples
  - [x] 3.2.3 UPDATE strategy use cases and examples
  - [x] 3.2.4 FULL_MERGE strategy use cases and examples
  - [x] 3.2.5 DEDUPLICATE strategy use cases and examples
- [x] 3.3 Add advanced features section (composite keys, performance optimization)
- [x] 3.4 Include error handling and validation guidance
- [x] 3.5 Add performance comparison with traditional merge approaches

## 4. Code Examples

- [x] 4.1 Create `examples/pyarrow/pyarrow_merge_example.py` with comprehensive examples
- [x] 4.2 Create `examples/datasets/getting_started/04_pyarrow_merges.py` for beginners
- [x] 4.3 Update `examples/datasets/getting_started/03_simple_merges.py` with merge-aware section
- [x] 4.4 Ensure all examples are tested and work out-of-the-box
- [x] 4.5 Add examples to `examples/README.md` index

## 5. Documentation Quality

- [x] 5.1 Review all new documentation for accuracy and completeness
- [x] 5.2 Ensure consistent terminology and style with existing docs
- [x] 5.3 Add cross-references between related documentation sections
- [x] 5.4 Validate all code examples in documentation
- [x] 5.5 Test documentation build process with new content