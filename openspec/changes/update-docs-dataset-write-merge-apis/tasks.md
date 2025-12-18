## 1. Update How-To Guides

### 1.1 Rewrite docs/how-to/read-and-write-datasets.md
- [x] 1.1.1 Update "Writing Datasets" section to use `write_dataset(mode='append'|'overwrite')`
- [x] 1.1.2 Add `WriteDatasetResult` usage examples showing field access
- [x] 1.1.3 Update merge-related sections to use `merge()` method
- [x] 1.1.4 Remove all references to `write_parquet_dataset(..., strategy=...)` pattern
- [x] 1.1.5 Remove references to `insert_dataset`, `upsert_dataset`, `update_dataset` convenience methods

### 1.2 Rewrite docs/how-to/merge-datasets.md
- [x] 1.2.1 Rewrite introduction to explain `merge()` API design (insert/update/upsert)
- [x] 1.2.2 Document `strategy='insert'` with complete working examples
- [x] 1.2.3 Document `strategy='update'` with complete working examples
- [x] 1.2.4 Document `strategy='upsert'` with complete working examples
- [x] 1.2.5 Add `MergeResult` documentation and usage examples
- [x] 1.2.6 Document `key_columns` parameter (single and composite keys)
- [x] 1.2.7 Document `partition_columns` parameter for partition-aware merges
- [x] 1.2.8 Add DuckDB vs PyArrow backend comparison for merge operations
- [x] 1.2.9 Remove all references to removed convenience methods

## 2. Update Tutorials

### 2.1 Fix docs/tutorials/getting-started.md
- [x] 2.1.1 Update DuckDB example to use `write_dataset()` and `merge()` if needed
- [x] 2.1.2 Rewrite PyArrow example section to use correct APIs:
  - `io.write_dataset(data, path, mode='append')` for writes
  - `io.merge(data, path, strategy='upsert', key_columns=[...])` for merges
- [x] 2.1.3 Show `WriteDatasetResult` field access in examples
- [x] 2.1.4 Show `MergeResult` field access in examples
- [x] 2.1.5 Update "Domain Package Structure" import examples to be accurate

## 3. Update Reference Documentation

### 3.1 Update docs/reference/api-guide.md
- [x] 3.1.1 Update "Dataset Operations" capability section to list current methods
- [x] 3.1.2 Document `write_dataset()` as the write API (modes: append, overwrite)
- [x] 3.1.3 Document `merge()` as the merge API (strategies: insert, update, upsert)
- [x] 3.1.4 Add `WriteDatasetResult` and `MergeResult` to type reference
- [x] 3.1.5 Remove any references to removed convenience methods
- [x] 3.1.6 Ensure PyArrow and DuckDB handlers are documented symmetrically

## 4. Update Explanation Documentation

### 4.1 Update docs/explanation/concepts.md
- [x] 4.1.1 Update "Dataset vs File-Level Operations" code examples
- [x] 4.1.2 Remove any outdated API references in code snippets
- [x] 4.1.3 Ensure all examples use current `write_dataset()` and `merge()` APIs

## 5. Validate Documentation

- [x] 5.1 Run MkDocs build to verify no broken links or errors
- [x] 5.2 Verify all code examples are syntactically valid Python
- [x] 5.3 Spot-check representative examples against current codebase
- [x] 5.4 Ensure consistent terminology across all updated files
