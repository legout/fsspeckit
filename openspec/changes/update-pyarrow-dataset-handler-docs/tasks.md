## 1. API Reference Documentation Updates

- [ ] 1.1 Add complete `PyarrowDatasetIO` class documentation to `docs/api/fsspeckit.datasets.md`
  - [ ] 1.1.1 Document `write_parquet_dataset()` method with parameter table and examples
  - [ ] 1.1.2 Document `insert_dataset()`, `upsert_dataset()`, `update_dataset()`, `deduplicate_dataset()` convenience methods
  - [ ] 1.1.3 Document `merge_parquet_dataset()`, `compact_parquet_dataset()`, `optimize_parquet_dataset()` maintenance methods
  - [ ] 1.1.4 Document `read_parquet()` and `write_parquet()` basic I/O methods
- [ ] 1.2 Add `PyarrowDatasetHandler` class documentation to `docs/api/fsspeckit.datasets.md`
  - [ ] 1.2.1 Document handler as high-level interface with method summary table
  - [ ] 1.2.2 Include context manager usage examples
- [ ] 1.3 Update autodoc section to include new PyArrow classes
  - [ ] 1.3.1 Verify `::: fsspeckit.datasets` includes both PyArrow classes
  - [ ] 1.3.2 Test that all method signatures are correctly generated

## 2. Dataset Handlers Documentation Updates

- [ ] 2.1 Add class-based PyArrow section to `docs/dataset-handlers.md`
  - [ ] 2.1.1 Document `PyarrowDatasetIO` class-based interface with strengths and features
  - [ ] 2.1.2 Include comprehensive usage examples for all major operations
  - [ ] 2.1.3 Document context manager support and resource management
- [ ] 2.2 Update backend comparison section
  - [ ] 2.2.1 Add PyArrow class-based approach to comparison table
  - [ ] 2.2.2 Update "Choosing a Backend" section to include class-based PyArrow
- [ ] 2.3 Add approach guidance section
  - [ ] 2.3.1 Create comparison between function-based vs class-based PyArrow approaches
  - [ ] 2.3.2 Provide use case recommendations for each approach
  - [ ] 2.3.3 Include migration guidance between approaches

## 3. Getting Started Tutorial Updates

- [ ] 3.1 Add PyArrow examples to "Your First Dataset Operation" section
  - [ ] 3.1.1 Add PyArrow class-based examples alongside existing DuckDB examples
  - [ ] 3.1.2 Show both `PyarrowDatasetIO` and `PyarrowDatasetHandler` usage
  - [ ] 3.1.3 Include merge-aware write examples with PyArrow
- [ ] 3.2 Update domain package structure section
  - [ ] 3.2.1 Add PyArrow handler imports to package structure examples
  - [ ] 3.2.2 Show both DuckDB and PyArrow import patterns
- [ ] 3.3 Update common use cases section
  - [ ] 3.3.1 Add PyArrow handler examples to dataset operations use case
  - [ ] 3.3.2 Ensure parity between DuckDB and PyArrow examples

## 4. API Guide Updates

- [ ] 4.1 Update "Dataset Operations" capability section in `docs/reference/api-guide.md`
  - [ ] 4.1.1 Add `PyarrowDatasetIO` and `PyarrowDatasetHandler` to classes list
  - [ ] 4.1.2 Update capability description to mention PyArrow class-based interface
- [ ] 4.2 Update domain package organization section
  - [ ] 4.2.1 Describe PyArrow class-based functionality in Datasets section
  - [ ] 4.2.2 Mention both function-based and class-based PyArrow approaches
- [ ] 4.3 Update usage patterns section
  - [ ] 4.3.1 Add PyArrow handler examples to basic workflow
  - [ ] 4.3.2 Show equivalent operations between DuckDB and PyArrow handlers

## 5. How-to Guides Updates

- [ ] 5.1 Update "Read and Write Datasets" guide (`docs/how-to/read-and-write-datasets.md`)
  - [ ] 5.1.1 Add section showing `PyarrowDatasetIO` class usage
  - [ ] 5.1.2 Include `PyarrowDatasetHandler` examples with context manager
  - [ ] 5.1.3 Update "Backend Selection Guidance" to include class-based PyArrow
  - [ ] 5.1.4 Show comparison between function-based and class-based approaches
- [ ] 5.2 Update "Merge Datasets" guide (`docs/how-to/merge-datasets.md`)
  - [ ] 5.2.1 Add examples using `PyarrowDatasetIO` for merge operations
  - [ ] 5.2.2 Show `PyarrowDatasetHandler` merge examples
  - [ ] 5.2.3 Update import statements throughout to show class-based approach
  - [ ] 5.2.4 Ensure all merge strategies have PyArrow class examples

## 6. Validation and Testing

- [ ] 6.1 Validate all code examples
  - [ ] 6.1.1 Test all PyArrow code examples for correctness
  - [ ] 6.1.2 Verify import statements work with new package structure
  - [ ] 6.1.3 Ensure examples run without errors in clean environment
- [ ] 6.2 Check cross-references and links
  - [ ] 6.2.1 Verify all internal links work correctly
  - [ ] 6.2.2 Check that cross-references between approaches are accurate
  - [ ] 6.2.3 Test navigation between related documentation sections
- [ ] 6.3 Review documentation consistency
  - [ ] 6.3.1 Ensure terminology is consistent across all updated files
  - [ ] 6.3.2 Verify formatting matches existing documentation standards
  - [ ] 6.3.3 Check that examples follow established patterns