## 1. API Reference Documentation Updates

- [x] 1.1 Add complete `PyarrowDatasetIO` class documentation to `docs/api/fsspeckit.datasets.md`
  - [x] 1.1.1 Document `write_parquet_dataset()` method with parameter table and examples
  - [x] 1.1.2 Document `insert_dataset()`, `upsert_dataset()`, `update_dataset()`, `deduplicate_dataset()` convenience methods
  - [x] 1.1.3 Document `merge_parquet_dataset()`, `compact_parquet_dataset()`, `optimize_parquet_dataset()` maintenance methods
  - [x] 1.1.4 Document `read_parquet()` and `write_parquet()` basic I/O methods
- [x] 1.2 Add `PyarrowDatasetHandler` class documentation to `docs/api/fsspeckit.datasets.md`
  - [x] 1.2.1 Document handler as high-level interface with method summary table
  - [x] 1.2.2 Include context manager usage examples
- [x] 1.3 Update autodoc section to include new PyArrow classes
  - [x] 1.3.1 Verify `::: fsspeckit.datasets` includes both PyArrow classes
  - [x] 1.3.2 Test that all method signatures are correctly generated

## 2. Dataset Handlers Documentation Updates

- [x] 2.1 Add class-based PyArrow section to `docs/dataset-handlers.md`
  - [x] 2.1.1 Document `PyarrowDatasetIO` class-based interface with strengths and features
  - [x] 2.1.2 Include comprehensive usage examples for all major operations
  - [x] 2.1.3 Document context manager support and resource management
- [x] 2.2 Update backend comparison section
  - [x] 2.2.1 Add PyArrow class-based approach to comparison table
  - [x] 2.2.2 Update "Choosing a Backend" section to include class-based PyArrow
- [x] 2.3 Add approach guidance section
  - [x] 2.3.1 Create comparison between function-based vs class-based PyArrow approaches
  - [x] 2.3.2 Provide use case recommendations for each approach
  - [x] 2.3.3 Include migration guidance between approaches

## 3. Getting Started Tutorial Updates

- [x] 3.1 Add PyArrow examples to "Your First Dataset Operation" section
  - [x] 3.1.1 Add PyArrow class-based examples alongside existing DuckDB examples
  - [x] 3.1.2 Show both `PyarrowDatasetIO` and `PyarrowDatasetHandler` usage
  - [x] 3.1.3 Include merge-aware write examples with PyArrow
- [x] 3.2 Update domain package structure section
  - [x] 3.2.1 Add PyArrow handler imports to package structure examples
  - [x] 3.2.2 Show both DuckDB and PyArrow import patterns
- [x] 3.3 Update common use cases section
  - [x] 3.3.1 Add PyArrow handler examples to dataset operations use case
  - [x] 3.3.2 Ensure parity between DuckDB and PyArrow examples

## 4. API Guide Updates

- [x] 4.1 Update "Dataset Operations" capability section in `docs/reference/api-guide.md`
  - [x] 4.1.1 Add `PyarrowDatasetIO` and `PyarrowDatasetHandler` to classes list
  - [x] 4.1.2 Update capability description to mention PyArrow class-based interface
- [x] 4.2 Update domain package organization section
  - [x] 4.2.1 Describe PyArrow class-based functionality in Datasets section
  - [x] 4.2.2 Mention both function-based and class-based PyArrow approaches
- [x] 4.3 Update usage patterns section
  - [x] 4.3.1 Add PyArrow handler examples to basic workflow
  - [x] 4.3.2 Show equivalent operations between DuckDB and PyArrow handlers

## 5. How-to Guides Updates

- [x] 5.1 Update "Read and Write Datasets" guide (`docs/how-to/read-and-write-datasets.md`)
  - [x] 5.1.1 Add section showing `PyarrowDatasetIO` class usage
  - [x] 5.1.2 Include `PyarrowDatasetHandler` examples with context manager
  - [x] 5.1.3 Update "Backend Selection Guidance" to include class-based PyArrow
  - [x] 5.1.4 Show comparison between function-based and class-based approaches
- [x] 5.2 Update "Merge Datasets" guide (`docs/how-to/merge-datasets.md`)
  - [x] 5.2.1 Add examples using `PyarrowDatasetIO` for merge operations
  - [x] 5.2.2 Show `PyarrowDatasetHandler` merge examples
  - [x] 5.2.3 Update import statements throughout to show class-based approach
  - [x] 5.2.4 Ensure all merge strategies have PyArrow class examples

## 6. Validation and Testing

- [x] 6.1 Validate all code examples
  - [x] 6.1.1 Test all PyArrow code examples for correctness
  - [x] 6.1.2 Verify import statements work with new package structure
  - [x] 6.1.3 Ensure examples run without errors in clean environment
- [x] 6.2 Check cross-references and links
  - [x] 6.2.1 Verify all internal links work correctly
  - [x] 6.2.2 Check that cross-references between approaches are accurate
  - [x] 6.2.3 Test navigation between related documentation sections
- [x] 6.3 Review documentation consistency
  - [x] 6.3.1 Ensure terminology is consistent across all updated files
  - [x] 6.3.2 Verify formatting matches existing documentation standards
  - [x] 6.3.3 Check that examples follow established patterns