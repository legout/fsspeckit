## 1. Implementation

### 1.1 Prepare OpenSpec proposal and deltas
- [x] Create `proposal.md` with why/what/impact sections (completed)
- [x] Create `design.md` documenting technical decisions (completed)
- [x] Create `tasks.md` with implementation checklist (this file)
- [x] Create `specs/datasets-duckdb/spec.md` delta with MODIFIED requirements
- [x] Create `specs/project-docs/spec.md` delta with MODIFIED requirements

### 1.2 Implement spec updates
- [x] Review and update `specs/datasets-duckdb/spec.md` with requirements for current API
- [x] Ensure requirements include scenarios for DuckDB merge parameters and backend-specific features
- [x] Update `specs/project-docs/spec.md` with dataset documentation requirements

### 1.3 Run validation
- [x] Run `openspec validate update-datasets-docs-latest-ux --strict`
- [x] Fix any validation errors before implementation

### 1.4 Audit current documentation
- [x] Read `docs/dataset-handlers.md` to understand current backend differences
- [x] Read `docs/how-to/merge-datasets.md` to identify outdated examples
- [x] Read `docs/how-to/read-and-write-datasets.md` to check handler usage patterns
- [x] Search docs for mentions of `DuckDBParquetHandler`, removed APIs, or legacy methods

### 1.5 Update backend differences documentation
- [x] Update `docs/dataset-handlers.md` backend differences table:
  - Add filter types row (DuckDB: `str | None` vs PyArrow: flexible)
  - Add merge backend row (DuckDB: `use_merge` vs PyArrow: streaming knobs)
  - Verify handler signatures match BaseDatasetHandler
- [x] Add cross-link from `docs/dataset-handlers.md` to generated API pages

### 1.6 Update merge how-to guide
- [x] Update `docs/how-to/merge-datasets.md`:
  - Add DuckDB `use_merge` parameter example
  - Document MERGE vs UNION ALL behavior for DuckDB
  - Add PyArrow streaming/memory knobs examples
  - Replace any old API references with current handlers
  - Add cross-link to `docs/dataset-handlers.md`
- [x] Ensure all code examples use `DuckDBDatasetIO` or `PyarrowDatasetIO`

### 1.7 Update read/write how-to guide
- [x] Update `docs/how-to/read-and-write-datasets.md`:
  - Add dataset section with handler usage examples
  - Document filter expectations per backend (DuckDB: SQL string; PyArrow: flexible)
  - Update all code examples to use current imports
  - Add link to `docs/dataset-handlers.md` for backend differences

### 1.8 Update API reference guide
- [x] Update `docs/reference/api-guide.md`:
  - Remove references to `DuckDBParquetHandler` and other removed APIs
  - Add guidance that API details are in generated pages
  - Point users to `docs/dataset-handlers.md` for backend-specific guidance
  - Ensure all recommended imports reference current handler classes

### 1.9 Update SQL filters guide (if needed)
- [x] Review `docs/how-to/use-sql-filters.md` for dataset filter documentation
- [x] Add section on dataset read filters per backend if not already present
- [x] Update examples to match backend filter type requirements

### 1.10 Search and replace removed API references
- [x] Search docs for all mentions of `DuckDBParquetHandler`
- [x] Search docs for mentions of `write_parquet_dataset` (old) or other legacy methods
- [x] Replace references with current handler classes or add deprecation notices
- [x] Verify no broken links or outdated examples remain
- [x] Update supporting documentation files:
  - `docs/explanation/concepts.md` - Updated import examples
  - `docs/explanation/architecture.md` - Updated API layer reference
  - `docs/how-to/migrate-package-layout.md` - Updated migration examples
  - `docs/reference/utils.md` - Updated multiple code examples
  - `docs/installation.md` - Updated installation references
  - `docs/api/fsspeckit.datasets.md` - Removed obsolete DuckDBParquetHandler section

### 1.9 Update SQL filters guide (if needed)
- [x] Review `docs/how-to/use-sql-filters.md` for dataset filter documentation
- [x] Add section on dataset read filters per backend if not already present
- [x] Update examples to match backend filter type requirements

### 1.10 Search and replace removed API references
- [x] Search docs for all mentions of `DuckDBParquetHandler`
- [x] Search docs for mentions of `write_parquet_dataset` (old) or other legacy methods
- [x] Replace references with current handler classes or add deprecation notices
- [x] Verify no broken links or outdated examples remain

## 2. Validation and Review

### 2.1 Build and verify docs
- [x] Run `mkdocs build` to confirm all pages render correctly
- [x] Serve docs locally with `mkdocs serve`
- [x] Check `docs/dataset-handlers.md` is accessible from how-to pages
- [x] Verify code examples work (copy-paste test if possible)
- [x] Check for broken links to generated API pages

### 2.2 Cross-check implementation alignment
- [x] Verify documented behavior matches actual code (e.g., DuckDB filter validation)
- [x] Check backend-specific parameters are correctly documented (DuckDB `use_merge`, PyArrow streaming knobs)
- [x] Ensure return types are accurately described (MergeResult vs list[pq.FileMetaData])

### 2.3 Request approval
- [x] Share proposal for review before implementing changes
- [x] Address any feedback on proposed documentation updates
- [x] Update proposal/design/tasks based on feedback

### 2.4 Archive change
- [x] Move `openspec/changes/update-datasets-docs-latest-ux/` to `openspec/changes/archive/YYYY-MM-DD-update-datasets-docs-latest-ux/`
- [x] Run `openspec validate --strict` after archiving
- [x] Commit and push documentation changes
