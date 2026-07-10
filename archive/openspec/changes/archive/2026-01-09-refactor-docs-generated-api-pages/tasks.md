## 1. Implementation

### 1.1 Prepare OpenSpec proposal and deltas
- [x] Create `proposal.md` with why/what/impact sections
- [x] Create `design.md` documenting technical decisions (completed)
- [x] Create `tasks.md` with implementation checklist (this file)
- [x] Create `specs/project-docs/spec.md` delta with MODIFIED requirements

### 1.2 Implement spec updates
- [x] Review and update `specs/project-docs/spec.md` with requirements for mkdocstrings-only API pages
- [x] Ensure requirements include scenarios for validation

### 1.3 Run validation
- [x] Run `openspec validate refactor-docs-generated-api-pages --strict`
- [x] Fix any validation errors before implementation

### 1.4 Update `docs/api/fsspeckit.datasets.md`
- [x] Replace entire file content with mkdocstrings directive:
  ```markdown
  # `fsspeckit.datasets` API Reference

  ::: fsspeckit.datasets
  ```
- [x] Ensure no manual tables or parameter descriptions remain

### 1.5 Update `docs/api/index.md`
- [x] Remove references to removed APIs (e.g., `DuckDBParquetHandler`)
- [x] Update migration tips to reference current handlers:
  - `from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection`
  - `from fsspeckit.datasets.pyarrow import PyarrowDatasetIO, PyarrowDatasetHandler`
- [x] Add link to `docs/dataset-handlers.md` for backend differences

### 1.6 Fix `mkdocs.yml` navigation
- [x] Remove duplicate "API Reference" nav block (lines 29-41)
- [x] Ensure single coherent "API Reference" section exists
- [x] Verify "Generated API" subsection is properly nested

### 1.7 Update `docs/api/fsspeckit.datasets.md`
- [x] Replace entire file content with mkdocstrings directive:
  ```markdown
  # `fsspeckit.datasets` API Reference

  ::: fsspeckit.datasets
  ```
- [x] Ensure no manual tables or parameter descriptions remain

### 1.8 Verify documentation build
- [x] Run `mkdocs build` to confirm generation succeeds
- [x] Serve docs locally with `mkdocs serve`
- [x] Check `docs/api/fsspeckit.datasets.md` renders correctly
- [x] Verify navigation is coherent (no duplicate sections)

### 1.9 Cross-check other API pages
- [x] Audit other `docs/api/*.md` files for hand-authored content that should be removed
- [x] Ensure all API pages use consistent mkdocstrings directive pattern

## 2. Approval and Deployment

### 2.1 Request approval
- [x] Share proposal for review before implementing changes
- [x] Address any feedback on design decisions

### 2.2 Archive change
- [x] Move `openspec/changes/refactor-docs-generated-api-pages/` to `openspec/changes/archive/YYYY-MM-DD-refactor-docs-generated-api-pages/`
- [x] Run `openspec validate --strict` after archiving
- [x] Commit and push documentation changes
