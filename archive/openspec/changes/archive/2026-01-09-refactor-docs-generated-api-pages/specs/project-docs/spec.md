## MODIFIED Requirements

### Requirement: API Reference Pages Use mkdocstrings Directives

The `docs/api/*` pages SHALL contain only mkdocstrings directives and SHALL NOT include hand-authored API tables or parameter descriptions. All API documentation must be generated from docstrings to ensure consistency with code implementation and prevent documentation drift.

#### Scenario: Users see accurate API docs

- **WHEN** a user navigates to `docs/api/fsspeckit.datasets.md` or any generated API page
- **THEN** the page contains only a mkdocstrings directive (e.g., `::: fsspeckit.datasets`)
- **AND** the page does not contain manually maintained parameter tables or descriptions
- **AND** documentation matches actual code signatures in `src/`

#### Scenario: MkDocs generates consistent API pages

- **WHEN** mkdocstrings runs to generate `docs/api/*` pages
- **THEN** all generated pages use the same pattern (module directive only)
- **AND** no generated page contains custom Markdown tables or hand-authored content
- **AND** generated content can be rebuilt by running mkdocs without manual editing

#### Scenario: API docs sync with code changes

- **WHEN** a developer modifies a class method signature in `src/`
- **THEN** running `mkdocs build` automatically updates `docs/api/*` pages
- **AND** no manual editing of API pages is required to reflect changes
- **AND** generated pages cannot drift from code implementation

### Requirement: Duplicate API Reference Navigation Removed

MkDocs configuration SHALL contain exactly one "API Reference" navigation section to prevent user confusion and ensure coherent documentation structure.

#### Scenario: Users navigate API Reference

- **WHEN** a user opens the MkDocs navigation
- **THEN** they see a single "API Reference" section
- **AND** the section does not contain duplicate subsections
- **AND** navigation hierarchy is clear and predictable

#### Scenario: Generated API pages are properly nested

- **WHEN** a user navigates to "Generated API" subsection
- **THEN** all generated pages (domain packages, core, storage options, utils) are nested under a single "Generated API" parent
- **AND** there are no duplicate "API Reference" entries at different nav levels

### Requirement: API Index References Current Dataset Handlers

The `docs/api/index.md` migration guidance SHALL reference only current dataset handler classes (DuckDBDatasetIO, PyarrowDatasetIO) and SHALL NOT mention removed APIs (DuckDBParquetHandler, legacy helper functions).

#### Scenario: Users see current import paths

- **WHEN** a user reads `docs/api/index.md` migration guidance
- **THEN** recommended imports are:
  - `from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection`
  - `from fsspeckit.datasets.pyarrow import PyarrowDatasetIO, PyarrowDatasetHandler`
- **AND** guidance does not recommend removed APIs like `DuckDBParquetHandler`
- **AND** all code examples use current handler classes

#### Scenario: Migration tips point to current docs

- **WHEN** a user follows migration tips in `docs/api/index.md`
- **THEN** tips point to `docs/dataset-handlers.md` for backend differences
- **AND** tips reference generated API pages (e.g., `docs/api/fsspeckit.datasets.md`) for detailed signatures
- **AND** no tips suggest using deprecated imports or helper functions
