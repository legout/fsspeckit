## MODIFIED Requirements

### Requirement: Guides Reflect Current Dataset Handler APIs

Getting started and API guides SHALL demonstrate using current dataset handler classes (`DuckDBDatasetIO`, `PyarrowDatasetIO`, `PyarrowDatasetHandler`) and SHALL NOT reference removed APIs like `DuckDBParquetHandler` or legacy merge methods.

#### Scenario: Quickstart guide uses current handlers

- **WHEN** a user follows examples in `docs/getting-started.md` or `docs/quickstart.md`
- **THEN** code samples import from `fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection`
- **AND** samples import from `fsspeckit.datasets.pyarrow import PyarrowDatasetIO, PyarrowDatasetHandler`
- **AND** examples do not reference `DuckDBParquetHandler` or other removed APIs
- **AND** samples show proper handler instantiation (connection setup for DuckDB)

#### Scenario: API guide points to correct entry points

- **WHEN** a user reads `docs/api-guide.md`
- **THEN** guide describes using `DuckDBDatasetIO` and `PyarrowDatasetIO` classes
- **AND** guide recommends checking `docs/dataset-handlers.md` for backend differences
- **AND** guide points to generated API pages (`docs/api/fsspeckit.datasets.md`) for complete signatures
- **AND** guide does not encourage usage of removed `fsspeckit.utils` dataset helpers

### Requirement: Dataset Filter Type Documentation

Documentation SHALL explicitly document filter type differences between DuckDB and PyArrow backends to guide users on correct filter usage.

#### Scenario: Users understand DuckDB filter requirements

- **WHEN** a user reads dataset documentation for DuckDB
- **THEN** they understand `filters` parameter requires SQL WHERE clause strings (`str | None`)
- **AND** documentation shows runtime validation error if non-string filter is provided
- **AND** examples demonstrate correct SQL filter usage (e.g., `filters="column > 5"`)

#### Scenario: Users understand PyArrow filter capabilities

- **WHEN** a user reads dataset documentation for PyArrow
- **THEN** they understand `filters` parameter accepts multiple types (PyArrow expressions, DNF tuples, SQL-like strings)
- **AND** documentation notes SQL-like strings are converted to PyArrow expressions
- **AND** examples show different filter types
- **AND** documentation contrasts with DuckDB (str-only requirement)

#### Scenario: Backend differences matrix is accessible

- **WHEN** a user needs to understand backend differences
- **THEN** they can find filter types row in `docs/dataset-handlers.md` backend differences matrix
- **AND** matrix includes row for filter types (DuckDB: `str` vs PyArrow: `Any`)
- **AND** how-to pages link to this matrix when explaining backend-specific behavior

### Requirement: Backend-Specific Parameters Documentation

Documentation SHALL clearly identify which parameters are backend-specific (DuckDB `use_merge`, PyArrow streaming knobs) and shall not imply cross-backend availability.

#### Scenario: Users identify DuckDB-only features

- **WHEN** a user reads DuckDB merge documentation
- **THEN** documentation marks `use_merge` parameter as DuckDB-specific
- **AND** documentation explains MERGE SQL vs UNION ALL fallback behavior
- **AND** examples do not show `use_merge` in PyArrow context
- **AND** `docs/dataset-handlers.md` backend differences table lists `use_merge` under DuckDB-only column

#### Scenario: Users identify PyArrow-only features

- **WHEN** a user reads PyArrow merge documentation
- **THEN** documentation shows PyArrow-specific knobs (e.g., `merge_chunk_size_rows`, `enable_streaming_merge`, `merge_max_memory_mb`)
- **AND** documentation explains streaming merge vs in-memory merge
- **AND** examples do not show PyArrow knobs in DuckDB context
- **AND** `docs/dataset-handlers.md` backend differences table lists these under PyArrow-only column

### Requirement: Narrative Docs Reference Generated API Pages

How-to and reference guides SHALL reference generated API pages (`docs/api/fsspeckit.datasets.md`) for complete signatures and SHALL NOT duplicate parameter tables in narrative content.

#### Scenario: How-to guide references generated API

- **WHEN** a user reads `docs/how-to/merge-datasets.md` or other how-to guides
- **THEN** examples focus on usage patterns and decision-making
- **AND** guide links to `docs/api/fsspeckit.datasets.md` or `docs/api/fsspeckit.core.merge.md` for complete parameter lists
- **AND** guide does not maintain its own copy of API signatures
- **AND** generated API page serves as the canonical signature reference

#### Scenario: Reference guide points to generated pages

- **WHEN** a user reads `docs/reference/api-guide.md`
- **THEN** guide directs users to generated API pages for method signatures
- **AND** guide explains that API pages are auto-generated from docstrings
- **AND** guide focuses on when to use which backend vs listing all parameters
- **AND** navigation from `docs/api/index.md` provides entry point to generated pages
