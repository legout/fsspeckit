## ADDED Requirements

### Requirement: Consistent import path documentation

The documentation SHALL use consistent import patterns that prefer the shortest valid path while remaining clear about package organization.

#### Scenario: Core symbols use top-level imports
- **WHEN** documentation demonstrates filesystem or storage options usage
- **THEN** examples SHALL prefer `from fsspeckit import filesystem, AwsStorageOptions`
- **AND** SHALL NOT show multiple alternative import paths for the same symbol

#### Scenario: Dataset handlers use package imports
- **WHEN** documentation demonstrates dataset operations
- **THEN** examples SHALL use `from fsspeckit.datasets import PyarrowDatasetIO, DuckDBParquetHandler`
- **AND** SHALL show consistent patterns across all how-to guides and tutorials

#### Scenario: SQL utilities use module imports
- **WHEN** documentation demonstrates SQL filter translation
- **THEN** examples SHALL use `from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter`
- **AND** SHALL be consistent across all documentation files

### Requirement: Import hierarchy documentation

The documentation SHALL include a brief section explaining the import path hierarchy for discoverability.

#### Scenario: Import patterns section exists
- **WHEN** a user reads `docs/explanation/concepts.md`
- **THEN** they SHALL find a section explaining import patterns:
  - Top-level: `from fsspeckit import ...` for core symbols (filesystem, storage options)
  - Package-level: `from fsspeckit.datasets import ...` for dataset handlers
  - Module-level: `from fsspeckit.sql.filters import ...` for specific utilities

#### Scenario: No deprecated import comments
- **WHEN** a user reads documentation examples
- **THEN** they SHALL NOT find "Legacy import (still works but deprecated)" comments
- **AND** SHALL NOT find multiple alternative import paths shown for the same operation
