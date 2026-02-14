## ADDED Requirements
### Requirement: Dataset backend dependency guards
Dataset backend handlers SHALL validate required optional dependencies and raise actionable ImportError messages before execution.

#### Scenario: DuckDB handler reports missing dependency
- **WHEN** a user instantiates or calls `DuckDBDatasetIO` without DuckDB installed
- **THEN** the handler SHALL raise ImportError immediately
- **AND** the error message SHALL recommend installing `fsspeckit[sql]` or `fsspeckit[datasets]`

#### Scenario: PyArrow handler reports missing dependency
- **WHEN** a user instantiates or calls `PyarrowDatasetIO` without PyArrow installed
- **THEN** the handler SHALL raise ImportError immediately
- **AND** the error message SHALL recommend installing `fsspeckit[datasets]`
