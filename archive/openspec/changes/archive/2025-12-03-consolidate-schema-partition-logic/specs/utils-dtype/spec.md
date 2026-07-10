## ADDED Requirements

### Requirement: Partition parsing has a single canonical implementation

Partition parsing utilities used across the project SHALL be implemented in a single canonical helper that is reused by backend-specific helpers.

#### Scenario: Backend helpers rely on the canonical partition parser
- **WHEN** a backend-specific helper (DuckDB or PyArrow) needs to extract partition information from a path
- **THEN** it SHALL call the canonical helper (e.g. `common.partitions.get_partitions_from_path`)
- **AND** SHALL not maintain its own separate parsing logic.
