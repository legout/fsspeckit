## MODIFIED Requirements

### Requirement: DuckDB helpers share cleanup behaviour

DuckDB helper modules SHALL share a single canonical implementation for unregistering DuckDB tables safely.

#### Scenario: DuckDB cleanup uses a central helper
- **WHEN** cleanup code in DuckDB-related modules unregisters DuckDB tables
- **THEN** it SHALL call a shared `_unregister_duckdb_table_safely` helper from a canonical DuckDB helpers module
- **AND** no module SHALL maintain its own divergent copy of this logic.

