## ADDED Requirements

### Requirement: DuckDB helpers reuse shared schema and partition logic

DuckDB-based helpers SHALL reuse the same shared schema and partition helpers as the PyArrow backend to ensure consistent behaviour.

#### Scenario: Shared schema compatibility rules across backends
- **WHEN** DuckDB helpers need to check or reconcile schemas
- **THEN** they SHALL call the shared schema helper
- **AND** SHALL honour the same compatibility rules as the PyArrow backend.

#### Scenario: Shared partition semantics across backends
- **WHEN** DuckDB helpers operate on partitioned datasets
- **THEN** they SHALL interpret partition paths according to the shared partition helper’s semantics
- **AND** SHALL not diverge from the behaviour used by PyArrow helpers.
