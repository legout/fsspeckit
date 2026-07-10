## ADDED Requirements

### Requirement: PyArrow helpers reuse shared schema and partition logic

PyArrow-based dataset helpers SHALL delegate schema compatibility and partition handling decisions to shared helper modules instead of maintaining separate implementations.

#### Scenario: Schema unification uses shared helper
- **WHEN** PyArrow-based helpers need to unify schemas across multiple tables or datasets
- **THEN** they SHALL call the shared schema helper (e.g. `common.schema`) for unification
- **AND** SHALL not embed duplicate unification logic locally.

#### Scenario: Partition handling uses shared helper
- **WHEN** PyArrow-based helpers need to reason about partitioned paths
- **THEN** they SHALL use the canonical partition helper (e.g. `common.partitions`)
- **AND** the semantics SHALL match the behaviour documented by that helper.
