## ADDED Requirements

### Requirement: Common and SQL Examples Match Public API Surface

The examples under `examples/common/` and `examples/sql/` SHALL use only public,
currently supported APIs and demonstrate correct usage patterns.

#### Scenario: Logging examples use supported logging helpers
- **WHEN** a user runs the logging example(s)
- **THEN** they SHALL configure logging using `fsspeckit.common.logging.setup_logging`
- **AND** obtain a logger via `fsspeckit.common.logging.get_logger` (or equivalent supported pattern).

#### Scenario: SQL filter examples supply required schema
- **WHEN** a user runs SQL filter examples that call `sql2pyarrow_filter` or `sql2polars_filter`
- **THEN** the examples SHALL provide the schema argument required by the current function signatures
- **AND** the examples SHALL run offline against a locally created dataset.

