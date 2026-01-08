## ADDED Requirements

### Requirement: Getting Started Examples Run Offline by Default

The `examples/datasets/getting_started/` scripts SHALL run successfully in a
clean environment using local temporary data and current dependency APIs.

#### Scenario: PyArrow compatibility
- **WHEN** a user runs a getting-started script using a supported PyArrow version
- **THEN** the script SHALL use supported table construction APIs (for example `pa.Table.from_pylist`)
- **AND** it SHALL not depend on deprecated behaviors that break on upgrade.

#### Scenario: Non-interactive execution
- **WHEN** a user runs the scripts in a non-interactive environment
- **THEN** the scripts SHALL complete without waiting for `input()`
- **AND** optional tutorial pauses SHALL be gated behind an explicit interactive flag.

