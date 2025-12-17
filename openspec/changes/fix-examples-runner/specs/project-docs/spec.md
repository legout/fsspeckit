## ADDED Requirements

### Requirement: Example Runner Supports Non-Interactive Validation

The project SHALL provide an example runner/validator that can execute the
maintained example suite in a non-interactive environment.

#### Scenario: Running a specific example by path
- **WHEN** a user runs `examples/run_examples.py --file <path>`
- **THEN** the runner SHALL accept both relative and absolute paths
- **AND** it SHALL resolve paths relative to the `examples/` directory, not the current working directory.

#### Scenario: CI-friendly operation
- **WHEN** the runner executes the example suite without a TTY
- **THEN** it SHALL not block on interactive prompts
- **AND** it SHALL provide deterministic pass/fail exit codes.

