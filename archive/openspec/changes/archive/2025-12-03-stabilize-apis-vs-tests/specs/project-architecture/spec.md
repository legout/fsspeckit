## ADDED Requirements

### Requirement: Helper APIs align with test expectations

Helper functions that are covered by tests SHALL preserve the semantics and key error messages asserted in those tests, except where a clear, documented behavioural change is explicitly approved.

#### Scenario: `run_parallel` supports generator input
- **WHEN** a caller passes a generator as the primary iterable argument to `run_parallel`
- **THEN** the function SHALL treat the generator as a valid iterable, materialising it as needed
- **AND** SHALL produce results consistent with passing an equivalent list.

#### Scenario: Error messages are stable for key error cases
- **WHEN** `run_parallel` raises due to mismatched iterable lengths or missing iterable arguments
- **THEN** the error messages SHALL include wording that satisfies the existing tests’ `match=` assertions
- **AND** any changes to these messages SHALL be accompanied by corresponding spec/test updates.

### Requirement: Avoid mutable defaults and dead code in core helpers

Core helper functions SHALL avoid mutable default arguments and unreachable code branches that cannot be exercised.

#### Scenario: No mutable default arguments in core helpers
- **WHEN** reviewing core helper function definitions
- **THEN** default values for parameters SHALL be immutable or `None`
- **AND** any required mutable objects SHALL be created inside the function body.

#### Scenario: Unreachable code eliminated
- **WHEN** analysing core helper functions for control flow
- **THEN** there SHALL be no branches that can never be executed in practice
- **AND** no branches SHALL refer to variables that are not guaranteed to be defined along all paths.
