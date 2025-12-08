## ADDED Requirements

### Requirement: Document PyArrow merge-aware write parameters

`write_pyarrow_dataset` API documentation SHALL include new merge strategy parameters and their behavior.

#### Scenario: Strategy parameter documentation
- **WHEN** a user views `write_pyarrow_dataset` API documentation
- **THEN** the documentation SHALL include the `strategy` parameter with all valid options ('insert', 'upsert', 'update', 'full_merge', 'deduplicate')
- **AND** SHALL describe when each strategy should be used
- **AND** SHALL document any requirements (like key_columns for relational strategies)

#### Scenario: Key columns parameter documentation
- **WHEN** a user views `write_pyarrow_dataset` API documentation
- **THEN** the documentation SHALL include the `key_columns` parameter
- **AND** SHALL explain its purpose for merge operations
- **AND** SHALL document which strategies require it
- **AND** SHALL show examples of single and composite key usage

### Requirement: Document convenience helper functions

All new convenience helper functions SHALL be documented with clear usage examples.

#### Scenario: Helper function documentation
- **WHEN** a user searches for merge functionality
- **THEN** the documentation SHALL include `insert_dataset`, `upsert_dataset`, `update_dataset`, and `deduplicate_dataset`
- **AND** each function SHALL have clear parameter documentation
- **AND** each function SHALL include practical usage examples
- **AND** each function SHALL document key column requirements