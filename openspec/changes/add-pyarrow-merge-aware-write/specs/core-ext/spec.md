## ADDED Requirements

### Requirement: PyArrow dataset writes support merge strategies

`write_pyarrow_dataset` SHALL accept optional merge strategy arguments and apply them when provided.

#### Scenario: Strategy-aware write
- **WHEN** a caller passes `strategy` (one of `insert`, `upsert`, `update`, `full_merge`, `deduplicate`) and `key_columns` (when required)
- **THEN** `write_pyarrow_dataset` SHALL apply the corresponding merge semantics instead of a plain append/overwrite
- **AND** behaviour without `strategy` remains unchanged.

#### Scenario: Convenience helpers
- **WHEN** a caller invokes filesystem helpers `insert_dataset`, `upsert_dataset`, `update_dataset`, or `deduplicate_dataset`
- **THEN** these helpers SHALL delegate to `write_pyarrow_dataset` with the appropriate `strategy`
- **AND** SHALL validate that required `key_columns` are provided for key-based strategies.
