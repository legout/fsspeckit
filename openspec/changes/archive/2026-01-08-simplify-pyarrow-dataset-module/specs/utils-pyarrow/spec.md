## MODIFIED Requirements

### Requirement: Merge Parquet Dataset with Strategies (PyArrow)

The system SHALL provide a `merge` method in `PyarrowDatasetIO` that merges a
source table or parquet dataset into a target parquet dataset directory using
PyArrow only, with configurable merge strategies.

#### Scenario: UPSERT with single key column
- **WHEN** user calls `io.merge(source, target_path, key_columns=["id"], strategy="upsert")`
- **AND** `source` contains ids [1, 2, 3] where id=1,2 exist in the target dataset
- **THEN** the helper rewrites the target dataset so that rows for id=1,2 are updated with source values, id=3 is inserted, and all other rows are preserved.


