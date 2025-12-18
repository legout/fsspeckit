## MODIFIED Requirements
### Requirement: Multi-Column Key Vectorization
Multi-column key operations SHALL use vectorized PyArrow operations instead of Python loops to maintain performance parity with single-column key operations.

#### Scenario: Vectorized multi-column key deduplication
- **GIVEN** a dataset with composite key [tenant_id, record_id]
- **WHEN** deduplication is performed with key_columns=["tenant_id", "record_id"]
- **THEN** all key operations SHALL use PyArrow vectorized functions
- **AND** no to_pylist() conversions SHALL occur in the key matching path
- **AND** performance SHALL be within 2x of single-column key operations

#### Scenario: Vectorized multi-column key merge
- **GIVEN** source and target datasets with composite keys
- **WHEN** merge operations (upsert, update, insert) are performed
- **THEN** key matching SHALL use PyArrow join operations
- **AND** set membership checking SHALL use native Arrow operations
- **AND** performance SHALL scale linearly with data size

## ADDED Requirements
### Requirement: Composite Key Helper Functions
The PyArrow backend SHALL provide helper functions for efficient composite key operations.

#### Scenario: Create composite key array
- **GIVEN** a table with multiple key columns
- **WHEN** _create_composite_key_array() is called
- **THEN** a single StructArray representing the composite key SHALL be returned
- **AND** the array SHALL support efficient comparison operations

#### Scenario: Filter by key membership
- **GIVEN** a table and a reference set of keys
- **WHEN** _filter_by_key_membership() is called
- **THEN** the table SHALL be filtered using PyArrow join operations
- **AND** both inclusion and exclusion modes SHALL be supported
