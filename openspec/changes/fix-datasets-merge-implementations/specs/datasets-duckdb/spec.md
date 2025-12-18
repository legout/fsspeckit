## ADDED Requirements
### Requirement: DuckDB Dataset UPSERT Operations
The DuckDB dataset backend SHALL implement proper UPSERT merge semantics that update existing records and insert new records based on key columns.

#### Scenario: UPSERT with existing and new records
- **GIVEN** a dataset with existing records and source data containing both updates and new records
- **WHEN** merge is called with strategy="upsert" and key_columns=["id"]
- **THEN** existing records with matching keys SHALL be updated with source data
- **AND** new records with non-matching keys SHALL be inserted as new records

#### Scenario: UPSERT with only new records
- **GIVEN** a dataset and source data containing only new records
- **WHEN** merge is called with strategy="upsert"
- **THEN** all source records SHALL be inserted as new records

#### Scenario: UPSERT with only updates
- **GIVEN** a dataset and source data containing only updates to existing records
- **WHEN** merge is called with strategy="upsert"
- **THEN** all source records SHALL update existing records, no new records inserted

### Requirement: DuckDB Dataset UPDATE Operations
The DuckDB dataset backend SHALL implement proper UPDATE merge semantics that only update existing records based on key columns.

#### Scenario: UPDATE with matching records
- **GIVEN** a dataset with existing records and source data containing updates
- **WHEN** merge is called with strategy="update" and key_columns=["id"]
- **THEN** only records with matching keys SHALL be updated
- **AND** records with non-matching keys SHALL remain unchanged

#### Scenario: UPDATE with no matching records
- **GIVEN** a dataset and source data with no matching keys
- **WHEN** merge is called with strategy="update"
- **THEN** no records SHALL be modified
- **AND** appropriate warning SHALL be logged

## MODIFIED Requirements
### Requirement: DuckDB Dataset Merge Implementation
The DuckDB dataset merge method SHALL have fully implemented merge strategies instead of raising NotImplementedError.

#### Scenario: All merge strategies functional
- **GIVEN** a DuckDB dataset handler with properly implemented merge methods
- **WHEN** any merge strategy is called (insert, update, upsert)
- **THEN** the operation SHALL complete successfully without NotImplementedError
- **AND** appropriate merge statistics SHALL be returned

## REMOVED Requirements
### Requirement: Placeholder Merge Methods
Placeholder merge methods that only contain "pass" statements SHALL be removed and replaced with full implementations.

#### Scenario: No placeholder implementations
- **GIVEN** the datasets module after refactoring
- **WHEN** source code is examined
- **THEN** no merge methods SHALL contain only "pass" statements
- **AND** all merge methods SHALL have complete implementations
