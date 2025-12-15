## ADDED Requirements

### Requirement: `mode` is Ignored When `strategy` is Provided (PyArrow)
The system SHALL treat `strategy` as the primary control for `write_parquet_dataset` behavior and SHALL ignore `mode` when `strategy` is not `None`.

#### Scenario: `mode="append"` does not affect upsert
- **WHEN** user calls `io.write_parquet_dataset(table, path, strategy="upsert", key_columns=["id"], mode="append")`
- **THEN** the method SHALL perform the UPSERT semantics
- **AND** SHALL NOT raise an error due to the presence of `mode`

