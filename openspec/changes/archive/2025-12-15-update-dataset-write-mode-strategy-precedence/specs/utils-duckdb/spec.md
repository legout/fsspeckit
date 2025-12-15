## ADDED Requirements

### Requirement: `mode` is Ignored When `strategy` is Provided (DuckDB)
The system SHALL treat `strategy` as the primary control for `write_parquet_dataset` behavior and SHALL ignore `mode` when `strategy` is not `None`.

#### Scenario: `mode="append"` does not affect upsert
- **WHEN** user calls `handler.write_parquet_dataset(table, path, strategy="upsert", key_columns=["id"], mode="append")`
- **THEN** the method SHALL perform the UPSERT semantics
- **AND** SHALL NOT raise an error due to the presence of `mode`

#### Scenario: `mode="overwrite"` does not affect update
- **WHEN** user calls `handler.write_parquet_dataset(table, path, strategy="update", key_columns=["id"], mode="overwrite")`
- **THEN** the method SHALL perform the UPDATE semantics
- **AND** SHALL NOT raise an error due to the presence of `mode`

