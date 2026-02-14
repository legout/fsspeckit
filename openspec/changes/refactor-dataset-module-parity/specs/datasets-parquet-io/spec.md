## ADDED Requirements
### Requirement: Dataset handler API parity
The dataset handlers for PyArrow and DuckDB SHALL expose identical public method signatures and defaults for core operations (`write_dataset`, `merge`, `compact_parquet_dataset`, `optimize_parquet_dataset`).

#### Scenario: Core dataset handler signatures align
- **WHEN** inspecting PyarrowDatasetIO and DuckDBDatasetIO core methods
- **THEN** each method SHALL accept the same parameters in the same order
- **AND** SHALL use identical default values for shared parameters
- **AND** backend-specific knobs SHALL be optional and documented separately

### Requirement: Canonical dataset operation results
`write_dataset` and `merge` SHALL return canonical result types with consistent schemas across backends.

#### Scenario: Write and merge return canonical results
- **WHEN** calling `write_dataset` on PyarrowDatasetIO or DuckDBDatasetIO
- **THEN** the result SHALL be a `WriteDatasetResult` with identical fields and semantics
- **AND** the per-file metadata schema SHALL match across backends
- **WHEN** calling `merge` on either backend
- **THEN** the result SHALL be a `MergeResult` with consistent fields and semantics
