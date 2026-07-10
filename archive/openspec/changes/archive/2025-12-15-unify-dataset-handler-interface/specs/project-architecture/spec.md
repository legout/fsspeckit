## ADDED Requirements

### Requirement: Shared dataset handler surface

Dataset handlers across backends SHALL share a clearly documented core surface (method names and key parameters) to
provide a consistent user experience.

#### Scenario: Common method naming across handlers
- **WHEN** comparing DuckDB and PyArrow dataset handlers
- **THEN** core operations (read, write, merge, compact/optimize where supported) SHALL use consistent method names and parameter conventions
- **AND** any backend-specific differences SHALL be explicitly documented.

#### Scenario: Optional protocol for tooling
- **WHEN** static analysis or editor tooling is used
- **THEN** a minimal protocol or type annotation MAY be provided to describe the shared handler surface
- **AND** handlers SHALL satisfy this protocol for the overlapping capabilities.
