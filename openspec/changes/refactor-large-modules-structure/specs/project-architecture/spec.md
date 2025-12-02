## MODIFIED Requirements

### Requirement: Large modules are decomposed into focused submodules

The system SHALL avoid monolithic modules with many unrelated responsibilities and instead organise functionality into focused submodules under each domain package.

#### Scenario: IO helpers split by format and responsibility
- **WHEN** inspecting the `fsspeckit.core` package
- **THEN** JSON, CSV, and Parquet helpers SHALL reside in clearly named submodules (or sections) according to their format
- **AND** the wiring layer that attaches these helpers to `AbstractFileSystem` SHALL be clearly separated from the helper implementations.

#### Scenario: Dataset modules separate schema logic from dataset operations
- **WHEN** inspecting `fsspeckit.datasets.pyarrow` and `fsspeckit.datasets.duckdb` implementations
- **THEN** schema/type inference and unification logic SHALL be factored into dedicated helpers
- **AND** dataset merge/maintenance operations SHALL delegate to the shared core and reuse those helpers instead of embedding all logic in the top-level dataset modules.

### Requirement: Public entrypoints are stable, internal structure is modular

The system SHALL preserve stable public import paths at the package level while allowing internal module structure to evolve towards smaller, focused units.

#### Scenario: Existing imports remain valid after refactor
- **WHEN** user code imports public helpers from `fsspeckit`, `fsspeckit.core`, or `fsspeckit.datasets`
- **THEN** those imports SHALL continue to work after the refactor
- **AND** any internal restructuring SHALL be reflected through re-exports or thin entrypoint modules so that external code does not need to change.

