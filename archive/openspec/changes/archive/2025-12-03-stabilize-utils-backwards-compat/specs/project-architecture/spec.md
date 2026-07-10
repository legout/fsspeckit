## ADDED Requirements

### Requirement: Backwards-Compatible Utils Façade (extended)

The `fsspeckit.utils` package SHALL continue to act as a backwards-compatible façade while explicitly defining which imports are supported and how they map to domain packages.

#### Scenario: Legacy utils imports remain supported
- **WHEN** existing user code imports helpers via `fsspeckit.utils` (including common patterns used in tests)
- **THEN** those imports SHALL continue to succeed and refer to the same objects as the canonical implementation in `datasets`, `sql`, or `common`.

#### Scenario: Shim modules preserve deeper import paths
- **WHEN** code imports names via deeper paths such as `fsspeckit.utils.misc.Progress`
- **THEN** small shim modules under `fsspeckit.utils` SHALL re-export those names from the canonical locations
- **AND** behaviour SHALL remain consistent across at least one major version to allow migration.

#### Scenario: New code avoids utils implementation
- **WHEN** new features are added to the library
- **THEN** their implementation modules SHALL live under the appropriate domain packages
- **AND** any exposure through `fsspeckit.utils` SHALL be limited to re-exports.
