## ADDED Requirements
### Requirement: Documentation Mirrors Implemented Capabilities
The documentation SHALL describe only functionality that exists in the source tree and SHALL explicitly avoid or fence off unimplemented systems (e.g., PerformanceTracker, cache sizing metrics, plugin registries, Delta Lake/Pydala helpers).

#### Scenario: Architecture and Advanced docs avoid fictitious components
- **WHEN** a reader opens `docs/architecture.md` or `docs/advanced.md`
- **THEN** the pages do not claim features absent from `src/` (for example PerformanceTracker, cache size APIs, plugin registration, Delta Lake write helpers)
- **AND** caching coverage is limited to the implemented `MonitoredSimpleCacheFileSystem` behavior (path-preserving cache and verbose logging).

#### Scenario: Examples exclude unsupported helpers
- **WHEN** a user follows examples in `docs/advanced.md` or `docs/examples.md`
- **THEN** only shipped APIs are referenced (for example `DuckDBParquetHandler`, `filesystem`, storage option factories)
- **AND** workflows that rely on missing functions are either removed or clearly marked as out-of-scope/future work.

### Requirement: API Reference Prioritizes Domain Packages
The API reference navigation and generated pages SHALL cover the domain packages (`core`, `storage_options`, `datasets`, `sql`, `common`) and SHALL mark `fsspeckit.utils` as a compatibility façade rather than the primary surface.

#### Scenario: MkDocs nav lists domain modules
- **WHEN** the MkDocs navigation renders API Reference
- **THEN** it includes entries for domain modules such as `fsspeckit.core`, `fsspeckit.storage_options`, `fsspeckit.datasets`, `fsspeckit.sql.filters`, and `fsspeckit.common`
- **AND** any `fsspeckit.utils.*` entries are labeled as “legacy” or “compatibility” and not presented as the main API.

### Requirement: Guides Use Accurate Imports and Path Safety Guidance
Getting started and API guides SHALL demonstrate recommended imports from domain packages and SHALL describe the path-safety semantics of `filesystem` / `DirFileSystem` without suggesting deprecated helpers.

#### Scenario: Quickstart/API guide use domain imports
- **WHEN** a user reads `docs/quickstart.md` or `docs/api-guide.md`
- **THEN** code samples import from `fsspeckit` or domain modules (`fsspeckit.storage_options`, `fsspeckit.datasets`, `fsspeckit.sql.filters`, `fsspeckit.common`)
- **AND** they do not recommend new usage via `fsspeckit.utils`.

#### Scenario: Path-safety guidance is documented
- **WHEN** the guides explain filesystem creation
- **THEN** they note DirFileSystem base-path enforcement and path normalization behavior from `src/fsspeckit/core/filesystem.py`
- **AND** examples avoid functions that do not exist (e.g., `get_cache_size`) while accurately describing `cached=True` and `verbose` semantics.
