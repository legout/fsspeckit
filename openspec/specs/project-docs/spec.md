# project-docs Specification

## Purpose
TBD - created by archiving change update-docs-fsspeckit-branding. Update Purpose after archive.
## Requirements
### Requirement: Canonical Package Naming in Public Docs

The documentation SHALL consistently refer to the library as `fsspeckit` and
MUST NOT reference the historical `fsspec-utils` name in user-facing
installation and contribution guides.

#### Scenario: Installation docs use fsspeckit name

- **WHEN** a user reads `docs/installation.md` or the README to install the
  library
- **THEN** all commands use `fsspeckit` (for example `pip install fsspeckit`)
- **AND** no examples mention `fsspec-utils`.

#### Scenario: Contributing docs use fsspeckit name

- **WHEN** a contributor reads `docs/contributing.md`
- **THEN** the project name and issue template instructions refer to
  `fsspeckit`
- **AND** version references use the `fsspeckit` package name.

### Requirement: Installation Docs Match Published Extras

The installation documentation SHALL describe optional extras and supported
Python versions that match `pyproject.toml`.

#### Scenario: Extras match pyproject configuration

- **WHEN** a user reads the "Optional Cloud Provider Support" section in
  `docs/installation.md`
- **THEN** the documented extras include `aws`, `gcp`, and `azure`
- **AND** example commands match the extras in `pyproject.toml` (for example
  `pip install "fsspeckit[aws,gcp,azure]"`).

#### Scenario: Python version requirement is accurate

- **WHEN** a user checks the prerequisites for installing `fsspeckit`
- **THEN** the docs state Python 3.11+ to match `requires-python` in
  `pyproject.toml`.

#### Scenario: DuckDB version requirement is accurate

- **WHEN** a user checks the DuckDB version prerequisites for `fsspeckit`
- **THEN** the documentation states `duckdb>=1.4.0` to match the actual
  dependency requirement in `pyproject.toml` for the `datasets` and `sql` extras
- **AND** the version reflects the need for native MERGE statement support
  introduced in DuckDB 1.4.0.

### Requirement: API Docs Reflect Current Package Namespace

API and logging documentation that comes from docstrings SHALL describe the
`fsspeckit` package namespace and supported use-cases, not the historical
`fsspec-utils` name.

#### Scenario: Logging helper docstrings use fsspeckit

- **WHEN** a user opens the logging API docs
  (`docs/api/fsspeckit.utils.logging.md` or generated equivalent)
- **THEN** descriptions and parameter docs refer to configuring logging for the
  `fsspeckit` package
- **AND** no messages suggest installing or configuring `fsspec-utils`.

#### Scenario: Top-level package docstring uses fsspeckit

- **WHEN** a user inspects `fsspeckit.__doc__` or generated API docs for the
  root package
- **THEN** the description identifies the package as `fsspeckit`
- **AND** describes its purpose without mentioning `fsspec-utils`.

### Requirement: Example Docs Align with Package Branding

Example READMEs and top-of-file comments that are intended as documentation SHALL
use the `fsspeckit` name and install commands.

#### Scenario: Example READMEs use fsspeckit

- **WHEN** a user reads example READMEs under `examples/`
- **THEN** titles and prerequisite sections reference `fsspeckit`
- **AND** suggested install commands use `pip install fsspeckit` (or extras)
  instead of `fsspec-utils`.

#### Scenario: Example script headers use fsspeckit

- **WHEN** a user opens example Python scripts (for example
  `examples/caching/caching_example.py`)
- **THEN** module-level docstrings and comments mention `fsspeckit`
- **AND** narrative text stays consistent with the rest of the documentation.

### Requirement: Comprehensive Architecture Documentation

The project SHALL provide comprehensive architecture documentation that serves as both a technical reference and practical implementation guide for fsspeckit users.

#### Scenario: Users understand system design decisions

- **WHEN** a developer reads `docs/architecture.md`
- **THEN** they find Architectural Decision Records (ADRs) explaining WHY key design choices were made
- **AND** the documentation includes real implementation patterns rather than idealized descriptions
- **AND** cross-domain integration examples show how packages work together in practice.

#### Scenario: Developers implement solutions with fsspeckit

- **WHEN** a user needs to implement a data processing solution
- **THEN** the architecture documentation provides concrete integration patterns between domain packages
- **AND** includes performance optimization strategies and deployment considerations
- **AND** shows extension points for custom development.

#### Scenario: Operators deploy fsspeckit in production

- **WHEN** a DevOps engineer reviews the architecture for production deployment
- **THEN** the documentation includes multi-cloud deployment patterns
- **AND** covers security, compliance, and operational excellence practices
- **AND** provides monitoring and observability architecture guidance.

### Requirement: Architectural Decision Records

The architecture documentation SHALL include formal Architectural Decision Records (ADRs) that document critical design choices with rationale and alternatives considered.

#### Scenario: Understanding domain package architecture

- **WHEN** a reader questions why fsspeckit uses a domain package structure
- **THEN** ADR-001 documents the decision with trade-offs and alternatives considered
- **AND** explains the benefits over monolithic or other organizational approaches.

#### Scenario: Evaluating backend-neutral planning

- **WHEN** a developer examines the merge and maintenance planning architecture
- **THEN** ADR-002 documents the centralized vs. decentralized design decision
- **AND** explains why backend-neutral planning was placed in the core package.

#### Scenario: Understanding backwards compatibility strategy

- **WHEN** a user questions the utils façade pattern
- **THEN** ADR-003 documents the migration strategy and reasoning
- **AND** explains the gradual transition path for existing users.

### Requirement: Real Implementation Documentation

The architecture documentation SHALL accurately reflect the actual implementation rather than idealized or outdated descriptions.

#### Scenario: Package interaction understanding

- **WHEN** a developer examines how packages integrate
- **THEN** the documentation shows actual data flow and component interactions
- **AND** includes real code examples from the current implementation
- **AND** documents error handling patterns and resilience strategies.

#### Scenario: Performance optimization

- **WHEN** a user needs to optimize fsspeckit performance
- **THEN** the architecture documentation documents actual caching strategies
- **AND** includes parallel processing patterns with real performance characteristics
- **AND** provides monitoring and observability implementation details.

### Requirement: Visual Architecture Documentation

The architecture documentation SHALL include comprehensive visual diagrams that accurately represent the system structure and data flow.

#### Scenario: High-level architecture understanding

- **WHEN** a stakeholder needs a quick overview
- **THEN** updated Mermaid diagrams show the actual domain package structure
- **AND** component interaction diagrams illustrate real integration patterns
- **AND** deployment architecture diagrams show production configurations.

#### Scenario: Implementation detail understanding

- **WHEN** a developer examines specific integration patterns
- **THEN** detailed data flow diagrams show how packages interact in practice
- **AND** component lifecycle diagrams illustrate initialization and shutdown patterns
- **AND** extension point diagrams show where custom code can be integrated.

### Requirement: Cross-Domain Integration Examples

The architecture documentation SHALL include practical examples of how different fsspeckit packages work together to solve real problems.

#### Scenario: Learning integration patterns

- **WHEN** a developer needs to combine multiple packages
- **THEN** the documentation provides end-to-end pipeline examples
- **AND** shows cross-domain error handling and retry patterns
- **AND** demonstrates configuration and deployment best practices.

#### Scenario: Production deployment patterns

- **WHEN** an organization deploys fsspeckit at scale
- **THEN** architecture examples show multi-cloud deployment strategies
- **AND** include security and compliance implementation patterns
- **AND** demonstrate monitoring and operational excellence practices.

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

### Requirement: Document Domain Package Layout

The documentation SHALL describe the domain package layout defined by the
`project-architecture` specification and provide canonical import examples for
each major package.

#### Scenario: README and index describe packages

- **WHEN** a user reads `README.md` or `docs/index.md`
- **THEN** they see a brief explanation of `fsspeckit.core`,
  `fsspeckit.storage_options`, `fsspeckit.datasets`, `fsspeckit.sql`,
  `fsspeckit.common`, and `fsspeckit.utils`
- **AND** each package has at least one example import that matches the
  current code layout.

### Requirement: Docs Prefer Canonical Imports After Refactor

The documentation SHALL use canonical import paths that reflect the refactored
package layout, while clearly marking `fsspeckit.utils` as a
backwards-compatible façade rather than the primary entrypoint for new code.

#### Scenario: Examples use datasets, sql, and common packages

- **WHEN** a user reads examples in `README.md`, `docs/utils.md`,
  `docs/api-guide.md`, or the `examples/` directory
- **THEN** dataset helpers are imported from `fsspeckit.datasets.*`,
  SQL helpers are imported from `fsspeckit.sql.*`, and cross-cutting
  utilities are imported from `fsspeckit.common.*` (or top-level imports)
- **AND** any remaining `fsspeckit.utils` imports are explicitly described as
  compatibility paths.

### Requirement: Utils Documentation Matches Façade Role

The "Utils" documentation SHALL present `fsspeckit.utils` as a façade that
re-exports helpers from domain packages, and SHALL point users to the
underlying canonical modules.

#### Scenario: Utils page explains façade and targets

- **WHEN** a user opens `docs/utils.md`
- **THEN** the introduction explains that `fsspeckit.utils` exposes selected
  helpers from `fsspeckit.common`, `fsspeckit.datasets`, and
  `fsspeckit.sql`
- **AND** each documented helper mentions its canonical module
  (for example, `DuckDBParquetHandler` from `fsspeckit.datasets.duckdb`,
  `sql2pyarrow_filter` from `fsspeckit.sql.filters`).

### Requirement: API Reference Covers New Domain Packages

The API reference SHALL include sections for the `common`, `datasets`, and
`sql` packages and SHALL align headings and links with the refactored
module names.

#### Scenario: mkdocs API nav includes domain packages

- **WHEN** a user navigates the API reference via `docs/api/index.md` and
  the MkDocs navigation
- **THEN** they see entries for `fsspeckit.common.*`, `fsspeckit.datasets.*`,
  and `fsspeckit.sql.*` alongside any remaining `fsspeckit.utils.*` entries
- **AND** the content for each entry references the correct module paths in
  code examples and headings.

### Requirement: Security and Error Handling Documentation

The documentation SHALL describe how fsspeckit protects against common security issues and SHALL include guidance on safe usage patterns for production deployments.

#### Scenario: Security helpers are documented

- **WHEN** a developer needs to implement secure data processing workflows
- **THEN** the documentation includes a dedicated "Security and Error Handling" section
- **AND** it describes the `fsspeckit.common.security` module helpers: `validate_path`, `validate_compression_codec`, `scrub_credentials`, `scrub_exception`, `safe_format_error`, and `validate_columns`
- **AND** shows realistic examples of using these helpers with DuckDB/PyArrow dataset operations and storage options.

#### Scenario: Production security patterns

- **WHEN** an operator reviews the documentation for security best practices
- **THEN** the architecture documentation references security helpers as part of the production deployment story
- **AND** covers monitoring, observability, and security considerations
- **AND** includes guidance on preventing credential leakage in logs and avoiding unsafe compression codecs.

### Requirement: Versioned Migration Guides

The project SHALL provide versioned migration guides for significant refactors and architectural changes, and these guides SHALL be discoverable from the homepage and architecture documentation.

#### Scenario: Migration guide exists for major refactors

- **WHEN** a user upgrades from one major version to another
- **THEN** they can find a versioned migration guide (e.g., `docs/migration-0.5.md`)
- **AND** the guide documents migration from pre–domain package layouts to the current architecture
- **AND** covers changes to logging, error handling, dataset maintenance, and optional dependencies.

#### Scenario: Migration guide is discoverable

- **WHEN** a user needs to understand migration steps
- **THEN** the migration guide is linked from `README.md`, `docs/index.md`, and `docs/architecture.md`
- **AND** the guide is easily discoverable through clear navigation and cross-references.

### Requirement: Documentation Layering and Roles

The documentation SHALL clearly separate tutorial, how-to, reference, and architecture content, and SHALL avoid duplication between layers while maintaining accuracy and consistency.

#### Scenario: Documentation follows layered roles

- **WHEN** a user reads different types of documentation
- **THEN** quickstart/tutorial docs focus on end-to-end "getting started" patterns
- **AND** API guide/how-to docs focus on task-oriented, practical patterns
- **AND** reference/API pages are generated from code and do not introduce novel behaviors
- **AND** architecture docs focus on design rationale and high-level patterns.

#### Scenario: Documentation avoids duplication

- **WHEN** a user reads multiple guides
- **THEN** detailed API behavior is covered in reference, not duplicated in narrative guides
- **AND** overlapping sections point to canonical examples instead of duplicating code
- **AND** cross-references guide users to the most appropriate section for their needs.

### Requirement: Navigation Exposes Documentation Roles

The documentation navigation and homepage SHALL make the primary documentation roles (tutorials, how-to guides, reference, and architecture/explanation) explicit so that users can quickly choose the right entrypoint for their current task.

#### Scenario: Homepage routes by documentation role

- **WHEN** a user opens the documentation landing page (`docs/index.md`)
- **THEN** they see clearly labeled links to at least:
  - A getting started/tutorial entrypoint
  - A collection of how-to guides
  - API/reference documentation
  - Architecture and/or conceptual explanations
- **AND** the page briefly describes the purpose of each link so users understand which one to choose.

#### Scenario: MkDocs navigation reflects documentation roles

- **WHEN** a user opens the documentation site navigation
- **THEN** tutorials, how-to guides, reference, and explanation/architecture content are grouped under distinguishable headings
- **AND** the number of top-level navigation items is kept small and descriptive
- **AND** domain package API reference pages remain organized under an API Reference section that treats `fsspeckit.utils` as a compatibility façade.

### Requirement: File Layout Aligns with Documentation Roles

The documentation file layout SHALL align with the defined documentation roles (tutorials, how-to guides, reference, and explanation/architecture) to reduce ambiguity and duplication, using consistent directory names where practical, while still allowing future evolution of the structure.

#### Scenario: Tutorials live under a dedicated path

- **WHEN** a contributor adds or updates a tutorial
- **THEN** the tutorial pages live under a dedicated tutorial-oriented path (for example `docs/tutorials/`)
- **AND** the navigation entry for those pages is clearly labeled as a tutorial or getting started guide.

#### Scenario: How-to guides are grouped by task

- **WHEN** a contributor adds or updates a task-focused how-to guide
- **THEN** the pages are grouped under a how-to-oriented path (for example `docs/how-to/`) or an equivalent logical grouping
- **AND** the navigation labels emphasize the task being accomplished (for example “Configure Cloud Storage”, “Read and Write Datasets”) rather than internal module names.

#### Scenario: Reference and explanation content are clearly separated

- **WHEN** a reader navigates to reference or architecture pages
- **THEN** reference-style content (API details, installation instructions) is located in reference-oriented files and sections
- **AND** explanation-style content (architecture, concepts, migration rationale) is located in explanation-oriented files and sections
- **AND** narrative guides link to reference pages instead of re-stating detailed API signatures.

### Requirement: Core Guides Match Implemented APIs

The core documentation guides (Quickstart, API guide, Advanced Usage, Utils reference) SHALL describe logging, filesystem/caching, and parallelism/sync helpers using the actual implemented APIs, without introducing fictitious helpers or outdated signatures.

#### Scenario: Logging guides use current entrypoints

- **WHEN** a user reads `README.md`, `docs/utils.md`, or logging sections in `docs/api-guide.md`
- **THEN** the examples configure logging via the current entrypoints (for example `setup_logging()` and the documented environment variables)
- **AND** they do not reference logging modules or configuration patterns that no longer exist in `src/fsspeckit`.

#### Scenario: Filesystem and caching examples match implementation

- **WHEN** a user follows filesystem or caching examples in `docs/quickstart.md` or `docs/api-guide.md`
- **THEN** the examples use the implemented `filesystem()` and cache behaviour from `src/fsspeckit/core/filesystem.py` and `src/fsspeckit/core/filesystem_cache.py`
- **AND** they do not reference functions that do not exist (for example `get_cache_size`) or omit critical path-safety semantics such as DirFileSystem base-path enforcement.

#### Scenario: Parallelism and sync docs reflect current helpers

- **WHEN** a user reads about `run_parallel`, `sync_dir`, or `sync_files` in `docs/utils.md`, `docs/api-guide.md`, or related guides
- **THEN** the function signatures and examples match the current implementations in `src/fsspeckit/common/misc.py`
- **AND** any documented optional dependency requirements (for example needing `joblib` via the appropriate extra) are accurate.

### Requirement: Provide comprehensive merge strategy guidance

Users SHALL have access to comprehensive documentation explaining when and how to use the `merge()` method with each merge strategy.

#### Scenario: Strategy selection guidance
- **WHEN** a user needs to perform merge operations
- **THEN** documentation SHALL demonstrate the `merge()` method signature:
  `io.merge(data, path, strategy='insert'|'update'|'upsert', key_columns=...)`
- **AND** SHALL provide clear guidance on selecting the appropriate strategy
- **AND** SHALL explain incremental file-level rewrite behavior
- **AND** SHALL document the `MergeResult` return type with its fields

#### Scenario: Practical examples for all strategies
- **WHEN** a user wants to implement merge operations
- **THEN** documentation SHALL include working examples using `io.merge(data, path, strategy=..., key_columns=...)`
- **AND** SHALL show `MergeResult` field access patterns
- **AND** SHALL demonstrate common patterns like composite keys and partition handling

### Requirement: Ensure discoverability of merge functionality

Merge-aware write functionality SHALL be easily discoverable through documentation structure.

#### Scenario: Documentation navigation
- **WHEN** a user browses dataset documentation
- **THEN** merge-aware write functionality SHALL be prominently featured
- **AND** SHALL be accessible from multiple entry points (API docs, how-to guides, examples)
- **AND** SHALL be cross-referenced with related functionality

### Requirement: Provide comprehensive DuckDB merge strategy guidance

Users SHALL have access to comprehensive documentation explaining when and how to use each DuckDB merge strategy.

#### Scenario: DuckDB strategy selection guidance
- **WHEN** a user needs to perform merge operations with DuckDB
- **THEN** documentation SHALL demonstrate `DuckDBDatasetIO.merge()` method
- **AND** SHALL include real-world use cases for each strategy
- **AND** SHALL explain DuckDB-specific behavior of each strategy with existing and new data

#### Scenario: DuckDB vs PyArrow backend selection
- **WHEN** a user chooses between DuckDB and PyArrow for merge operations
- **THEN** documentation SHALL provide guidance on backend selection
- **AND** SHALL include performance characteristics
- **AND** SHALL document feature differences
- **AND** SHALL recommend use cases for each backend

### Requirement: Ensure DuckDB merge functionality discoverability

DuckDB merge-aware write functionality SHALL be easily discoverable through documentation structure.

#### Scenario: DuckDB merge documentation navigation
- **WHEN** a user browses dataset documentation
- **THEN** DuckDB merge-aware write functionality SHALL be prominently featured
- **AND** SHALL be accessible from multiple entry points (API docs, how-to guides, examples)
- **AND** SHALL be cross-referenced with PyArrow merge functionality
- **AND** existing comprehensive examples SHALL be integrated into documentation flow

### Requirement: PyArrow Dataset Handler Class Documentation

The documentation SHALL include comprehensive coverage of PyArrow class-based dataset handlers (`PyarrowDatasetIO`, `PyarrowDatasetHandler`) with same level of detail as DuckDB handler documentation.

#### Scenario: Getting started tutorial includes correct PyArrow examples
- **WHEN** a user follows `docs/tutorials/getting-started.md`
- **THEN** the "Your First Dataset Operation" section SHALL show:
  - `io.write_dataset(data, path, mode='append')` for writes
  - `io.merge(data, path, strategy='upsert', key_columns=[...])` for merges
- **AND** SHALL demonstrate `WriteDatasetResult` and `MergeResult` usage

#### Scenario: How-to guides demonstrate current APIs
- **WHEN** a user reads `docs/how-to/read-and-write-datasets.md` or `docs/how-to/merge-datasets.md`
- **THEN** all examples SHALL use `write_dataset()` and `merge()` methods
- **AND** import statements SHALL show correct PyArrow class imports

### Requirement: PyArrow Approach Guidance Documentation

The documentation SHALL provide clear guidance on when to use function-based vs class-based PyArrow approaches with migration paths between them.

#### Scenario: Approach comparison and selection guidance
- **WHEN** a user evaluates PyArrow options in `docs/dataset-handlers.md`
- **THEN** they SHALL find clear comparison between function-based and class-based approaches
- **AND** use case recommendations SHALL be provided for each approach
- **AND** migration guidance SHALL be available for switching between approaches.

#### Scenario: Consistent cross-references between approaches
- **WHEN** a user reads PyArrow documentation
- **THEN** function-based and class-based approaches SHALL cross-reference each other
- **AND** examples SHALL show equivalent operations in both approaches
- **AND** import statements SHALL be clearly differentiated.

### Requirement: Examples Are Curated and Reference Only Supported APIs

The repository SHALL maintain an `examples/` directory whose runnable scripts
reference only APIs that exist in `src/` and do not require private credentials
or network access by default.

#### Scenario: Obsolete example trees are removed
- **WHEN** an example directory references non-existent helpers (for example `fs.pydala_dataset`) or out-of-date public APIs
- **THEN** the directory SHALL be removed from the runnable example suite (or migrated to docs-only narrative)
- **AND** the remaining `examples/README.md` SHALL not reference removed content.

#### Scenario: Examples install guidance matches maintained set
- **WHEN** a user follows installation instructions in `examples/README.md` or `examples/requirements.txt`
- **THEN** the documented dependencies SHALL be sufficient to run the maintained examples
- **AND** the guidance SHALL avoid requiring cloud credentials or external services by default.

### Requirement: Example Runner Supports Non-Interactive Validation

The project SHALL provide an example runner/validator that can execute the
maintained example suite in a non-interactive environment.

#### Scenario: Running a specific example by path
- **WHEN** a user runs `examples/run_examples.py --file <path>`
- **THEN** the runner SHALL accept both relative and absolute paths
- **AND** it SHALL resolve paths relative to the `examples/` directory, not the current working directory.

#### Scenario: CI-friendly operation
- **WHEN** the runner executes the example suite without a TTY
- **THEN** it SHALL not block on interactive prompts
- **AND** it SHALL provide deterministic pass/fail exit codes.

### Requirement: Dataset write documentation uses write_dataset API

The documentation SHALL demonstrate the `write_dataset()` method for append and overwrite operations.

#### Scenario: Write operations use write_dataset
- **WHEN** a user reads `docs/how-to/read-and-write-datasets.md`
- **THEN** append examples SHALL use `io.write_dataset(data, path, mode='append')`
- **AND** overwrite examples SHALL use `io.write_dataset(data, path, mode='overwrite')`
- **AND** examples SHALL demonstrate accessing `WriteDatasetResult` fields
- **AND** examples SHALL show per-file metadata from the result

#### Scenario: Clear separation of write vs merge
- **WHEN** a user reads dataset documentation
- **THEN** documentation SHALL clearly distinguish:
  - Write operations (append/overwrite): `write_dataset()`
  - Merge operations (insert/update/upsert): `merge()`
- **AND** SHALL explain when to use each approach

### Requirement: Document WriteDatasetResult type

The documentation SHALL include comprehensive coverage of the `WriteDatasetResult` type returned by write operations.

#### Scenario: WriteDatasetResult field documentation
- **WHEN** a user reads dataset write documentation
- **THEN** they SHALL find documentation for `WriteDatasetResult` with its fields:
  - `files`: list of `FileWriteMetadata` (path, row_count, size_bytes)
  - `total_rows`: total rows written
  - `mode`: the write mode used ('append' or 'overwrite')
  - `backend`: the backend used ('pyarrow' or 'duckdb')

#### Scenario: WriteDatasetResult usage examples
- **WHEN** a user implements write operations
- **THEN** documentation SHALL show how to:
  - Access result fields for logging and monitoring
  - Iterate over written files for downstream processing
  - Check operation success via result counts

### Requirement: Document MergeResult type

The documentation SHALL include comprehensive coverage of the `MergeResult` type returned by merge operations.

#### Scenario: MergeResult field documentation
- **WHEN** a user reads merge documentation
- **THEN** they SHALL find documentation for `MergeResult` with its fields:
  - `strategy`: the merge strategy used
  - `source_count`, `target_count_before`, `target_count_after`: row counts
  - `inserted`, `updated`, `deleted`: operation counts
  - `files`: list of `MergeFileMetadata`
  - `rewritten_files`, `inserted_files`, `preserved_files`: file path lists

#### Scenario: MergeResult usage examples
- **WHEN** a user implements merge operations
- **THEN** documentation SHALL show how to:
  - Access result fields for logging and monitoring
  - Verify operation success via inserted/updated counts
  - Identify which files were affected by the merge

### Requirement: Consistent import path documentation

The documentation SHALL use consistent import patterns that prefer the shortest valid path while remaining clear about package organization.

#### Scenario: Core symbols use top-level imports
- **WHEN** documentation demonstrates filesystem or storage options usage
- **THEN** examples SHALL prefer `from fsspeckit import filesystem, AwsStorageOptions`
- **AND** SHALL NOT show multiple alternative import paths for the same symbol

#### Scenario: Dataset handlers use package imports
- **WHEN** documentation demonstrates dataset operations
- **THEN** examples SHALL use `from fsspeckit.datasets import PyarrowDatasetIO, DuckDBParquetHandler`
- **AND** SHALL show consistent patterns across all how-to guides and tutorials

#### Scenario: SQL utilities use module imports
- **WHEN** documentation demonstrates SQL filter translation
- **THEN** examples SHALL use `from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter`
- **AND** SHALL be consistent across all documentation files

### Requirement: Import hierarchy documentation

The documentation SHALL include a brief section explaining the import path hierarchy for discoverability.

#### Scenario: Import patterns section exists
- **WHEN** a user reads `docs/explanation/concepts.md`
- **THEN** they SHALL find a section explaining import patterns:
  - Top-level: `from fsspeckit import ...` for core symbols (filesystem, storage options)
  - Package-level: `from fsspeckit.datasets import ...` for dataset handlers
  - Module-level: `from fsspeckit.sql.filters import ...` for specific utilities

#### Scenario: No deprecated import comments
- **WHEN** a user reads documentation examples
- **THEN** they SHALL NOT find "Legacy import (still works but deprecated)" comments
- **AND** SHALL NOT find multiple alternative import paths shown for the same operation

### Requirement: Local-First Examples Respect DirFileSystem Base Paths

Examples that operate on temporary local data SHALL be written to respect the
default `DirFileSystem` base-path semantics of `fsspeckit.filesystem()`.

#### Scenario: Temp data is readable via rooted filesystem
- **WHEN** an example creates files under a temporary directory
- **THEN** it SHALL create its filesystem rooted at that directory (for example `filesystem(temp_dir, ...)`)
- **AND** it SHALL use relative patterns within that base for glob/exists/read operations.

#### Scenario: JSON examples use consistent read/write options
- **WHEN** an example writes dict-like JSON data and then reads it back
- **THEN** it SHALL use compatible `read_json` options (for example `as_dataframe=False`) or write data in a format matching the default read mode.

### Requirement: Common and SQL Examples Match Public API Surface

The examples under `examples/common/` and `examples/sql/` SHALL use only public,
currently supported APIs and demonstrate correct usage patterns.

#### Scenario: Logging examples use supported logging helpers
- **WHEN** a user runs the logging example(s)
- **THEN** they SHALL configure logging using `fsspeckit.common.logging.setup_logging`
- **AND** obtain a logger via `fsspeckit.common.logging.get_logger` (or equivalent supported pattern).

#### Scenario: SQL filter examples supply required schema
- **WHEN** a user runs SQL filter examples that call `sql2pyarrow_filter` or `sql2polars_filter`
- **THEN** the examples SHALL provide the schema argument required by the current function signatures
- **AND** the examples SHALL run offline against a locally created dataset.

### Requirement: Getting Started Examples Run Offline by Default

The `examples/datasets/getting_started/` scripts SHALL run successfully in a
clean environment using local temporary data and current dependency APIs.

#### Scenario: PyArrow compatibility
- **WHEN** a user runs a getting-started script using a supported PyArrow version
- **THEN** the script SHALL use supported table construction APIs (for example `pa.Table.from_pylist`)
- **AND** it SHALL not depend on deprecated behaviors that break on upgrade.

#### Scenario: Non-interactive execution
- **WHEN** a user runs the scripts in a non-interactive environment
- **THEN** the scripts SHALL complete without waiting for `input()`
- **AND** optional tutorial pauses SHALL be gated behind an explicit interactive flag.

### Requirement: Examples Are Safe to Run (No Destructive Cleanup)

Example scripts that create temporary files/directories SHALL clean up only the
paths they created and MUST NOT delete broad parent directories.

#### Scenario: Safe cleanup behavior
- **WHEN** a schema/workflow example creates a temporary directory for its data
- **THEN** cleanup SHALL remove only that directory
- **AND** it SHALL not attempt to delete shared system paths such as `/tmp`.

#### Scenario: Workflow examples are local-first
- **WHEN** a workflow example demonstrates cloud-style datasets
- **THEN** it SHALL run offline by default using local simulation
- **AND** any real-cloud execution SHALL be gated behind explicit configuration.

