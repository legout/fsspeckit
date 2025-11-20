# Capability: Project Documentation

## ADDED Requirements

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

