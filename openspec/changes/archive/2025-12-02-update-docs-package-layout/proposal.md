## Why

The core refactor captured in `refactor-package-layout-fsspeckit` reorganized
the codebase into domain packages (`common`, `core`, `datasets`, `sql`,
`storage_options`, `utils`) and updated the canonical import paths. However,
the written documentation (README, guides, utils page, API reference, and
examples) still primarily references the old `fsspeckit.utils.*` layout and
does not explain the new domain packages or how they relate to the
architecture spec.

This mismatch makes it harder for users to discover the preferred imports for
filesystem, dataset, SQL, and common helpers, and for contributors to align
their changes with the `project-architecture` and `project-docs` specs.

## What Changes

- Update README and high-level docs to:
  - Introduce the new domain packages (`fsspeckit.common`,
    `fsspeckit.datasets`, `fsspeckit.sql`) and how they map to major
    capabilities.
  - Show preferred import paths that reflect the refactored layout while
    noting that `fsspeckit.utils` remains supported as a façade.
- Refresh the "Utils" page and API guide so that:
  - Dataset helpers are documented under `fsspeckit.datasets.*` sections.
  - SQL helpers are documented under `fsspeckit.sql.*`.
  - Cross-cutting utilities are documented as `fsspeckit.common.*`.
  - `fsspeckit.utils` is described as a backwards-compatible convenience
    module, not the primary home for new features.
- Align examples (especially DuckDB and PyArrow examples) with the new
  package layout, preferring `fsspeckit.datasets` and `fsspeckit.sql` over
  direct `utils` imports where appropriate.

## Impact

- Affected specs:
  - `project-docs` — extend documentation requirements so that docs explain
    the domain package layout defined by `project-architecture` and use
    canonical imports consistent with the refactored code.
- Affected artifacts:
  - `README.md` and `docs/index.md`, `docs/architecture.md`,
    `docs/advanced.md`, `docs/utils.md`, `docs/api-guide.md`.
  - API reference pages under `docs/api/` for utils-related modules (e.g.
    `fsspeckit.utils.*`) that now have `common`, `datasets`, or `sql`
    equivalents.
  - Example scripts and READMEs under `examples/` that serve as user-facing
    documentation for dataset and SQL helpers.
- No behavior changes to the public APIs; this is a documentation-only
  change that aligns the written guides with the already-completed refactor.

