## Context

The current `fsspeckit` layout follows a broad separation between `core/`,
`storage_options/`, and `utils/`:

- `core/` contains filesystem factories (`filesystem`, `get_filesystem`),
  caching wrappers, and backend-neutral merge and maintenance layers
  (`core.merge`, `core.maintenance`).
- `storage_options/` holds configuration classes for local, cloud, and VCS
  backends (AwsStorageOptions, GcsStorageOptions, GitLabStorageOptions,
  etc.).
- `utils/` contains a mix of:
  - High-value dataset features: DuckDBParquetHandler, DuckDB dataset
    maintenance helpers (`compact_parquet_dataset`, `optimize_parquet_dataset`),
    PyArrow dataset merge/maintenance helpers
    (`merge_parquet_dataset_pyarrow`, `compact_parquet_dataset_pyarrow`,
    `optimize_parquet_dataset_pyarrow`).
  - SQL filter translation (`sql2pyarrow_filter`, `sql2polars_filter`).
  - Cross-cutting helpers: logging setup, datetime parsing, parallel
    execution (`run_parallel`), Polars helpers, type conversion utilities.

Specs under `openspec/specs/utils-duckdb`, `utils-pyarrow`, and `utils-sql`
describe dataset merge and maintenance semantics, while `core.merge` and
`core.maintenance` now host the backend-neutral planning logic. This means
the existing code already behaves like a layered architecture, but the
package boundaries do not make that layering obvious.

## Goals / Non-Goals

- Goals:
  - Make dataset operations (DuckDB and PyArrow) first-class citizens in the
    package structure, not just “utils”.
  - Make `core` clearly responsible for backend-neutral semantics and
    filesystem primitives, with an explicit `core.filesystem` module.
  - Introduce domain packages (`datasets`, `sql`, `common`) that match how
    users think about the library: datasets, SQL filters, and reusable
    helpers.
  - Preserve the existing public behavior and signatures (particularly for
    `utils-*` specs) while offering clearer, more discoverable import paths.
  - Codify simple layering rules to keep core logic independent of
    higher-level helpers.

- Non-Goals:
  - No behavioral changes to merge or maintenance semantics described in
    `utils-duckdb` and `utils-pyarrow` specs.
  - No immediate removal of `fsspeckit.utils` imports; deprecation and
    eventual removal can be handled in future changes if desired.
  - No change to external dependency footprint or supported backends.

## Decisions

- **Introduce domain packages**:
  - `fsspeckit.datasets`: dataset-level operations on Parquet (and future
    formats), implemented via DuckDB and PyArrow, delegating to
    `core.merge` and `core.maintenance`.
  - `fsspeckit.sql`: SQL-to-filter translation helpers for PyArrow and
    Polars, currently implemented via `sqlglot`.
  - `fsspeckit.common`: shared helpers used across the library (datetime
    parsing, logging setup, parallel execution, Polars and type utilities).

- **Clarify `core` responsibilities**:
  - Create `fsspeckit.core.filesystem` as the canonical home for filesystem
    factories and cache wrappers, re-exported from `fsspeckit.core.__init__`.
  - Keep `fsspeckit.core.merge` and `fsspeckit.core.maintenance` as the
    only owners of backend-neutral merge/maintenance logic; dataset backends
    must call into these modules rather than duplicating logic.

- **Backwards-compatible `utils` façade**:
  - Keep `fsspeckit.utils` as a thin façade that imports from
    `datasets`, `sql`, and `common` modules and exposes the existing public
    names described in the `utils-*` specs.
  - Avoid adding new functionality directly under `fsspeckit.utils`
    going forward; instead, add new features under the appropriate domain
    package and selectively re-export them from `utils` if needed.

- **Layering rules**:
  - `core` may depend on `storage_options` and `common`, but not on
    `datasets`, `sql`, or `utils`.
  - `datasets` may depend on `core`, `storage_options`, `sql`, and
    `common`, but not on `utils`.
  - `sql` may depend on `common`, but not on `datasets` or `utils`.
  - `common` must not depend on `datasets` or `sql`, to avoid cycles.
  - `utils` acts purely as a façade layer on top of `datasets`, `sql`, and
    `common`.

## Alternatives considered

- **Stay with the current layout** (everything under `core` and `utils`):
  - Pros: No refactor needed; zero risk of import churn.
  - Cons: Continues to hide the distinction between backend-neutral and
    backend-specific code; new contributors are likely to keep adding
    functionality under `utils`, making the module harder to navigate.

- **Flatten everything at the top level** (e.g. `fsspeckit.duckdb`,
  `fsspeckit.pyarrow`):
  - Pros: Simplest import paths.
  - Cons: Loses the clear relationship between dataset operations and
    their underlying backends; does not group related helpers (SQL filters,
    datetime, types) in a cohesive way.

- **Introduce `backends` instead of `datasets`** (e.g.
  `fsspeckit.backends.duckdb`, `fsspeckit.backends.pyarrow`):
  - Pros: Emphasizes backends explicitly.
  - Cons: Slightly less intuitive for users who think in terms of “dataset
    operations” rather than backend engines; `datasets` more directly maps
    to the high-level concept described in the docs and specs.

Given the current functionality and docs, `datasets` as a domain package
strikes the best balance between clarity and future extensibility.

## Risks / Trade-offs

- **Import churn risk**: Internal imports and some user code may rely on
  specific module paths (e.g. `fsspeckit.utils.duckdb`). Mitigation:
  - Keep the `utils` façade and implement alias modules under the new
    structure so that old imports continue to work.
  - Update docs and examples to favor the new imports but call out the
    legacy paths as supported for now.

- **Complexity risk**: Adding new packages (`datasets`, `sql`, `common`)
  increases the number of modules. Mitigation:
  - Keep each package focused and small.
  - Avoid deep nesting beyond what is necessary (e.g. `datasets.duckdb`,
    `datasets.pyarrow` only).

- **Tooling configuration**: Linting, type checking, and docs may need
  updates to include the new packages. Mitigation:
  - Update `pyproject.toml` and `mkdocs.yml` as part of this change.

## Migration Plan

1. Introduce the new packages (`datasets`, `sql`, `common`) and populate
   them via imports/aliases from existing `utils` modules so that no behavior
   changes and existing imports keep working.
2. Gradually move implementation code out of `fsspeckit.utils.*` modules into
   the new packages, keeping thin wrappers or re-exports under `utils` for
   backwards compatibility.
3. Update documentation, examples, and API reference to use the new domain
   packages as the preferred import paths.
4. Optionally (in a future change) deprecate direct use of
   `fsspeckit.utils` in favor of the domain packages, with clear guidance
   and a deprecation timeline.

## Open Questions

- How long should `fsspeckit.utils` remain fully supported as a façade before
  being formally deprecated (if ever)?
- Should dataset helpers for non-Parquet formats (if/when added) also live
  under `fsspeckit.datasets`, or is a separate `formats` or `io` package
  preferable in the long term?
- Do we want a static import-layer check (e.g. a small script or ruff rule)
  enforced in CI to prevent future violations of the layering rules?

