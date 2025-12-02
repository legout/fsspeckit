## Why

The current package layout mixes core filesystem primitives, backend-neutral
planning logic, and high-level dataset helpers under `core/` and `utils/` in
ways that are hard to navigate:

- `fsspeckit.utils` contains both generic helpers (logging, datetime parsing,
  parallelism, type conversion) and core value features (DuckDB-based parquet
  handler, PyArrow dataset merge/maintenance, SQL-to-filter translators).
- Backend-neutral planning code for merge and maintenance lives under
  `fsspeckit.core.merge` and `fsspeckit.core.maintenance`, while the public
  dataset APIs live under `fsspeckit.utils.duckdb` and
  `fsspeckit.utils.pyarrow`, which makes the logical layering harder to see.
- There is no explicit “datasets” domain package; users have to discover
  dataset operations via `utils`, which reads as incidental rather than
  first-class.
- Structural boundaries (e.g. “core must not depend on dataset backends”) are
  not documented, making future contributions more likely to introduce
  cross-layer coupling.

This makes it difficult for new users and contributors to understand where to
add new functionality (filesystem vs dataset vs generic utility) and makes the
project feel flatter and more ad-hoc than the underlying architecture really
is.

## What Changes

- Introduce explicit domain packages for the main areas of the library:
  `fsspeckit.datasets` for dataset-level operations (DuckDB and PyArrow),
  `fsspeckit.sql` for SQL-to-filter translation, and `fsspeckit.common` for
  cross-cutting helpers (datetime, logging, parallelism, type/DF utilities).
- Clarify `fsspeckit.core` as the backend-neutral layer that owns filesystem
  factories, merge semantics, and maintenance planning, with a dedicated
  `core.filesystem` module holding the public filesystem API surface.
- Keep `fsspeckit.utils` as a backwards-compatible façade that re-exports
  functionality from the new domain packages, but stop treating it as the
  primary home for new features.
- Document and enforce import layering so that `core` and `storage_options`
  do not depend on dataset or `sql` packages, while dataset backends depend
  on `core` and `common`, not the other way round.

## Impact

- Affected specs:
  - New capability: `project-architecture` describing domain-driven package
    layout and layering rules.
  - Existing capabilities (`utils-duckdb`, `utils-pyarrow`, `utils-sql`) keep
    their behavioral contracts but will be reachable through both the legacy
    `fsspeckit.utils` façade and new domain packages.
- Affected code:
  - New packages: `fsspeckit.datasets`, `fsspeckit.sql`, `fsspeckit.common`.
  - `fsspeckit.core`: introduce/clarify a `filesystem` module that owns
    public filesystem factory APIs and avoid importing from dataset or
    `sql` packages.
  - `fsspeckit.utils`: refactored into a thin compatibility layer that
    forwards to `datasets`, `sql`, and `common` modules.
- Migration:
  - Existing imports like `from fsspeckit.utils import DuckDBParquetHandler`
    or `from fsspeckit.utils import run_parallel` remain supported for at
    least one deprecation cycle.
  - Documentation and examples will gradually be updated to use the new
    domain packages (`fsspeckit.datasets`, `fsspeckit.common`, `fsspeckit.sql`)
    as the preferred entry points.

