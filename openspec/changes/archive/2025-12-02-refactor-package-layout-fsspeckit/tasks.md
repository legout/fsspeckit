## 1. Package and module structure

- [x] 1.1 Introduce new domain packages under `src/fsspeckit/`:
  - `datasets/` for dataset-level operations (DuckDB and PyArrow helpers).
  - `sql/` for SQL-to-filter helpers.
  - `common/` for cross-cutting utilities (datetime, logging, parallelism,
    type conversion, Polars helpers).
- [x] 1.2 Create `fsspeckit.core.filesystem` as the canonical home for
  filesystem-facing APIs (`filesystem`, `get_filesystem`, `DirFileSystem`,
  `MonitoredSimpleCacheFileSystem`, `GitLabFileSystem`), and re-export them
  from `fsspeckit.core.__init__`.
- [x] 1.3 Refactor `fsspeckit.utils` into a thin façade that re-exports from
  `datasets`, `sql`, and `common` modules, keeping the existing public names
  available for backwards compatibility.

## 2. Code moves and aliases

- [x] 2.1 Move or alias DuckDB dataset helpers from
  `fsspeckit.utils.duckdb` into `fsspeckit.datasets.duckdb`, ensuring that
  `DuckDBParquetHandler` and dataset maintenance/merge helpers live under the
  new package.
- [x] 2.2 Move or alias PyArrow dataset helpers from
  `fsspeckit.utils.pyarrow` into `fsspeckit.datasets.pyarrow`, keeping
  behavior identical and delegating merge/maintenance semantics to
  `fsspeckit.core.merge` and `fsspeckit.core.maintenance`.
- [x] 2.3 Move or alias SQL filter helpers from `fsspeckit.utils.sql` into
  `fsspeckit.sql.filters`, keeping the existing function names and signatures
  intact.
- [x] 2.4 Move or alias cross-cutting utilities (`datetime`, `logging`,
  `misc`, `polars`, `types`) into `fsspeckit.common.*` modules, updating
  internal imports to reference `fsspeckit.common` rather than `utils`.

## 3. Layering and imports

- [x] 3.1 Add import rules (and, where practical, static checks) to ensure
  that:
  - `core` modules only depend on the standard library, third-party deps,
    and `storage_options`/`common` (not on `datasets` or `sql`).
  - `datasets` modules may depend on `core`, `storage_options`, `sql`, and
    `common`, but not on `utils`.
  - `sql` modules may depend on `common` but not on `datasets`.
  - `utils` remains a façade only, with no new implementation code.
- [x] 3.2 Update type-checking and linting configuration (if needed) so that
  new packages are included in `mypy` and `ruff` runs.

## 4. Public API and docs

- [x] 4.1 Update documentation and examples to use the new domain packages as
  the preferred import paths (for example,
  `from fsspeckit.datasets import DuckDBParquetHandler`), while noting that
  `fsspeckit.utils` is kept for backwards compatibility.
- [x] 4.2 Update API reference configuration (MkDocs/mkdocstrings) to include
  `fsspeckit.datasets`, `fsspeckit.sql`, and `fsspeckit.common` sections.
- [x] 4.3 Add a short migration guide explaining the new layout and mapping
  from old imports (`fsspeckit.utils.*`) to the new domain packages.

## 5. Validation

- [x] 5.1 Run `openspec validate refactor-package-layout-fsspeckit --strict`
  and ensure the change passes.
- [x] 5.2 Run the full test suite (`uv run pytest`) and the documentation
  build (`uv run mkdocs build` or `mkdocs build`) to confirm that imports
  and API docs are consistent with the new layout.

