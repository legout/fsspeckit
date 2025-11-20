## 1. Discover and plan

- [x] 1.1 Survey references to `fsspeckit.utils.*` in README, docs, and
  examples and classify each as:
  - Preferred new import (e.g. `fsspeckit.datasets`, `fsspeckit.sql`,
    `fsspeckit.common`, `fsspeckit.core`), or
  - Intentionally kept as a `utils` façade example.
- [x] 1.2 Cross-check the `project-architecture` spec to ensure that planned
  documentation aligns with the canonical package layout and layering rules.

## 2. Update high-level docs

- [x] 2.1 Update `README.md` to introduce the domain packages and show
  canonical import examples for:
  - Filesystem (`fsspeckit.core` / top-level imports).
  - Storage options (`fsspeckit.storage_options`).
  - Datasets (`fsspeckit.datasets`).
  - SQL helpers (`fsspeckit.sql`).
  - Common utilities (`fsspeckit.common`).
- [x] 2.2 Update `docs/index.md`, `docs/architecture.md`, and
  `docs/advanced.md` to:
  - Reference the new packages when describing architecture and capabilities.
  - Mention that `fsspeckit.utils` is a compatibility façade that re-exports
    selected helpers.

## 3. Update utils/docs and API reference

- [x] 3.1 Refactor `docs/utils.md` so that:
  - Section structure mirrors the domain packages (common, datasets, sql),
    with `utils` clearly labeled as a façade.
  - Code examples prefer new imports (e.g.
    `from fsspeckit.datasets import DuckDBParquetHandler`) while optionally
    showing legacy `utils` imports in "Compatibility" notes.
- [x] 3.2 Update `docs/api-guide.md` examples to use the new imports, except
  where explicitly demonstrating backwards compatibility.
- [x] 3.3 Adjust `mkdocs.yml` and `docs/api/index.md` to:
  - Include API sections for `fsspeckit.common`, `fsspeckit.datasets`, and
    `fsspeckit.sql` if not already present.
  - Clarify where `fsspeckit.utils` fits in the API reference.

## 4. Update examples

- [x] 4.1 Update DuckDB examples (e.g. under `examples/duckdb/`) to import
  `DuckDBParquetHandler` and related helpers from `fsspeckit.datasets`.
- [x] 4.2 Update PyArrow examples (e.g. `examples/pyarrow/*`) to import
  dataset helpers (merge/compaction/optimization) from
  `fsspeckit.datasets.pyarrow` and SQL helpers from `fsspeckit.sql` where
  applicable.
- [x] 4.3 Ensure example READMEs mention the new package structure and map
  commonly used helpers to their new canonical modules.

## 5. Validation

- [x] 5.1 Run `openspec validate update-docs-package-layout --strict` and
  ensure the change passes.
- [x] 5.2 Build the docs site (e.g. `uv run mkdocs build` or `mkdocs build`)
  and scan for:
  - Broken links or outdated import paths.
  - Inconsistencies with `project-architecture` and `project-docs` specs.

