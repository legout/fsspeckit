# Change: Refactor API docs to mkdocstrings-generated pages

## Why

Current `docs/api/*` pages contain manual hand-authored content that is now stale (references removed APIs like `DuckDBParquetHandler`, outdated return types, old parameters). Since `mkdocs.yml` configures mkdocstrings to generate these pages from docstrings, the hand-authored content conflicts with generation and causes documentation drift.

Generated API pages must reflect the actual code implementation, not manual descriptions that can become outdated.

## What Changes

- Convert `docs/api/fsspeckit.datasets.md` to use mkdocstrings directive only (same pattern as other generated pages)
- Update `docs/api/index.md` migration guidance to reference current dataset APIs (DuckDBDatasetIO, PyarrowDatasetIO, etc.)
- Fix `mkdocs.yml` navigation to remove duplicate "API Reference" blocks
- Establish clear separation: `docs/api/*` for generated docs, `docs/how-to/*` and `docs/reference/*` for narrative guidance

## Impact

- Affected specs: `project-docs`
- Affected code: None (documentation only)
- Affected files: `docs/api/fsspeckit.datasets.md`, `docs/api/index.md`, `mkdocs.yml`
