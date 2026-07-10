## Context

Documentation structure currently has two separate issues:

1. **Stale generated API docs**: `docs/api/fsspeckit.datasets.md` contains hand-authored tables that reference removed APIs (`DuckDBParquetHandler`, old return types, outdated parameters) and conflict with mkdocstrings generation.
2. **Duplicate nav blocks**: `mkdocs.yml` has two separate "API Reference" navigation blocks (lines 29-41 and 36-41), creating potential user confusion.
3. **Inconsistent patterns**: Other generated API pages (e.g., `docs/api/fsspeckit.core.merge.md`) use a simple directive-based pattern (`::: module.name`), while `docs/api/fsspeckit.datasets.md` uses detailed manual tables.

## Goals / Non-Goals

- **Goals**:
  - Make `docs/api/*` purely mkdocstrings-generated, reflecting actual code
  - Establish clear separation: generated API docs vs narrative guidance
  - Fix navigation to eliminate duplicate sections
  - Prevent documentation drift by removing stale hand-authored API content

- **Non-Goals**:
  - Changing how mkdocstrings generates documentation (that's configured correctly)
  - Removing narrative content from `docs/api/index.md` (it serves as an overview/navigation hub)

## Decisions

- **Decision: Replace manual `docs/api/fsspeckit.datasets.md` with mkdocstrings directive**
  - Rationale: Consistency with other generated pages and guaranteed sync with code
  - Alternative considered: Keep manual content but update it regularly
  - Why not: High maintenance burden; manual content will inevitably drift

- **Decision: Remove duplicate "API Reference" nav block from `mkdocs.yml`**
  - Rationale: Single coherent navigation section prevents user confusion
  - Alternative considered: Merge blocks but keep both titles
  - Why not: Duplicate sections in nav is semantically confusing

- **Decision: Update `docs/api/index.md` migration guidance to reference current APIs**
  - Rationale: Current text still recommends removed `DuckDBParquetHandler`
  - Alternative considered: Add separate "Deprecated APIs" section
  - Why not: Better to remove references entirely if APIs are gone

## Migration Plan

### Phase 1: Prepare proposal and spec updates
- Create `openspec/changes/refactor-docs-generated-api-pages/proposal.md` (this file)
- Create `openspec/changes/refactor-docs-generated-api-pages/design.md` (this file)
- Draft `openspec/changes/refactor-docs-generated-api-pages/specs/project-docs/spec.md` delta with MODIFIED requirements

### Phase 2: Implement changes (after approval)
1. Replace `docs/api/fsspeckit.datasets.md` content with simple directive:
   ```markdown
   ::: fsspeckit.datasets
   ```
2. Update `docs/api/index.md` migration tips:
   - Remove references to `DuckDBParquetHandler`
   - Add current handler classes: `DuckDBDatasetIO`, `PyarrowDatasetIO`, `create_duckdb_connection`
   - Point to `docs/dataset-handlers.md` for backend differences
3. Remove duplicate nav block from `mkdocs.yml` (lines 29-41 duplicate)
4. Verify mkdocstrings generates correct `docs/api/fsspeckit.datasets.md` content

### Phase 3: Validation
- Run `openspec validate refactor-docs-generated-api-pages --strict`
- Build docs locally with `mkdocs serve` to verify nav is correct
- Check generated page renders expected API signatures

## Open Questions

- Should we add a "Deprecated APIs" section in `docs/api/index.md` to provide migration context for removed classes?
- Do we need to update other `docs/api/*.md` hand-authored pages beyond `fsspeckit.datasets.md`?

## Risks / Trade-offs

- **Risk**: Users with old bookmarked URLs may get 404s if generated page structure changes significantly
  - **Mitigation**: Keep mkdocstrings paths stable; generated content should be semantically equivalent
- **Trade-off**: Loss of custom narrative in `docs/api/fsspeckit.datasets.md` tables
  - **Benefit**: Guaranteed accuracy and consistency with code
  - **Mitigation**: Move any critical narrative to `docs/dataset-handlers.md` or `docs/reference/api-guide.md`
