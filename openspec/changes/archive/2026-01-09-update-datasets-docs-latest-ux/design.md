## Context

Recent work on datasets module UX unification introduced:

1. **Filter type differences**: DuckDB now validates `filters: str | None` at runtime; PyArrow accepts multiple filter types (expressions, DNF tuples, SQL-like strings with conversion).
2. **Backend-specific knobs**: DuckDB exposes `use_merge` (MERGE vs UNION ALL), `use_threads` (write_parquet only); PyArrow exposes streaming/memory controls (merge_chunk_size_rows, enable_streaming_merge, etc.).
3. **Handler standardization**: Both backends use `write_dataset()`, `merge()`, `compact_parquet_dataset()`, `optimize_parquet_dataset()`, `read_parquet()` with consistent signatures.

Human-authored documentation needs to reflect these changes without duplicating API signatures (which are generated from docstrings).

## Goals / Non-Goals

- **Goals**:
  - Make all dataset documentation reflect current implementation (post-UX-unification)
  - Ensure filter type guidance is consistent and backend-appropriate
  - Document backend-specific parameters clearly (DuckDB `use_merge`; PyArrow streaming knobs)
  - Keep clear separation: narrative docs (how-to/reference) vs generated API docs

- **Non-Goals**:
  - Removing narrative content from how-to/reference pages
  - Changing how mkdocstrings generates documentation
  - Making docs overly prescriptive (users should refer to generated API for signatures)

## Decisions

- **Decision: Update docs/dataset-handlers.md as canonical backend differences page**
  - Rationale: Already has backend comparison table; best place to document filter types, backend-specific knobs, and handler signature differences
  - Alternative considered: Create new dedicated page for backend differences
  - Why not: Avoids content duplication; `docs/dataset-handlers.md` already serves this role

- **Decision: Update docs/how-to/merge-datasets.md with backend-specific examples**
  - Rationale: Merge operations have significant backend differences (DuckDB MERGE SQL vs PyArrow streaming)
  - Alternative considered: Separate merge docs per backend
  - Why not: Increases maintenance burden; single doc with conditional examples is better

- **Decision: Update docs/how-to/read-and-write-datasets.md with current dataset UX**
  - Rationale: Dataset reading/writing is a common entry point; examples need to show current handler usage
  - Alternative considered: Keep old examples with deprecation warnings
  - Why not: New users shouldn't encounter deprecated patterns

- **Decision: Add explicit filter type guidance to relevant docs**
  - Rationale: DuckDB now enforces `str` filters at runtime; this is a breaking change for users passing other types
  - Alternative considered: Keep silent and let runtime errors guide users
  - Why not: Proactive documentation prevents confusion and provides clear migration path

- **Decision: Update docs/reference/api-guide.md to point to handlers, not legacy helpers**
  - Rationale: Handler classes are the canonical API surface; legacy helpers are gone
  - Alternative considered: Document legacy helpers as "deprecated"
  - Why not: Better to guide users to current API directly

## Migration Plan

### Phase 1: Audit current documentation
1. Read `docs/dataset-handlers.md` to understand current backend differences table
2. Read `docs/how-to/merge-datasets.md` to identify outdated examples
3. Read `docs/how-to/read-and-write-datasets.md` to check handler usage patterns
4. Search docs for mentions of `DuckDBParquetHandler`, `write_parquet_dataset` (old), or other removed APIs

### Phase 2: Update backend differences documentation (after approval)
1. Update `docs/dataset-handlers.md` backend differences table:
   - Add row for filter types: DuckDB (`str | None`) vs PyArrow (expressions, DNF, SQL-like)
   - Add row for merge backend: DuckDB (`use_merge`) vs PyArrow (streaming/memory knobs)
   - Verify handler method signatures are correctly described
2. Add cross-links from how-to pages to `docs/dataset-handlers.md`

### Phase 3: Update how-to guides (after approval)
1. Update `docs/how-to/merge-datasets.md`:
   - Add DuckDB `use_merge` example (document MERGE vs UNION ALL behavior)
   - Add PyArrow streaming knobs examples
   - Ensure all code examples use `DuckDBDatasetIO` or `PyarrowDatasetIO`
2. Update `docs/how-to/read-and-write-datasets.md`:
   - Add dataset section with handler usage examples
   - Document filter expectations per backend (DuckDB: SQL string; PyArrow: flexible)
   - Update all code examples to use current imports

### Phase 4: Update API guide (after approval)
1. Update `docs/reference/api-guide.md`:
   - Remove references to removed APIs (DuckDBParquetHandler, legacy helpers)
   - Add guidance that API details are in generated pages
   - Point users to `docs/dataset-handlers.md` for backend-specific guidance

### Phase 5: Validation
1. Build docs with `mkdocs build`
2. Serve docs locally and verify:
   - Backend differences page is accessible from all how-to pages
   - Code examples work (copy-paste test if possible)
   - No broken links to generated API pages
3. Check for any remaining references to removed APIs

## Open Questions

- Should we add a "Breaking Changes" section to highlight filter type validation change?
- Do we need to add deprecation notices for old docs that reference removed APIs until they're fully updated?
- Should `docs/use-sql-filters.md` have a section on dataset read filters specifically?

## Risks / Trade-offs

- **Risk**: Users may copy old examples that use removed APIs
  - **Mitigation**: Search docs for all references to removed APIs before deployment
  - **Fallback**: Runtime errors will occur with clear messages (DuckDB filter validation already in place)
- **Trade-off**: More detailed backend documentation vs higher maintenance burden
  - **Benefit**: Users have clear guidance on choosing backends
  - **Mitigation**: Keep `docs/dataset-handlers.md` as single source of truth for backend differences
- **Risk**: Documentation updates may miss some backend-specific edge cases
  - **Mitigation**: Cross-reference generated API pages for complete signature details
