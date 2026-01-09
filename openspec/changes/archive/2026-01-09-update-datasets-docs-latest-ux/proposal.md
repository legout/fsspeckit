# Change: Update datasets documentation to match latest UX unification

## Why

Recent UX unification work between `datasets.duckdb` and `datasets.pyarrow` (filter type validation, backend-specific knobs, handler signatures) is not reflected in human-authored documentation. Users may encounter:

- Stale API references (DuckDBParquetHandler, legacy merge methods)
- Incorrect filter type guidance (DuckDB: `str | None` vs PyArrow: flexible)
- Missing backend-specific parameters in examples (DuckDB `use_merge`, PyArrow streaming knobs)
- Outdated return type references (list[pq.FileMetaData] vs actual MergeResult/WriteDatasetResult)

Documentation needs to align with current implementation while keeping clear separation between generated API docs and narrative guidance.

## What Changes

- Update `docs/dataset-handlers.md` to reflect unified UX and backend differences matrix
- Update `docs/how-to/merge-datasets.md` to document DuckDB `use_merge` and PyArrow streaming knobs
- Update `docs/how-to/read-and-write-datasets.md` with current dataset handler patterns
- Update `docs/reference/api-guide.md` to point to correct entry points (handlers, not legacy helpers)
- Ensure all filter examples distinguish DuckDB (SQL WHERE string only) vs PyArrow (expressions, DNF, SQL-like conversion)
- Add cross-links: how-to pages → `docs/dataset-handlers.md` + generated API pages

## Impact

- Affected specs: `datasets-duckdb`, `project-docs`
- Affected code: None (documentation only)
- Affected files: `docs/dataset-handlers.md`, `docs/how-to/merge-datasets.md`, `docs/how-to/read-and-write-datasets.md`, `docs/reference/api-guide.md`
