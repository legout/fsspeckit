# Change Proposal: Strengthen SQL→PyArrow Filter Interop

## Why

The `sql2pyarrow_filter` helper in `src/fsspeckit/utils/sql.py` converts SQL
filter strings into PyArrow `pc.Expression` objects. This is already used in
tests and can be leveraged by both DuckDB and PyArrow dataset workflows.

Given the new PyArrow dataset merge and maintenance helpers, we want to treat
`sql2pyarrow_filter` as a stable capability that:

- Produces expressions that are directly usable as filters in
  `pyarrow.dataset.Scanner` (for example, `dataset.to_table(filter=expr)`).
- Handles common SQL constructs (comparisons, boolean logic, `IN`, `IS NULL`,
  timestamps/dates/times) in a way that works across DuckDB and pure PyArrow
  flows.
- Avoids regressions as we evolve SQL parsing or PyArrow versions.

## What Changes

- Introduce a `utils-sql` capability spec describing `sql2pyarrow_filter`
  requirements for PyArrow interoperability.
- Ensure `sql2pyarrow_filter`:
  - Works with PyArrow’s timestamp/date/time and boolean types.
  - Produces `pc.Expression` objects compatible with
    `pyarrow.dataset.Scanner(filter=expr)`.
  - Properly handles `IN`/`NOT IN`, `IS NULL`/`IS NOT NULL`, and basic boolean
    logic (`AND`, `OR`, `NOT`).
- Align existing tests in `tests/test_utils/test_utils_sql.py` with the spec and
  treat them as contract tests.

## Impact

- New change defines behavior for an existing helper (`sql2pyarrow_filter`).
- No new dependencies; relies on `sqlglot` and `pyarrow.compute` as today.
- Tests already exist and will be extended/annotated as necessary.

