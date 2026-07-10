## 1. Implementation

- [x] 1.1 Review `sql2pyarrow_filter` in `src/fsspeckit/utils/sql.py` and ensure
        it consistently returns `pyarrow.compute.Expression` objects compatible
        with `pyarrow.dataset.Scanner(filter=...)`.
- [x] 1.2 Verify correct handling of:
        - equality and inequality comparisons (`=`, `!=`, `>`, `>=`, `<`, `<=`)
        - `IN` / `NOT IN` lists
        - `IS NULL` / `IS NOT NULL`
        - boolean literals (`true`, `false`)
        - datetime/date/time literals for columns with timestamp/date/time
          PyArrow types.
- [x] 1.3 Ensure that implementation does not rely on DuckDB-specific behavior
        and can be used directly with PyArrow datasets for row-level pruning.

## 2. Testing

- [x] 2.1 Treat `tests/test_utils/test_utils_sql.py` as contract tests for
        `sql2pyarrow_filter` and extend them where necessary to cover:
        - Use of the returned expression as a dataset filter
          (for example, apply `expr` via `dataset.to_table(filter=expr)`).
        - Edge cases around booleans and time-based columns under current
          PyArrow versions.
- [x] 2.2 Add a small dataset-level test that:
        - Builds a `pyarrow.dataset.Dataset` from a temp parquet directory.
        - Uses `sql2pyarrow_filter` to construct a filter expression.
        - Applies filter via `scanner.to_table(filter=expr)` and asserts
          that the filtered rows match expectations.

## 3. Documentation

- [x] 3.1 Add a short section in `docs/utils.md` or a dedicated SQL utilities
        doc describing `sql2pyarrow_filter`, its supported SQL subset, and how
        to use it with `pyarrow.dataset`.

## 4. Validation

- [x] 4.1 Run `pytest tests/test_utils/test_utils_sql.py`.
- [x] 4.2 Run `ruff check src/fsspeckit/utils/sql.py`.
- [x] 4.3 Run `mypy --ignore-missing-imports src/fsspeckit/utils/sql.py`.
- [x] 4.4 Validate OpenSpec:
        `openspec validate add-pyarrow-sql-filter-interop --strict`.

