# Change: Update `common/` and `sql/` Examples to Current APIs

## Why
The `examples/common/*` and `examples/sql/*` scripts are currently broken due to API drift:

- Logging: the examples assume `setup_logging()` returns a logger and supports kwargs it does not accept.
- Parallel processing: the examples use `time.get_ident()` (non-existent) instead of `threading.get_ident()`.
- Type conversion: the examples import functions that don’t exist in `fsspeckit.common.types`.
- SQL filters: `sql2pyarrow_filter/sql2polars_filter` now require an explicit `schema` argument; examples omit it.
- PyArrow table creation uses patterns that no longer work (`pa.table(list_of_dicts)`).

These should be kept small, correct, and runnable by default.

## What Changes
- Align logging examples with `fsspeckit.common.logging.setup_logging` + `get_logger`.
- Fix parallel processing example to use correct thread/process identifiers.
- Update type conversion example to import and use the currently exported conversion helpers.
- Update SQL examples to:
  - Create datasets with supported PyArrow APIs
  - Pass schemas into `sql2pyarrow_filter/sql2polars_filter`
  - Run offline using local datasets

## Impact
- Restores a working “utilities + SQL filters” story in examples.
- Reduces confusion about the public surface of `fsspeckit.common` and `fsspeckit.sql`.

