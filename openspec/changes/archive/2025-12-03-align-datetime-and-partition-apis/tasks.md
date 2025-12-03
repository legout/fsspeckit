## 1. Implementation

- [x] 1.1 Update `get_timestamp_column` to recognise pandas DataFrames and normalise them to a Polars/Arrow representation before applying selectors.
- [x] 1.2 Adjust `get_timedelta_str` to fall back to a `"value unit"` style string for unknown units, in both the Polars and DuckDB directions, without raising `KeyError`.
- [x] 1.3 Redefine `get_partitions_from_path` to:
  - [x] 1.3.1 Normalise the input path using `pathlib.Path` (or equivalent) to handle Windows and relative paths.
  - [x] 1.3.2 Default to Hive-style `key=value` parsing when `partitioning is None`.
  - [x] 1.3.3 Return a `dict[str, str]` mapping partition keys to their values, aligning with existing tests.

## 2. Testing

- [x] 2.1 Extend datetime tests to cover pandas DataFrames in `get_timestamp_column` and ensure results match expectations.
- [x] 2.2 Add tests for `get_timedelta_str` using invalid or unknown units, asserting graceful fallback behaviour.
- [x] 2.3 Extend partition tests to cover:
  - [x] 2.3.1 Windows-style paths (e.g. `C:\data\year=2023\month=12\file.parquet`).
  - [x] 2.3.2 Relative paths.
  - [x] 2.3.3 Paths with mixed or edge-case partition components.

## 3. Documentation

- [x] 3.1 Clarify in docstrings and any README snippets how `get_timestamp_column` behaves with pandas, Polars, and PyArrow objects.
- [x] 3.2 Document the fallback behaviour of `get_timedelta_str` for unknown units.
- [x] 3.3 Document the default Hive-style behaviour and return type of `get_partitions_from_path`.

