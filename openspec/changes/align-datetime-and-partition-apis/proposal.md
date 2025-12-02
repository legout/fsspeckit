## Why

Datetime and partition helper functions do not currently match the behaviour expected by tests and by users:

- `get_timestamp_column` only works for Polars/Arrow, but tests call it with pandas DataFrames and expect timestamp columns to be detected.
- `get_timedelta_str` raises errors for unknown units instead of falling back to a reasonable string representation.
- `get_partitions_from_path` returns a list-of-tuples and requires an explicit `partitioning` argument, whereas tests expect Hive-style parsing and a dictionary result by default, including working on Windows-style and relative paths.

These inconsistencies produce broken tests and surprising behaviour for callers.

## What Changes

- Extend `get_timestamp_column` to support pandas DataFrames by converting them to Polars or Arrow using the existing optional-dependency helpers.
- Make `get_timedelta_str` robust to unknown units, returning `"value unit"` (or a sensible variant) instead of raising.
- Redefine `get_partitions_from_path` so that:
  - The default behaviour (`partitioning is None`) is to interpret the path as Hive-style `key=value` partitions.
  - The function returns a `dict[str, str]` mapping partition keys to their values.
  - Paths are normalised (e.g. via `Path(path).as_posix()`) so that Windows-style and relative paths behave consistently.

## Impact

- **Behaviour:**  
  - `get_timestamp_column` becomes more broadly useful and matches its tests and documentation.  
  - `get_timedelta_str` no longer raises on unknown units, improving robustness when strings come from external sources.  
  - `get_partitions_from_path` behaves as tests expect and works across different path styles.

- **Specs affected:**  
  - `utils-dtype` (datetime parsing and type conversion utilities).

- **Code affected (non-exhaustive):**
  - `src/fsspeckit/common/datetime.py`
  - `src/fsspeckit/common/misc.py`

