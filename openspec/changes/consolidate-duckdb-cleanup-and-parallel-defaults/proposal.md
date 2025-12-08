# Change: Consolidate DuckDB cleanup helpers and clarify parallel defaults

## Why

There is duplicated cleanup logic for DuckDB tables (`_unregister_duckdb_table_safely`) across multiple modules, which
increases maintenance cost and the risk of behavioural drift. At the same time, some CSV/Parquet helpers default to
threaded execution that depends on joblib, creating surprising `ImportError` failures in environments without the
`fsspeckit[datasets]` extra installed.

These concerns both relate to making error handling and execution defaults predictable and easy to reason about.

## What Changes

- Consolidate DuckDB cleanup helpers by:
  - Moving `_unregister_duckdb_table_safely` into a single canonical helpers module under the
    `fsspeckit.datasets.duckdb` package introduced by the module-layout refactor.
  - Importing and using this helper from all DuckDB-related modules that need it (targeting the package-based layout,
    not legacy shim modules).
- Clarify parallel execution defaults by:
  - Ensuring CSV/Parquet read helpers only require joblib when `use_threads=True`.
  - Optionally setting `use_threads=False` as the default to keep base behaviour serial and avoid surprising failures.
  - Ensuring that when parallel execution is requested without joblib, the error message clearly explains how to enable it.

## Impact

- Reduces duplication in DuckDB cleanup logic and keeps behaviour consistent with the centralised error-handling specs.
- Makes parallel execution an explicit opt-in, aligning with the `core-lazy-imports` requirement that joblib is optional.
- No changes to public signatures, but some defaults may be clarified or adjusted for predictability.
