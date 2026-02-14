# Change: Refactor dataset module for backend parity

## Why
The dataset module currently has diverging API surfaces and duplicated logic between the PyArrow and DuckDB backends. This makes it harder to maintain feature parity, reason about behavior, and provide consistent documentation and error handling. The refactor will unify the public dataset API, reduce duplication, and align optional dependency loading so users can switch backends without code churn.

## What Changes
- Standardize the dataset handler API (class-based) so PyarrowDatasetIO and DuckDBDatasetIO share identical method signatures and defaults.
- Consolidate shared validation, normalization, and error handling into shared helpers.
- Ensure canonical result types for write/merge operations across backends.
- Normalize conditional dependency loading and error messaging for dataset backends.
- Remove unused dataset code and add parity tests + documentation updates.

## Impact
- **Affected specs:** `datasets-parquet-io`, `datasets-conditional-loading`.
- **Affected code:** `src/fsspeckit/datasets/*`, `src/fsspeckit/core/*`, and dataset docs/tests.
- **Risk:** Cross-cutting refactor touching both backends; mitigated by parity tests and incremental changes.
