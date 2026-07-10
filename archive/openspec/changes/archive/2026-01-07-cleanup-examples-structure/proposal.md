# Change: Cleanup Examples Structure (Remove Obsolete/Legacy Examples)

## Why
The current `examples/` tree has drifted from the actual `fsspeckit` APIs and runtime behavior:

- Multiple example sets are duplicated (legacy `duckdb/`, `pyarrow/` vs the newer `datasets/*` learning path).
- Some examples reference APIs that no longer exist (e.g. `fs.pydala_dataset`, `DuckDBParquetHandler.register_dataset`, `validate_sql_query`).
- Some examples require personal credentials/profiles or network access by default, making them non-runnable in a clean/offline environment.
- The top-level `examples/README.md` describes directories that don’t exist or don’t match the current repo layout.

This creates a poor first-run experience and makes the example suite hard to maintain.

## What Changes
- Remove obsolete/legacy example directories and files that are:
  - Not runnable without private credentials or external services by default
  - Based on APIs that are no longer supported
  - Duplicated by other examples that will be kept and updated
- Update example documentation to reflect the remaining, supported example set.
- Simplify example dependency guidance so users can install only what the kept examples require.

## Scope (Proposed Removals)
Remove these example groups (or migrate them to docs-only narrative if needed):

- `examples/__pydala_dataset/` (references non-existent `fs.pydala_dataset`)
- `examples/deltalake_delta_table/` (hard-coded personal AWS profile and external S3 path)
- `examples/s3_pyarrow_dataset/` (network/credentials by default; overlaps with other examples)
- `examples/duckdb/` (legacy duplicates; heavily out-of-date with current DuckDB handler APIs)
- `examples/pyarrow/` (legacy duplicates; out-of-date with current PyArrow dataset APIs)
- `examples/cross_domain/` (references SQL APIs that do not exist in current implementation)
- `examples/datasets/advanced/` (currently broken and/or too heavy; candidate for docs-only rewrite later)

## Impact
- Reduces maintenance burden by removing duplicated and broken example code.
- Restores a “runnable by default (offline)” baseline for the example suite.
- Makes the remaining examples easier to discover and keep in sync with the project APIs.

