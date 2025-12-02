## Why

Schema-related and partition-related functionality is currently spread across several modules:

- `fsspeckit.core.maintenance` contains canonical dataset stats and grouping logic.
- `fsspeckit.datasets.pyarrow` and `fsspeckit.datasets.duckdb` each contain their own schema and maintenance helpers.
- Partition parsing appears in `fsspeckit.common.misc.get_partitions_from_path` and is conceptually tied to dataset operations.

This distribution leads to:

- Duplicated or near-duplicated implementations for schema compatibility and unification.
- Slightly different behaviour between backends in areas that should be unified.
- Difficulty in knowing where the “authoritative” implementation lives.

## What Changes

- Consolidate schema and partition logic so that:
  - Schema compatibility/unification and timezone alignment routines live in clearly named shared helpers (`common` or `core`).
  - Both PyArrow and DuckDB dataset backends reuse those helpers instead of maintaining duplicate logic.
  - Partition parsing routines are centralised and documented, with a single canonical implementation used by dataset helpers and other consumers.

## Impact

- **Behaviour:**
  - DuckDB and PyArrow backends will make schema and partition decisions consistently, reducing surprises when switching backends.
  - Tests can target the shared helpers to validate behaviour once, rather than duplicating test logic per backend.

- **Specs affected:**
  - `utils-pyarrow` (schema handling and maintenance).
  - `utils-duckdb` (schema handling and maintenance).
  - `utils-dtype` (partition parsing and type conversion).

