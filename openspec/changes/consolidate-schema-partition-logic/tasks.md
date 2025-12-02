## 1. Implementation

- [x] 1.1 Inventory current schema and partition helpers:
  - [x] 1.1.1 Identify the canonical dataset stats and grouping logic in `fsspeckit.core.maintenance`.
  - [x] 1.1.2 Identify schema compatibility/unification routines in `fsspeckit.datasets.pyarrow` and any similar logic in `fsspeckit.datasets.duckdb`.
  - [x] 1.1.3 Identify partition parsing implementations in `fsspeckit.common.misc` and any backend-specific partition handling.

- [x] 1.2 Introduce shared helper modules:
  - [x] 1.2.1 Create a schema helper module (e.g. `fsspeckit.common.schema`) that owns schema compatibility/unification and timezone-standardisation logic.
  - [x] 1.2.2 Create or extend a partition helper module (e.g. `fsspeckit.common.partitions`) that provides a canonical `get_partitions_from_path` and related utilities.

- [x] 1.3 Refactor backends to use the shared helpers:
  - [x] 1.3.1 Update `fsspeckit.datasets.pyarrow` to delegate schema and partition decisions to the shared helpers.
  - [x] 1.3.2 Update `fsspeckit.datasets.duckdb` to do the same, where appropriate.
  - [x] 1.3.3 Remove or simplify duplicated backend-specific schema/partition logic once the shared helpers are in place.

## 2. Testing

- [x] 2.1 Add tests for the new shared schema and partition helper modules to:
  - [x] 2.1.1 Cover common cases and edge cases for schema unification and timezone handling.
  - [x] 2.1.2 Cover partition parsing across different path styles and partition schemes.
- [x] 2.2 Add backend integration tests that verify DuckDB and PyArrow backends:
  - [x] 2.2.1 Produce the same maintenance/merge decisions for the same datasets.
  - [x] 2.2.2 Use partition and schema logic consistent with the shared helpers.

## 3. Documentation

- [x] 3.1 Document the new shared helpers in the relevant spec docs and developer documentation:
  - [x] 3.1.1 Indicate that schema-related code should go through the shared helper module rather than reimplementing logic in each backend.
  - [x] 3.1.2 Explain the canonical partition parsing semantics and how they are used across backends.

