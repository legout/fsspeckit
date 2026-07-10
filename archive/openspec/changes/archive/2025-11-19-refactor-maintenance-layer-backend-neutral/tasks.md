# Implementation Tasks: Backend-Neutral Maintenance Layer

## 1. Design & Spec Deltas

- [x] 1.1 Finalize maintenance design in `design.md` and align with current specs.
- [x] 1.2 Inventory compaction and optimize requirements in `openspec/specs/utils-duckdb/spec.md`.
- [x] 1.3 Inventory compaction and optimize requirements in `openspec/specs/utils-pyarrow/spec.md`.
- [x] 1.4 Draft MODIFIED requirements to describe shared stats structure and streaming constraints.
- [x] 1.5 Run `openspec validate refactor-maintenance-layer-backend-neutral --strict` and fix spec/delta issues.

## 2. Backend-Neutral Maintenance Core

- [x] 2.1 Create `src/fsspeckit/core/maintenance.py` (or similar) for shared logic.
- [x] 2.2 Extract dataset discovery/statistics behavior from `collect_dataset_stats_pyarrow` into a reusable helper that can serve both backends.
- [x] 2.3 Implement a shared compaction grouping function that:
  - Accepts file descriptors (`path`, `size_bytes`, `num_rows`) plus thresholds.
  - Returns compaction groups and a stats "plan" (`planned_groups`, counts, bytes).
- [x] 2.4 Implement a shared optimization planning helper that:
  - Validates `zorder_columns` against a sample schema.
  - Returns grouping and planning metadata without reading all data into memory.
- [x] 2.5 Define a canonical stats dictionary shape for compaction and optimization and implement helper constructors to produce it.
- [x] 2.6 Add unit tests for the new maintenance core (based on synthetic file descriptors, independent of DuckDB/PyArrow IO).

## 3. DuckDB Maintenance Refactor

- [x] 3.1 Refactor `DuckDBParquetHandler._collect_dataset_stats` to defer to the new core maintenance stats helper (or remove it if redundant).
- [x] 3.2 Refactor `compact_parquet_dataset` to:
  - Use shared grouping logic and stats plan.
  - Focus on reading/writing group files via DuckDB/Arrow, not computing groups.
  - Preserve existing dry-run behavior and partition filter semantics.
- [x] 3.3 Refactor `optimize_parquet_dataset` to:
  - Use shared optimization plan for grouping and z-order column validation.
  - Avoid fully-reading the filtered dataset into one Arrow table where possible; instead process per-group and write `optimized-*.parquet` files.
  - Maintain or improve correctness for partition filters and stats.
- [x] 3.4 Update `tests/test_utils/test_duckdb.py` maintenance tests to:
  - Assert canonical stats keys and semantics.
  - Cover edge cases: no files, filters that match nothing, invalid thresholds, and "already optimized" no-op scenarios.

## 4. PyArrow Maintenance Refactor

- [x] 4.1 Refactor `collect_dataset_stats_pyarrow` to live in or use the core maintenance stats helper.
- [x] 4.2 Refactor `compact_parquet_dataset_pyarrow` to use the shared grouping logic, while keeping:
  - group-by-group streaming semantics, and
  - dry-run planning and stats structure.
- [x] 4.3 Refactor `optimize_parquet_dataset_pyarrow` to:
  - Use the shared optimization planner and stats helpers.
  - Replace the "concat all tables" approach with a per-group streaming approach where feasible.
  - Maintain dry-run capabilities and `zorder_columns` checks.
- [x] 4.4 Update `tests/test_utils/test_utils_pyarrow.py` maintenance tests to assert canonical stats structure and any tightened streaming guarantees.

## 5. Documentation & Examples

- [x] 5.- [ ] 5.1 Update docstrings for DuckDB and PyArrow maintenance helpers to describe shared stats and streaming behavior.
- [x] 5.- [ ] 5.2 Update relevant docs in `docs/utils.md` / `docs/api` to explain the backend-neutral maintenance planning.
- [x] 5.- [ ] 5.3 Add or adjust maintenance examples in `examples/duckdb/` and `examples/pyarrow/` to showcase compaction and optimization using both backends.

## 6. Validation

- [x] 6.- [ ] 6.1 Run `pytest tests/test_utils/test_duckdb.py -k "compact or optimize" -v`.
- [x] 6.- [ ] 6.2 Run `pytest tests/test_utils/test_utils_pyarrow.py -k "compact or optimize" -v`.
- [x] 6.- [ ] 6.3 Run `openspec validate refactor-maintenance-layer-backend-neutral --strict`.
- [x] 6.- [ ] 6.4 Optionally run simple benchmarks to confirm that optimize operations no longer require full dataset materialization for realistic workloads.

