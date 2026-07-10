# Implementation Tasks: Backend-Neutral Merge Layer

## 1. Design & Spec Deltas

- [x] 1.1 Finalize backend-neutral merge design in `design.md` and review against current specs.
- [x] 1.2 Identify all merge-related requirements in `openspec/specs/utils-duckdb/spec.md`.
- [x] 1.3 Identify all merge-related requirements in `openspec/specs/utils-pyarrow/spec.md`.
- [x] 1.4 Draft MODIFIED requirements for `utils-duckdb` to reference shared semantics, streaming, and stats.
- [x] 1.5 Draft MODIFIED requirements for `utils-pyarrow` to reference shared semantics, streaming, and stats.
- [x] 1.6 Run `openspec validate refactor-merge-layer-backend-neutral --strict` and fix delta/spec formatting issues.

## 2. Backend-Neutral Merge Core

- [x] 2.1 Create `src/fsspeckit/core/merge.py` (or similar module) for backend-neutral helpers.
- [x] 2.2 Implement helper to normalize and validate `key_columns` input.
- [x] 2.3 Implement helper for NULL-key detection on a generic "table descriptor" abstraction.
- [x] 2.4 Implement schema compatibility helper that can be reused by both DuckDB and PyArrow backends.
- [x] 2.5 Implement strategy semantics helpers that define which records are considered inserts, updates, deletes for each strategy.
- [x] 2.6 Implement a small statistics object / helper that accumulates `"inserted"`, `"updated"`, `"deleted"`, `"total"` from actual operations.
- [x] 2.7 Add focused unit tests for `core.merge` helpers (pure Python / PyArrow), independent of DuckDB or PyArrow datasets.

## 3. DuckDB Backend Refactor

- [x] 3.1 Refactor `DuckDBParquetHandler.merge_parquet_dataset` to call backend-neutral merge helpers for:
  - key normalization and validation,
  - schema compatibility checks,
  - stats calculation.
- [x] 3.2 Replace full-table `read_parquet(target_path)` usage with a streaming / SQL-based approach that operates directly on the dataset on disk where feasible.
- [x] 3.3 Ensure merge execution preserves or improves atomicity (e.g. by writing to a temporary dataset directory and swapping).
- [x] 3.4 Align edge-case behavior (empty target, empty source, invalid strategy) with the shared semantics.
- [x] 3.5 Adjust `_calculate_merge_stats` or its replacement to use the new shared statistics helper.
- [x] 3.6 Update `tests/test_utils/test_duckdb.py` to assert:
  - statistics correctness for all strategies,
  - expected behavior for empty-target/empty-source cases,
  - failure modes for invalid keys and schema incompatibility.

## 4. PyArrow Backend Refactor

- [x] 4.1 Refactor `merge_parquet_dataset_pyarrow` to use the same backend-neutral merge helpers for:
  - key normalization and validation,
  - schema compatibility checks,
  - stats calculation.
- [x] 4.2 Remove or simplify the current "filtered scanner touch" shim and ensure filtered scanners are used in the main merge logic.
- [x] 4.3 Confirm that the implementation never calls `dataset.to_table()` on the full target dataset and adheres to streaming/batch constraints from the spec.
- [x] 4.4 Align edge-case behavior (empty target, empty source, invalid strategy) with the shared semantics.
- [x] 4.5 Update `tests/test_utils/test_pyarrow_dataset_merge.py` to assert:
  - statistics correctness for all strategies,
  - use of filtered scanners (via existing monkeypatch pattern or improved equivalent),
  - consistent behavior with DuckDB for matching scenarios.

## 5. Documentation & Examples

- [x] 5.1 Update docstrings for DuckDB and PyArrow merge helpers to reference shared semantics and clarifications from the design.
- [x] 5.2 Add or update documentation in `docs/utils.md` / `docs/api` to describe backend-neutral merge behavior and differences (if any) between backends.
- [x] 5.3 Add or update examples in `examples/duckdb/` and `examples/pyarrow/` to demonstrate equivalent merge scenarios using both backends.

## 6. Validation

- [x] 6.1 Run `pytest tests/test_utils/test_duckdb.py tests/test_utils/test_pyarrow_dataset_merge.py -v`.
- [x] 6.2 Run broader utility tests (`pytest tests/test_utils -k "duckdb or pyarrow" -v`) to catch regressions.
- [x] 6.3 Run `openspec validate refactor-merge-layer-backend-neutral --strict`.
- [x] 6.4 Optionally run performance benchmarks on representative large datasets to confirm streaming and scaling properties.

