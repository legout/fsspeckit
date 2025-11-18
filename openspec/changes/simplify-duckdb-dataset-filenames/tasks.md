# Implementation Tasks: Simplify DuckDB Dataset Filename Requirements

## 1. Spec Deltas

- [x] 1.1 Review `openspec/specs/utils-duckdb/spec.md` sections related to `write_parquet_dataset` and filename strategies.
- [x] 1.2 Draft MODIFIED requirements that:
  - Describe UUID-based unique filenames as the primary requirement.
  - Clarify the role of `basename_template` without committing to timestamp or sequential semantics.
  - Remove or reframe scenarios that require explicit timestamp- or sequence-based naming.
- [x] 1.3 Run `openspec validate simplify-duckdb-dataset-filenames --strict` and fix any spec issues.

## 2. Documentation Alignment

- [x] 2.1 Update `DuckDBParquetHandler.write_parquet_dataset` docstring in `src/fsspeckit/utils/duckdb.py` to match the simplified spec:
  - Explain the UUID-based naming.
  - Explain how `basename_template` is used.
  - Remove references to explicit timestamp/sequence semantics if present.
- [x] 2.2 Review examples in `examples/duckdb/` and `docs/` for any references to specific sequential filenames and adjust them to emphasize uniqueness rather than exact numbering.

## 3. Tests Review

- [x] 3.1 Review `tests/test_utils/test_duckdb.py` tests related to dataset writes:
  - Confirm tests assert uniqueness and basic prefix behavior, not specific sequential or timestamped file names.
  - Update any tests that rely on stronger assumptions so they match the simplified spec.

## 4. Validation

- [x] 4.1 Run `pytest tests/test_utils/test_duckdb.py -k "dataset" -v`.
- [x] 4.2 Run `openspec validate simplify-duckdb-dataset-filenames --strict`.

