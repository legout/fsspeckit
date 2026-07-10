# Change Proposal: Refactor Merge Layer to Backend-Neutral Semantics

## Why

Current merge functionality is implemented separately in the DuckDB and PyArrow backends:

- `DuckDBParquetHandler.merge_parquet_dataset` reads the full target dataset into a PyArrow table, performs an in-memory SQL-style merge, then rewrites the dataset. This contradicts the “larger-than-memory” intent of the DuckDB spec, makes behavior harder to reason about for large datasets, and complicates atomicity.
- `merge_parquet_dataset_pyarrow` in `utils.pyarrow` uses PyArrow datasets, but still performs an unfiltered full scan of the target during the main merge loop and only uses filtered scanners in a “touch” phase to satisfy tests/spec wording. This under-delivers on the spec’s “filtered, batch-oriented” requirement.
- Merge statistics (`inserted`, `updated`, `deleted`, `total`) are computed differently between backends and do not always reflect actual operations (e.g., DuckDB’s UPDATE strategy counts all existing rows as “updated”).
- Edge-case semantics diverge between backends (empty target with UPDATE, FULL_MERGE with empty source, schema incompatibility behavior), even though the specs say PyArrow semantics should match the DuckDB capability.
- Both backends re-implement similar concerns (key normalization, key/schema validation, NULL-key checks, stats calculation) in slightly different ways, making it easy for behavior to drift and harder to add new strategies or constraints consistently.

This proposal introduces a backend-neutral merge layer that centralizes merge semantics, validation, and statistics, while allowing DuckDB and PyArrow to provide their own execution engines.

## What Changes

- Add a backend-neutral merge planning layer under `src/fsspeckit/core/` (e.g. `core/merge.py`) that:
  - Normalizes and validates merge inputs (key columns, strategy, source/target descriptors).
  - Encodes canonical semantics for all strategies (`upsert`, `insert`, `update`, `full_merge`, `deduplicate`), including how edge cases behave.
  - Computes merge statistics from the actual operations instead of backend-specific heuristics.
  - Provides small, testable helpers for key normalization, NULL-key checks, schema compatibility checks, and strategy-specific row-selection logic.
- Refactor `DuckDBParquetHandler.merge_parquet_dataset` to:
  - Delegate validation and statistics calculation to the backend-neutral merge layer.
  - Use DuckDB SQL to execute the merge on the target dataset without materializing the entire dataset as a single PyArrow table when not required.
  - Preserve or improve atomicity guarantees (e.g. via write-to-temp-then-swap or equivalent patterns).
- Refactor `merge_parquet_dataset_pyarrow` to:
  - Delegate validation and statistics calculation to the backend-neutral merge layer, reusing the same strategy semantics as DuckDB.
  - Use key-filtered `pyarrow.dataset.Scanner` queries as the primary way to touch the target (not only in a preflight “touch” phase).
  - Maintain the current streaming / batch-oriented design but align it with the canonical semantics and stats.
- Align edge-case behavior across backends, including:
  - Empty target dataset with `strategy="update"` or `strategy="insert"`.
  - FULL_MERGE behavior when the source is empty (including correct `deleted` counts).
  - Schema incompatibility handling: when to fail with a descriptive error versus when to rely on `unify_schemas`-style promotion.
- Update `openspec/specs/utils-duckdb/spec.md` and `openspec/specs/utils-pyarrow/spec.md` with MODIFIED requirements:
  - Clearly specifying shared merge semantics, streaming constraints, and statistics correctness.
  - Referencing backend-neutral merge semantics so future backends (e.g. pure SQL engines) can adopt the same behavior.

## Impact

- **Affected specs**
  - `utils-duckdb` (merge semantics, performance and stats requirements).
  - `utils-pyarrow` (merge semantics, streaming and filter requirements).
- **Affected code**
  - New: `src/fsspeckit/core/merge.py` (or similar) for backend-neutral logic.
  - Modified: `src/fsspeckit/utils/duckdb.py` (`merge_parquet_dataset` and related helpers).
  - Modified: `src/fsspeckit/utils/pyarrow.py` (`merge_parquet_dataset_pyarrow` and helpers such as `_ensure_no_null_keys_table`, `_ensure_no_null_keys_dataset`, `_write_tables_to_dataset`).
  - Modified tests: `tests/test_utils/test_duckdb.py`, `tests/test_utils/test_pyarrow_dataset_merge.py` (to assert aligned semantics and statistics).
- **Behavioral impact**
  - External public API signatures remain the same, but:
    - Merge operations become more scalable and predictable for large datasets.
    - Statistics become accurate and consistent across backends.
    - Edge cases and error conditions become aligned and documented.
- **Risk / rollout**
  - Medium: behavior and stats changes will be visible in some edge cases; strong tests and spec alignment will mitigate.
  - Local-only refactor inside `fsspeckit`; no external dependencies or network behavior changes.

