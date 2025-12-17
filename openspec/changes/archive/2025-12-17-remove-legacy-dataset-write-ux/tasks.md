## 0. Preconditions (sanity)
- [x] Confirm both backends expose the new explicit APIs:
  - `write_dataset(data, path, mode="append"|"overwrite", ...)`
  - `merge(data, path, strategy="insert"|"update"|"upsert", key_columns=..., ...)`

## 1. Specs (breaking cleanup)
- [x] Remove/modify requirements that describe the old `mode/strategy/rewrite_mode` UX in `utils-pyarrow`, `utils-duckdb`, and `core-ext`.
- [x] Add requirements documenting removal of legacy APIs (breaking change).

## 2. Call-site migration (tests + internal)
Goal: eliminate all usage of the legacy UX before deleting it.
- [x] Replace `write_parquet_dataset(...)` call sites with:
  - `write_dataset(...)` for append/overwrite writes
  - `merge(...)` for insert/update/upsert semantics
- [x] Replace `insert_dataset`/`upsert_dataset`/`update_dataset` wrappers with `merge(...)` (or remove usage).
- [x] Remove legacy-only tests that validate old semantics:
  - `tests/test_utils/test_mode_strategy_precedence.py`
  - `tests/test_utils/test_mode_strategy_compatibility.py`
  - `tests/test_incremental_rewrite.py` (rewrite_mode validation)
- [x] Update protocol tests (`tests/test_dataset_handler_protocol.py`) to validate the new UX instead of legacy function surfaces.

## 3. Protocol + public API reshaping
Goal: make the new UX the only supported public surface.
- [x] Replace/modify `src/fsspeckit/datasets/interfaces.py` to drop legacy methods/params:
  - remove `write_parquet_dataset(...)`
  - remove `merge_parquet_dataset(..., rewrite_mode=...)`
  - define a protocol centered on `write_dataset(...)` + `merge(...)` (plus maintenance ops).
- [x] Remove/stop exporting legacy re-exports that exist only for backward compatibility.

## 4. Code deletion (legacy surface removal)
- [x] Disable legacy `write_parquet_dataset(..., strategy, rewrite_mode, ...)` entrypoints in both backends:
  - `src/fsspeckit/datasets/pyarrow/io.py`
  - `src/fsspeckit/datasets/duckdb/dataset.py`
- [x] Disable convenience methods in both backends:
  - `insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset`
- [x] Disable legacy helper functions and stop exporting them:
  - `merge_parquet_dataset_pyarrow` (and related ext wrappers in `core/ext/dataset.py`)

Optional follow-ups (not required for behavior):
- [x] Physically delete dead legacy code blocks (currently unreachable due to `NotImplementedError`).

## 5. Docs
- [~] Update docs/tutorials to use `write_dataset` and `merge`.
- [~] Remove guidance about mode/strategy precedence and `rewrite_mode`.

> **Note**: Documentation updates are addressed in separate proposals and are no longer relevant to this change.

## Progress Notes (current reality)
- Specs deltas are complete.
- Legacy APIs are no longer used by tests/internal call sites and are disabled via `NotImplementedError`.
- Dead legacy code blocks have been physically deleted from both PyArrow and DuckDB backends.
- Tests updated to reflect modern API surface.
- **All implementation tasks completed.**
- Documentation updates are handled in separate proposals.
