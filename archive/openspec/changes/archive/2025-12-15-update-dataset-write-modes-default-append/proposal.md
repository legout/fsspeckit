# Proposal: Default Append Dataset Writes + Write Modes

## Why
`write_parquet_dataset` currently cannot reliably support file-level write modes:
- DuckDB handler does not implement `mode="append"`/`mode="overwrite"` despite being specified and tested.
- PyArrow handler uses a deterministic default `basename_template` and `existing_data_behavior` that can overwrite prior files, making “append” unsafe.

This makes repeated writes risky (silent overwrite) and prevents users from choosing safe default behavior.

## What Changes
- Add a `mode` keyword argument to dataset writes across handlers:
  - `mode in {"append", "overwrite"}`
  - Default `mode="append"` for safety (avoid accidental data loss).
- Define clear interaction rules between `mode` and merge `strategy`:
  - `mode="append"` is rejected for rewrite strategies (`upsert`, `update`, `full_merge`, `deduplicate`).
  - `strategy="insert"` MAY use `mode="append"` as an optimization: write only new rows (by `key_columns`) to a new file without rewriting existing files.
- Ensure append writes are collision-free by default:
  - DuckDB writes UUID-based parquet filenames per write (and per split chunk).
  - PyArrow default `basename_template` becomes unique per write call.
- Keep overwrite semantics non-destructive to non-parquet files:
  - `mode="overwrite"` deletes `**/*.parquet` under the dataset path but preserves other files (e.g. `README.txt`, `_metadata`).

## Impact
- Behavior change: calling `write_parquet_dataset(...)` repeatedly without `mode=` now appends by default instead of replacing/overwriting files.
  - Migration path: pass `mode="overwrite"` to restore the previous “replace” intent.
- Users relying on deterministic file names must explicitly set `basename_template` (and accept collision rules).
- Merge strategies become stricter when combined with `mode="append"` (explicit errors instead of surprising rewrites).

## Out of Scope
- Adding new merge strategies or changing merge semantics.
- Adding a dedicated dataset deduplication maintenance API (handled in a separate change).

