# Status: 🟢 FULLY IMPLEMENTED

## What Exists (New UX)
- Explicit dataset write API: `write_dataset(mode="append"|"overwrite")` for both backends.
- Explicit incremental merge API: `merge(strategy="insert"|"update"|"upsert")` for both backends.

## What Changed (Legacy UX removal)
- Legacy dataset write/merge entrypoints are disabled via `NotImplementedError` and no longer exported/registered.
- Tests and internal call sites are migrated to `write_dataset(...)` / `merge(...)`.
- Protocol `src/fsspeckit/datasets/interfaces.py` now models only the new UX.
- Dead legacy code blocks have been physically deleted from both PyArrow and DuckDB backends.
- Tests updated to validate modern API surface and legacy method disabling.

## Next Step
- No remaining tasks for this change (documentation is handled in separate proposals).
