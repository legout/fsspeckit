# Status: 🟡 MOSTLY IMPLEMENTED (docs pending)

## What Exists (New UX)
- Explicit dataset write API: `write_dataset(mode="append"|"overwrite")` for both backends.
- Explicit incremental merge API: `merge(strategy="insert"|"update"|"upsert")` for both backends.

## What Changed (Legacy UX removal)
- Legacy dataset write/merge entrypoints are disabled via `NotImplementedError` and no longer exported/registered.
- Tests and internal call sites are migrated to `write_dataset(...)` / `merge(...)`.
- Protocol `src/fsspeckit/datasets/interfaces.py` now models only the new UX.

## Next Step
- Update docs/examples to remove references to `write_parquet_dataset`, `rewrite_mode`, and legacy ext helpers.
