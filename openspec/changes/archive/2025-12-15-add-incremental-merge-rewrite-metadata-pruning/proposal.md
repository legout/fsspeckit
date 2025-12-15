# Proposal: Incremental Merge Rewrite via Parquet Metadata Pruning

## Why
Current `strategy="upsert"` and `strategy="update"` workflows rewrite the full dataset output, even when only a small number of keys change. For large datasets with many files, this is unnecessarily expensive.

Parquet datasets provide metadata (partitioning layout and per-file/per-row-group statistics, optionally bloom filters) that can be used to conservatively identify which files might contain the keys being updated.

## What Changes
- Add an opt-in incremental rewrite mode for merge-aware writes and merges:
  - `rewrite_mode in {"full", "incremental"}` (default: `"full"` for backward compatibility).
- When `rewrite_mode="incremental"` and `strategy in {"upsert","update"}`:
  - Identify candidate/affected parquet files using partition pruning first (when applicable).
  - Use parquet metadata to conservatively prune files that cannot contain any of the source keys.
  - Rewrite only affected files; preserve all other parquet files byte-for-byte and keep them in place.
  - Write new parquet file(s) for inserted rows (UPSERT only).
- Disallow incremental rewrite for full dataset sync strategies:
  - `strategy in {"full_merge","deduplicate"}` SHALL reject `rewrite_mode="incremental"` because correctness requires a full rewrite.

## Impact
- New optional parameter; default behavior remains a full rewrite.
- Significant performance improvement for small updates on large datasets when metadata/partitioning enables pruning.
- Correctness is preserved by requiring conservative pruning: if a file cannot be proven unaffected, it is treated as affected (may degenerate to rewriting many files).

## Dependencies
- Complements `update-dataset-write-modes-default-append` (mode/strategy validation and safer default writes) but can be implemented independently.

