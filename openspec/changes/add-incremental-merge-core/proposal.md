# Change: Add incremental merge core (planning + affected-file rewrite)

## Why
Merge-aware dataset operations require a correct, incremental implementation that rewrites only affected files and adds
new files for inserts. Today, “incremental” is either incomplete (DuckDB) or degrades to full dataset rewrites (PyArrow).

## What Changes
- Add shared internals to support incremental merges:
  - Hive partition parsing from dataset paths
  - Parquet metadata extraction for pruning (min/max/null_count, conservative fallbacks)
  - Candidate pruning using partition info and stats
  - Confirmation of actually affected files via key intersection scans
  - Staging + replace mechanics for safe per-file rewrites
- Define merge invariants for upcoming implementations:
  - Full-row replacement for matching keys
  - Partition columns MUST NOT change for existing keys (reject row moves)

## Scope
This change builds the **backend-neutral core** and does not yet expose a public `merge(...)` API.

## Impact
- Introduces new internal modules/helpers used by both PyArrow and DuckDB merge implementations.
- Enables correct “rewrite only affected files” semantics in subsequent changes.

