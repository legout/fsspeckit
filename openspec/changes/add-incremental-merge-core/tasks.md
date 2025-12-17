## 1. Specs
- [x] Add `datasets-parquet-io` requirements for incremental merge planning, pruning, and invariants.

## 2. Shared internals
- [x] Implement dataset file listing + hive partition parsing helper (filesystem-agnostic).
- [x] Implement parquet metadata extraction helper compatible with current PyArrow and DuckDB.
- [x] Implement candidate pruning:
  - partition pruning (when partition values are known)
  - stats pruning (when exact key stats are known)
  - conservative fallback when unknown/inexact
- [x] Implement affected-file confirmation by scanning only `key_columns`.
- [x] Implement staging directory + atomic-ish replace helper for rewritten files.

## 3. Validation helpers
- [x] Reject merges where source has null keys in `key_columns`.
- [x] Reject merges that would change partition columns for existing keys.
