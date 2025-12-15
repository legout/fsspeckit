## 1. Implementation

- [x] 1.1 Extend `write_pyarrow_dataset` to accept optional `strategy` and `key_columns`, implementing the changes in the
      package-based layout introduced by `refactor-module-layout-packages` (i.e. in the canonical ext/PyArrow modules,
      not in legacy shim files).
- [x] 1.2 When `strategy` is provided, reuse `merge_parquet_dataset_pyarrow` logic (or extracted shared helper) to apply the merge semantics directly on the incoming data.
- [x] 1.3 Add filesystem convenience helpers (`insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset`) that call `write_pyarrow_dataset` with the appropriate strategy and require `key_columns` when necessary.
- [x] 1.4 Keep existing behaviour intact when `strategy` is None; ensure backward compatibility for current callers.

## 2. Testing

- [x] 2.1 Add tests for each strategy path via `write_pyarrow_dataset`:
  - [x] 2.1.1 Insert only new rows
  - [x] 2.1.2 Upsert behaviour with key columns
  - [x] 2.1.3 Update-only behaviour with key columns
  - [x] 2.1.4 Deduplicate behaviour (with and without key columns)
- [x] 2.2 Add tests for convenience helpers to ensure they delegate correctly and enforce required `key_columns`.

## 3. Documentation

- [x] 3.1 Update API docs and how-to guides to illustrate merge-aware writes and the new convenience helpers.
- [x] 3.2 Document required extras (PyArrow) and key-column requirements for strategies that need them.
