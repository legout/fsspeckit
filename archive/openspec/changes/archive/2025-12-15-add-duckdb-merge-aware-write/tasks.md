## 1. Implementation

- [x] 1.1 Extend `DuckDBDatasetIO.write_parquet_dataset` (in the canonical `fsspeckit.datasets.duckdb` package module)
      to accept optional `strategy` and `key_columns`, targeting the package-based layout introduced by
      `refactor-module-layout-packages` rather than any legacy shim modules.
- [x] 1.2 When `strategy` is provided, route through the existing merge logic (or shared helper) rather than plain write.
- [x] 1.3 Add convenience methods `insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset` on `DuckDBDatasetIO` and ensure `DuckDBParquetHandler` re-exports them.
- [x] 1.4 Preserve legacy behaviour when `strategy` is None.

## 2. Testing

- [x] 2.1 Add tests per strategy through the write path (insert, upsert, update, deduplicate, full_merge).
- [x] 2.2 Add tests for convenience helpers enforcing `key_columns` when required.

## 3. Documentation

- [x] 3.1 Update docs/examples to show merge-aware writes via DuckDB and the new shortcut helpers.
- [x] 3.2 Clarify key column requirements and compression/partition interactions when using strategies.
