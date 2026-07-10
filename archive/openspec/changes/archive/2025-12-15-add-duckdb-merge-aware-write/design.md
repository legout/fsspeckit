## Context
`write_parquet_dataset` currently writes data without merge semantics. Merge strategies exist in `merge_parquet_dataset`,
which operates on existing parquet datasets referenced by path. To match PyArrow UX plans and reduce friction, callers
should be able to apply merge strategies directly when writing in-memory data with DuckDB.

## Goals / Non-Goals
- Goals:
  - Add optional `strategy` + `key_columns` to `write_parquet_dataset`.
  - Provide convenience helpers mirroring strategy names.
  - Keep backward compatibility when `strategy` is omitted.
- Non-Goals:
  - Changing merge semantics already defined in `merge_parquet_dataset`.
  - Introducing strategies beyond the existing set.

## Decisions
- When `strategy` is provided, stage the incoming data (temporary table) and use the existing DuckDB merge pipeline
  rather than duplicating merge logic.
- Add helper methods on `DuckDBDatasetIO` and ensure `DuckDBParquetHandler` re-exports them for parity with PyArrow.
- Keep default behaviour identical when `strategy` is None.

## Risks / Trade-offs
- Additional code paths in the write function; mitigated by reusing existing merge machinery.
- Slightly larger public surface; mitigated with concise docs/examples.

## Migration Plan
1) Extend signature and branch on `strategy` presence.
2) Add helper methods and re-export.
3) Add tests per strategy and for helpers.
4) Update docs and examples.

## Open Questions
- Should we allow specifying a staging temp view/table name to aid debugging? (Out of scope for now.)
