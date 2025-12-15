## Context
`write_pyarrow_dataset` currently writes datasets without merge semantics. Merge strategies exist separately via
`merge_parquet_dataset_pyarrow`, which operates on existing datasets. Users want one-step merges from in-memory data
frames/tables without staging intermediate datasets.

## Goals / Non-Goals
- Goals:
  - Allow merge strategies (insert/upsert/update/full_merge/deduplicate) directly from `write_pyarrow_dataset`.
  - Provide ergonomic shortcut methods (`insert_dataset`, etc.) mirroring strategy names.
  - Maintain backward compatibility when `strategy` is omitted.
- Non-Goals:
  - Changing merge semantics already defined in `merge_parquet_dataset_pyarrow`.
  - Adding new strategies beyond the existing set.

## Decisions
- Accept optional `strategy` and `key_columns` in `write_pyarrow_dataset`.
- When `strategy` is set, convert incoming data to a temporary in-memory table/dataset and reuse existing merge logic
  rather than re-implementing merge semantics.
- Add filesystem-level convenience methods that simply wrap `write_pyarrow_dataset` with a fixed strategy.
- Keep default behaviour unchanged when `strategy` is None.

## Risks / Trade-offs
- Slight increase in API surface; mitigated by keeping new params optional and by providing concise docs/examples.
- Converting in-memory data to a temporary dataset for merge may have overhead; acceptable for feature parity and UX.

## Migration Plan
1) Extend signature and branch internally on `strategy is None` vs merge path.
2) Add convenience helpers.
3) Add tests per strategy and for helpers.
4) Update docs/how-tos.

## Open Questions
- Should we allow a temporary on-disk staging path for very large in-memory data? (Out of scope for now; note as future enhancement.)
