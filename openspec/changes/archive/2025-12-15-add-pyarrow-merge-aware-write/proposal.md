# Change: Add merge-aware write support to PyArrow dataset writes

## Why
Users can merge parquet datasets via `merge_parquet_dataset_pyarrow`, but `write_pyarrow_dataset` cannot apply merge
strategies directly when writing DataFrame-like objects. This forces callers to materialize separate datasets and then
invoke merge helpers, making common workflows (insert/upsert/update/deduplicate) more cumbersome and less discoverable.

## What Changes
- Extend `write_pyarrow_dataset` to accept optional `strategy` (`insert`, `upsert`, `update`, `full_merge`, `deduplicate`)
  and `key_columns`. When provided, delegate to the existing merge pipeline instead of plain write.
- Add convenience helpers (monkeypatched on filesystems and exposed via `core.ext`) such as
  `.insert_dataset`, `.upsert_dataset`, `.update_dataset`, `.deduplicate_dataset` that forward to
  `write_pyarrow_dataset` with the corresponding strategy and key columns.
- Preserve current behaviour when `strategy` is omitted; existing signatures remain valid.

## Impact
- Improves UX: callers can perform merge-style writes in one step with PyArrow.
- Public API surface grows by a small set of convenience helpers.
- Behavioural change is opt-in via new parameters; default write semantics are unchanged.

