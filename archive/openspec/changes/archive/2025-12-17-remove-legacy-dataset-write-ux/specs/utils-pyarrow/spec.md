# utils-pyarrow Specification (Delta)

## REMOVED Requirements

### Requirement: Legacy `write_parquet_dataset` merge-aware UX
**Reason**: superseded by explicit `write_dataset(mode=...)` and `merge(strategy=...)` APIs.
**Migration**: callers SHALL switch to `write_dataset` for append/overwrite and `merge` for insert/update/upsert.

