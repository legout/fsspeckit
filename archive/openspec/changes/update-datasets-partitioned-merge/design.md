## Context
Partitioned merges currently write inserted rows into dataset roots because partition columns are not forwarded during dataset writes. In addition, DuckDB merge rewrites sometimes inject partition columns into file schemas, which can create mixed-schema datasets (some files contain partition columns, others rely on hive path inference). This change standardizes partition handling across backends.

## Goals / Non-Goals
- Goals:
  - Ensure partitioned merges write inserts into correct hive partition directories.
  - Maintain consistent file schemas across rewritten and preserved files.
  - Align PyArrow and DuckDB behavior for partitioned datasets.
  - Remove dead/unused helper code and fix compaction execution.
- Non-Goals:
  - Redesign merge strategies or introduce new storage formats (e.g. Iceberg/Delta).
  - Add new public APIs beyond current merge/write parameters.

## Decisions
- **Partition column policy (default):** Hive-style partitioning, where partition values are inferred from directory paths and partition columns are not injected into Parquet file schemas during merge rewrites.
  - Rationale: aligns with current DuckDB `COPY ... PARTITION_BY` defaults and PyArrow `write_dataset` hive partitioning.
  - This prevents mixed-schema datasets and keeps partitioning consistent with existing layouts.
- **Merge insert writes:** All merge insert paths must forward `partition_columns` into `write_dataset` to preserve layout.

## Alternatives considered
- **Write partition columns into Parquet files** (DuckDB `WRITE_PARTITION_COLUMNS true` + PyArrow pre-adding columns): rejected for now due to risk of mixed schemas in existing datasets and compatibility with existing hive-style outputs.
- **Per-backend policies**: rejected to avoid divergent behaviors and surprising cross-backend differences.

## Risks / Trade-offs
- Datasets that already rely on partition columns being injected into rewritten files may see changed schema behavior. Mitigation: document policy clearly and align with defaults across backends.
- If users want partition columns stored in Parquet files, a future explicit option can be added without changing defaults.

## Migration Plan
- No data migration required. Behavior changes apply to new merge operations only.
- Add regression tests to ensure partitioned inserts land in correct directories and schemas remain consistent.

## Open Questions
- Should we expose an explicit `write_partition_columns` option in dataset APIs for advanced users?
