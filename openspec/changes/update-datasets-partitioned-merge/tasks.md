## 1. Implementation
- [x] 1.1 Propagate `partition_columns` through merge write paths for DuckDB and PyArrow (insert/upsert/initial write).
- [x] 1.2 Enforce partition immutability in PyArrow merges using shared validation helpers.
- [x] 1.3 Align hive partition policy during rewrites (do not inject partition columns into file schemas by default) and update any path parsing logic accordingly.
- [x] 1.4 Fix DuckDB compaction execution path and remove dead/unused merge helper code.
- [x] 1.5 Remove unused PyArrow helper functions that are no longer referenced.

## 2. Tests
- [x] 2.1 Add regression tests for partitioned merge inserts/upserts (DuckDB + PyArrow) to ensure files land under hive partitions.
- [x] 2.2 Add tests for partition immutability enforcement (PyArrow).
- [x] 2.3 Add tests for DuckDB compaction non-dry run behavior.

## 3. Documentation
- [x] 3.1 Update dataset docs/changelog to clarify partitioned merge behavior and hive partition policy.
