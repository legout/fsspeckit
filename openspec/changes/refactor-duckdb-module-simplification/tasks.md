## 1. Cleanup and Preparation
- [x] 1.1 Remove `DuckDBParquetHandler` from `__init__.py` and update `__all__`.
- [x] 1.2 Remove legacy API stubs (`write_parquet_dataset`, `merge_parquet_dataset`, etc.) from `DuckDBDatasetIO` in `dataset.py`.
- [x] 1.3 Remove redundant type aliases and imports.

## 2. Refactor Maintenance Operations
- [x] 2.1 Update `compact_parquet_dataset_duckdb` to use DuckDB SQL (`COPY ... TO ...`) for data movement.
- [x] 2.2 Simplify `deduplicate_parquet_dataset` to use DuckDB's `DISTINCT` or `DISTINCT ON` SQL clauses.
  - Note: The method already uses DuckDB SQL with DISTINCT ON and COPY commands.
  - Further optimization (removing intermediate temp tables) was deferred for task 3.x.

## 3. Redesign Merge Logic
- [ ] 3.1 Implement a SQL-based merge core that can handle `UPSERT`, `INSERT`, and `UPDATE` strategies.
  - Note: SQL-based merge core (`_merge_with_sql` method) already exists for full-dataset merges.
- [ ] 3.2 Update `DuckDBDatasetIO.merge` to use the new SQL-based core.
  - Note: This requires significant refactoring of incremental merge logic to use SQL subqueries
  - Requires preserving `IncrementalFileManager` atomic file replacement logic
  - Requires handling PyArrow Table input by registering as temporary DuckDB views
- [ ] 3.3 Ensure the new merge implementation correctly handles PyArrow Table input by registering it as a temporary DuckDB table.
- [ ] 3.4 Ensure atomic file replacement and staging logic is preserved using `IncrementalFileManager`.

## 4. Consolidation and Refinement
- [ ] 4.1 Consolidate private helper methods for writing and reading datasets.
- [ ] 4.2 Standardize error handling and logging across the module.
- [ ] 4.3 Verify that all tests pass (especially those related to DuckDB merge and compaction).
  - Note: Updated test files to use new APIs (`create_duckdb_connection`, `DuckDBDatasetIO`)
  - Several test files have cache/import issues that need investigation
