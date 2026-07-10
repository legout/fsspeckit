## Context
The DuckDB integration in `fsspeckit` was initially built with a mix of PyArrow and DuckDB. Over time, many operations that could be performed efficiently in DuckDB were implemented using PyArrow's in-memory processing, leading to higher memory usage and unnecessary data movement between the two engines.

## Goals
- Minimize data movement between DuckDB and PyArrow.
- Leverage DuckDB's streaming `COPY` and native SQL joins for maintenance and merge operations.
- Remove legacy APIs that clutter the codebase and confuse users.

## Decisions

### 1. SQL-Based Merge Strategy
Instead of joining PyArrow tables in memory:
1. Register the input PyArrow Table as a temporary view in DuckDB.
2. Use DuckDB SQL to perform the merge logic (e.g., `INSERT`, `UPDATE`, `UPSERT`).
3. For incremental merges, still use `fsspeckit.core` to identify affected files, but use DuckDB to process each file:
   ```sql
   COPY (
     SELECT * FROM source_view s
     UNION ALL
     SELECT * FROM parquet_scan('affected_file.parquet') t
     WHERE NOT EXISTS (SELECT 1 FROM source_view s2 WHERE s2.key = t.key)
   ) TO 'staging_file.parquet' (FORMAT PARQUET)
   ```

### 2. Native Compaction
Replace PyArrow-based compaction with:
```sql
COPY (SELECT * FROM parquet_scan(list_of_files)) TO 'compacted.parquet' (FORMAT PARQUET, COMPRESSION codec)
```
This avoids loading all files into PyArrow memory before writing.

### 3. Native Deduplication
Use `DISTINCT ON` or `GROUP BY` in DuckDB SQL to deduplicate datasets:
```sql
COPY (
  SELECT * FROM parquet_scan(path)
  QUALIFY row_number() OVER (PARTITION BY key ORDER BY sort_col DESC) = 1
) TO 'deduplicated.parquet' (FORMAT PARQUET)
```

## Risks / Trade-offs
- **Risk**: DuckDB SQL behavior might differ slightly from PyArrow (e.g., null handling in joins).
- **Mitigation**: Comprehensive test suite coverage for all merge strategies.
- **Trade-off**: Dropping `DuckDBParquetHandler` is a breaking change, but aligns with the new modular architecture (`DuckDBConnection` + `DuckDBDatasetIO`).

## Migration Plan
- Users of `DuckDBParquetHandler` should switch to:
  ```python
  conn = create_duckdb_connection(filesystem=fs)
  io = DuckDBDatasetIO(conn)
  ```
- Deprecated methods in `DuckDBDatasetIO` (e.g. `write_parquet_dataset`) are removed.
