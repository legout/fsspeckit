# DuckDB Datasets - Merge with MERGE Statement

This specification defines DuckDB merge functionality with native MERGE statement support.

---

## MODIFIED Requirements

### Requirement: `DuckDBDatasetIO.merge` SHALL implement merge strategies using DuckDB's native MERGE statement (DuckDB >= 1.4.0)

**Description**:
The `DuckDBDatasetIO.merge` method SHALL implement INSERT, UPDATE, and UPSERT strategies using DuckDB's native MERGE statement when DuckDB version 1.4.0 or later is available. For earlier DuckDB versions, the method SHALL fallback to UNION ALL + NOT EXISTS approach.

#### Scenario: User calls merge with DuckDB >= 1.4.0 (auto-detect)

**WHEN** `DuckDBDatasetIO.merge` is called with `use_merge=None` (default)
**AND** DuckDB version is >= 1.4.0
**THEN** the method SHALL:
1. Detect DuckDB version via `pragma_version()` query
2. Use native MERGE statement for merge operation
3. Log "Using DuckDB MERGE for merge operation (auto-detected)"
4. Return correct inserted/updated counts from MERGE RETURNING clause

**AND** the method SHALL NOT:
- Use UNION ALL + NOT EXISTS subqueries
- Raise errors about MERGE unavailability
- Require user intervention for MERGE usage

#### Scenario: User calls merge with DuckDB >= 1.4.0 (explicit request)

**WHEN** `DuckDBDatasetIO.merge` is called with `use_merge=True`
**AND** DuckDB version is >= 1.4.0
**THEN** the method SHALL:
1. Detect DuckDB version via `pragma_version()` query
2. Use native MERGE statement for merge operation
3. Log "Using DuckDB MERGE (explicitly requested)"
4. Return correct inserted/updated counts from MERGE RETURNING clause

**AND** the method SHALL NOT:
- Use UNION ALL + NOT EXISTS subqueries
- Fall back to UNION ALL implementation

**AND** if MERGE is unavailable:
- Raise `DatasetMergeError` with clear message: "DuckDB MERGE requested but not available. Required version: >= 1.4.0, Current: <version>"

#### Scenario: User calls merge with DuckDB < 1.4.0 (auto-detect fallback)

**WHEN** `DuckDBDatasetIO.merge` is called with `use_merge=None` (default)
**AND** DuckDB version is < 1.4.0
**THEN** the method SHALL:
1. Detect DuckDB version via `pragma_version()` query
2. Use UNION ALL + NOT EXISTS approach for merge operation
3. Log "Using UNION ALL fallback for merge operation (DuckDB < 1.4.0 or MERGE unavailable)"
4. Return correct inserted/updated counts from existing logic

**AND** the method SHALL NOT:
- Attempt to use MERGE statement
- Raise errors about version incompatibility

#### Scenario: User calls merge with DuckDB < 1.4.0 (explicit fallback)

**WHEN** `DuckDBDatasetIO.merge` is called with `use_merge=False`
**THEN** the method SHALL:
1. Use UNION ALL + NOT EXISTS approach for merge operation
2. Log "Using UNION ALL (explicitly requested)"
3. Return correct inserted/updated counts from existing logic

**AND** the method SHALL NOT:
- Check DuckDB version
- Attempt to use MERGE statement

#### Scenario: MERGE INSERT strategy execution

**WHEN** merge strategy is "insert"
**AND** MERGE implementation is selected
**THEN** the method SHALL generate and execute SQL:
\`\`\`sql
MERGE INTO parquet_scan('{file_path}') AS target
    USING {source_view} AS source
    ON (target."{key}" = source."{key}" ...)
    WHEN NOT MATCHED THEN INSERT BY NAME
\`\`\`

**AND** the method SHALL return:
- `updated_count = 0`
- `inserted_count = number of rows inserted`

**AND** the method SHALL NOT:
- Update any existing rows
- Modify keys not present in source

#### Scenario: MERGE UPDATE strategy execution

**WHEN** merge strategy is "update"
**AND** MERGE implementation is selected
**THEN** the method SHALL generate and execute SQL:
\`\`\`sql
MERGE INTO parquet_scan('{file_path}') AS target
    USING {source_view} AS source
    ON (target."{key}" = source."{key}" ...)
    WHEN MATCHED THEN UPDATE SET *
\`\`\`

**AND** the method SHALL return:
- `updated_count = number of rows updated`
- `inserted_count = 0`

**AND** the method SHALL NOT:
- Insert any new rows
- Affect rows not matching source keys

#### Scenario: MERGE UPSERT strategy execution

**WHEN** merge strategy is "upsert"
**AND** MERGE implementation is selected
**THEN** the method SHALL generate and execute SQL:
\`\`\`sql
MERGE INTO parquet_scan('{file_path}') AS target
    USING {source_view} AS source
    ON (target."{key}" = source."{key}" ...)
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT BY NAME
\`\`\`

**AND** the method SHALL:
1. Use RETURNING clause to count INSERT vs UPDATE actions
2. Create temporary table or aggregation to count merge_action values
3. Return tuple: `(updated_count, inserted_count)`

**AND** the method SHALL NOT:
- Mismatch counts from RETURNING clause
- Use UNION ALL approach for counting

#### Scenario: Multi-column key handling with MERGE

**WHEN** `key_columns` contains multiple columns (e.g., `["id", "category"]`)
**AND** MERGE implementation is selected
**THEN** ON clause SHALL join on all key columns:
\`\`\`sql
ON (target."id" = source."id" AND target."category" = source."category")
\`\`\`

**AND** the method SHALL:
1. Generate correct JOIN conditions for all key columns
2. Properly quote column names to handle special characters
3. Maintain same behavior as existing UNION ALL implementation

**AND** the method SHALL NOT:
- Join on only first key column
- Fail on composite keys

#### Scenario: RETURNING clause audit trail

**WHEN** MERGE implementation is used
**THEN** the method SHALL leverage RETURNING clause:
\`\`\`sql
MERGE ... RETURNING merge_action, *
\`\`\`

**AND** the method SHALL:
1. Count actions via temporary table: `SELECT merge_action, COUNT(*) FROM temp_result GROUP BY merge_action`
2. Extract accurate inserted_count and updated_count
3. Provide audit capability for debugging

**AND** the method SHALL NOT:
- Rely on row count differences (may be inaccurate)
- Skip RETURNING clause when MERGE is available

---

## ADDED Requirements

### Requirement: System SHALL detect DuckDB version for MERGE support

**Description**:
The system SHALL provide runtime version detection to determine if DuckDB MERGE statement is available (DuckDB >= 1.4.0).

#### Scenario: DuckDB version detection succeeds

**WHEN** DuckDB connection is available
**THEN** the system SHALL:
1. Execute `SELECT * FROM pragma_version()` query
2. Parse `library_version` field to extract version string
3. Convert version string to tuple: `(major, minor, patch)`
4. Cache version result per connection for performance

**AND** version parsing SHALL:
- Handle standard format: "1.4.0"
- Handle format with patches: "1.4.1"
- Handle multiple digits: "10.2.3"
- Validate that all components are integers

**AND** the system SHALL NOT:
- Assume a specific version without checking
- Raise errors for well-formed version strings

#### Scenario: DuckDB version detection fails gracefully

**WHEN** `pragma_version()` query fails or returns unexpected format
**THEN** the system SHALL:
1. Log warning: "Could not determine DuckDB version"
2. Return safe default: `(0, 0, 0)`
3. Continue with fallback implementation (UNION ALL)

**AND** the system SHALL NOT:
- Raise exceptions for version detection failure
- Crash or hang due to version query issues

#### Scenario: MERGE support check

**WHEN** version detection completes successfully
**THEN** the system SHALL compare against `(1, 4, 0)`:
1. If `version >= (1, 4, 0)`: MERGE is supported
2. If `version < (1, 4, 0)`: MERGE is NOT supported

**AND** comparison SHALL:
- Use tuple comparison for semantic versioning
- Support all future DuckDB versions correctly

**AND** the system SHALL NOT:
- Use string comparison (incorrect for 10.2 vs 1.4.0)
- Assume MERGE is unavailable without checking

---

### Requirement: System SHALL provide explicit control over MERGE usage

**Description**:
Users SHALL be able to explicitly request or opt-out of MERGE statement usage via `use_merge` parameter.

#### Scenario: Explicit MERGE request with incompatible DuckDB

**WHEN** user sets `use_merge=True`
**AND** DuckDB version is < 1.4.0
**THEN** the system SHALL raise `DatasetMergeError`:
\`\`\`
DuckDB MERGE requested but not available.
Required version: >= 1.4.0, Current: <detected_version>
\`\`\`

**AND** error message SHALL:
- Clearly state requirement
- Show detected version
- Suggest upgrade or use `use_merge=False`

**AND** the system SHALL NOT:
- Silently fall back to UNION ALL
- Allow merge to proceed without MERGE capability

#### Scenario: Explicit fallback request

**WHEN** user sets `use_merge=False`
**THEN** the system SHALL:
1. Use UNION ALL + NOT EXISTS implementation
2. Skip version detection entirely
3. Log: "Using UNION ALL (explicitly requested)"

**AND** the system SHALL NOT:
- Check DuckDB version
- Attempt to use MERGE statement

#### Scenario: Auto-detect mode (default)

**WHEN** user does not specify `use_merge` parameter (None)
**THEN** the system SHALL:
1. Detect DuckDB version
2. Use MERGE if version >= 1.4.0
3. Use UNION ALL if version < 1.4.0
4. Log which implementation was selected

**AND** the system SHALL:
- Make optimal choice automatically
- Provide visibility via logging
- Allow override if needed

**AND** the system SHALL NOT:
- Require user to know about MERGE feature
- Always use one implementation regardless of version

---

### Requirement: MERGE implementation SHALL handle PyArrow Table input

**Description**:
The MERGE implementation SHALL handle PyArrow Table input by registering it as a temporary DuckDB view before executing MERGE statement.

#### Scenario: Register PyArrow Table as temporary view

**WHEN** source data is provided as `pa.Table`
**THEN** the system SHALL:
1. Generate unique view name: `"source_view_<uuid>"` or similar
2. Register table via `conn.register(view_name, source_table)`
3. Use view name in MERGE USING clause
4. Unregister view after merge completes

**AND** registration SHALL:
- Preserve all column names and types
- Handle large tables efficiently (no in-memory copy)
- Use unique names to avoid collisions

**AND** the system SHALL NOT:
- Require PyArrow Table conversion to another format
- Fail on tables with special characters in column names

#### Scenario: Clean up temporary views after merge

**WHEN** MERGE operation completes (success or failure)
**THEN** the system SHALL:
1. Call `_unregister_duckdb_table_safely()` for source view
2. Handle case where view was never registered
3. Log cleanup if needed for debugging

**AND** the system SHALL NOT:
- Leave temporary views in connection
- Rely on garbage collection for cleanup

---

### Requirement: MERGE implementation SHALL use parquet_scan() for target files

**Description**:
The MERGE statement SHALL use `parquet_scan('{file_path}')` to access existing parquet files for merging.

#### Scenario: Build MERGE with parquet_scan() in INTO clause

**WHEN** merging with existing parquet file
**THEN** MERGE statement SHALL use:
\`\`\`sql
MERGE INTO parquet_scan('/path/to/file.parquet') AS target
    USING source_view AS source
    ON (target.key = source.key)
    WHEN MATCHED THEN ...
\`\`\`

**AND** the system SHALL:
1. Validate file path via `PathValidator.validate_path_for_sql()`
2. Escape file path for SQL injection prevention
3. Use proper alias: `AS target` for column references

**AND** the system SHALL NOT:
- Load entire file into memory before MERGE
- Require file to be registered as temporary table

#### Scenario: Validate and escape file paths

**WHEN** building MERGE with `parquet_scan('{path}')`
**THEN** the system SHALL:
1. Call `PathValidator.validate_path_for_sql(path)` before using
2. Call `PathValidator.escape_for_sql(path)` for escaped version
3. Use escaped path in MERGE SQL
4. Raise `ValueError` if path contains dangerous characters

**AND** validation SHALL reject:
- SQL injection attempts (quotes, semicolons, comments)
- Path traversal attempts (.. sequences)
- Suspicious Unicode characters

**AND** validation SHALL allow:
- Alphanumeric characters
- Dots, underscores, hyphens
- Forward slashes (for paths)
- Equals signs (for Hive partitions)
- Colons (for Windows paths)

---

### Requirement: MERGE implementation SHALL preserve atomic file replacement

**Description**:
The MERGE implementation SHALL work seamlessly with existing `IncrementalFileManager` to preserve atomic file-level operations.

#### Scenario: MERGE output to staging file

**WHEN** MERGE operation generates merged output
**THEN** the system SHALL:
1. Write MERGE output to staging file via `COPY (MERGE ...) TO '{staging_file}'`
2. Return from `_merge_using_duckdb_merge()` with updated/inserted counts
3. Allow calling code to use `IncrementalFileManager.atomic_replace_files()`
4. Maintain existing atomic file replacement pattern

**AND** the system SHALL:
- Keep staging directory management in IncrementalFileManager
- Preserve atomic file swap semantics
- Clean up staging files via `IncrementalFileManager.cleanup_staging_files()`

**AND** the system SHALL NOT:
- Replace original files directly without staging
- Modify IncrementalFileManager internals

#### Scenario: Integration with incremental rewrite workflow

**WHEN** merge operation processes multiple affected files
**THEN** the system SHALL:
1. For each file: generate merged output to staging
2. Collect all staging files
3. Call `IncrementalFileManager.atomic_replace_files(staging_files, original_files)`
4. Ensure all file replacements happen atomically
5. Call `IncrementalFileManager.cleanup_staging_files()` on success

**AND** the system SHALL provide:
- Row-level atomicity from MERGE statement
- File-level atomicity from IncrementalFileManager
- Dual-layer protection against data corruption

**AND** the system SHALL NOT:
- Mix MERGE and UNION ALL implementations in same merge operation
- Break existing file replacement guarantees

---

### Requirement: MERGE implementation SHALL support RETURNING clause for audit

**Description**:
When available, the MERGE implementation SHALL use RETURNING clause to provide accurate counts of inserted vs updated rows.

#### Scenario: Count MERGE actions via RETURNING

**WHEN** MERGE operation completes successfully
**THEN** the system SHALL:
1. Execute MERGE with `RETURNING merge_action, *` or similar
2. Create temporary table or CTE to count actions
3. Count rows where `merge_action = 'INSERT'`
4. Count rows where `merge_action = 'UPDATE'`
5. Return tuple: `(updated_count, inserted_count)`

**AND** counting SHALL:
- Be accurate for MERGE operations
- Handle edge cases (all inserts, all updates, mixed)
- Work with both single and multi-column keys

**AND** the system SHALL NOT:
- Estimate counts from source row counts
- Use approximate counting methods

#### Scenario: Temporary table cleanup after counting

**WHEN** RETURNING clause counting completes
**THEN** the system SHALL:
1. Drop temporary table used for counting
2. Unregister any registered views for counting
3. Clean up DuckDB resources
4. Proceed with file write operation

**AND** cleanup SHALL NOT:
- Leave tables or views registered
- Leak DuckDB resources

---

### Requirement: MERGE implementation SHALL provide clear error handling

**Description**:
The MERGE implementation SHALL provide specific error handling for MERGE syntax errors, DuckDB version issues, and invalid usage scenarios.

#### Scenario: MERGE syntax error

**WHEN** MERGE SQL contains syntax error
**THEN** the system SHALL catch:
- `duckdb.ParserException`
- `duckdb.InvalidInputException`

**AND** the system SHALL raise `DatasetMergeError` with:
- Clear error message: "MERGE operation failed: <DuckDB error>"
- Original exception preserved as cause
- Context: merge strategy, key columns, operation name

**AND** the system SHALL log:
- Error type and message
- Merge strategy being executed
- File paths involved
- DuckDB error details

#### Scenario: Version detection error

**WHEN** DuckDB version query fails
**THEN** the system SHALL:
1. Log warning with error details
2. Return safe default version `(0, 0, 0)`
3. Continue with UNION ALL fallback
4. Not raise exception unless critical

**AND** the system SHALL NOT:
- Crash due to version detection failure
- Prevent merge operations from proceeding

#### Scenario: Invalid use_merge parameter

**WHEN** user provides invalid `use_merge` value
**THEN** the system SHALL raise `ValueError`:
\`\`\`
Invalid use_merge value: <value>
Must be True, False, or None for auto-detect
\`\`\`

**AND** error SHALL:
- Clearly state the requirement
- Show valid options
- Help user correct the issue

**AND** the system SHALL NOT:
- Silently ignore invalid values
- Default to None without warning
