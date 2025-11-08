# Design Document: DuckDB Parquet Handler

## Context

fsspeckit needs a high-performance solution for reading and writing parquet datasets with SQL analytics capabilities across multiple storage backends (local, S3, GCS, Azure). DuckDB is an embedded analytical database with excellent parquet support and native fsspec integration via filesystem registration.

The design must support:
- Seamless integration with existing fsspeckit storage options
- Both single files and directory-based parquet datasets
- SQL query execution on parquet data
- Resource management via context managers
- Type safety and comprehensive documentation

## Goals / Non-Goals

### Goals
- Create simple, intuitive API for parquet operations using DuckDB
- Enable SQL analytics on parquet data stored locally or remotely
- Integrate with existing fsspeckit storage options and filesystem abstractions
- Provide performance benefits of DuckDB (columnar processing, parallel execution)
- Support common parquet operations (read, write, query, column projection)

### Non-Goals
- Replace existing PyArrow-based parquet utilities in fsspeckit
- Support non-parquet formats in DuckDB handler
- Implement complex DuckDB features (transactions, indexes, views)
- Provide ORM or query builder abstractions
- Support DuckDB extensions beyond core parquet functionality

## Decisions

### Design Decision 1: Class-Based Handler with Context Manager

**Decision**: Implement as a class with context manager protocol rather than functional API.

**Rationale**:
- DuckDB connections need explicit lifecycle management
- Context manager ensures proper resource cleanup
- Allows maintaining connection state for multiple operations
- Follows patterns used in examples (`with DuckDBParquetHandler() as handler:`)

**Alternatives considered**:
- Functional API with connection parameter: Would require passing connection explicitly, less ergonomic
- Module-level singleton: Would prevent concurrent usage with different configurations

### Design Decision 2: Filesystem Registration Strategy

**Decision**: Register fsspec filesystem in DuckDB connection using `connection.register_filesystem(fs)` during initialization.

**Rationale**:
- DuckDB's native fsspec support provides optimal performance
- Single registration enables all operations (read, write, query)
- Supports all fsspec-compatible filesystems automatically
- Avoids manual path translation or data copying

**Alternatives considered**:
- Manual file download/upload: Would be slower and require temporary storage
- Using DuckDB's native cloud extensions: Would bypass fsspec and lose fsspeckit integration benefits

### Design Decision 3: Flexible Initialization

**Decision**: Support three initialization modes: storage_options, filesystem instance, or default.

```python
# Mode 1: From storage options
handler = DuckDBParquetHandler(storage_options=AwsStorageOptions(...))

# Mode 2: From filesystem instance
fs = filesystem("s3", ...)
handler = DuckDBParquetHandler(filesystem=fs)

# Mode 3: Default local filesystem
handler = DuckDBParquetHandler()
```

**Rationale**:
- Matches existing patterns in fsspeckit
- Provides flexibility for different use cases
- Allows reusing existing filesystem instances
- Defaults to sensible local filesystem behavior

### Design Decision 4: Return PyArrow Tables

**Decision**: All read and query operations return `pyarrow.Table` objects.

**Rationale**:
- Consistent with fsspeckit's existing data utilities
- PyArrow is standard for columnar data interchange
- Easily convertible to pandas, polars, or other formats
- DuckDB's `.arrow()` method provides efficient zero-copy conversion

**Alternatives considered**:
- Return DuckDB relations: Would expose DuckDB-specific API
- Return pandas DataFrames: Less efficient, not zero-copy
- Return polars DataFrames: Additional dependency complexity

### Design Decision 5: SQL via parquet_scan Function

**Decision**: Users write SQL using DuckDB's `parquet_scan()` function for file references.

```python
query = "SELECT * FROM parquet_scan('s3://bucket/data.parquet') WHERE col > 10"
result = handler.execute_sql(query)
```

**Rationale**:
- Leverages DuckDB's native parquet_scan functionality
- Explicit about data sources in queries
- Supports multiple files in single query
- No magic table name mapping required

**Alternatives considered**:
- Automatic table registration: Would require naming conventions and file tracking
- Read-then-query pattern: Less efficient, loses DuckDB's predicate pushdown

### Design Decision 6: Compression Configuration

**Decision**: Expose compression parameter in write_parquet with sensible default.

```python
handler.write_parquet(table, path, compression="snappy")  # or "gzip", "zstd", etc.
```

**Rationale**:
- Common use case for parquet operations
- Direct mapping to DuckDB's COPY command options
- Allows users to balance compression ratio vs speed
- "snappy" default balances performance and compression

## Implementation Details

### Class Structure

```python
class DuckDBParquetHandler:
    """Handler for parquet operations using DuckDB with fsspec integration."""
    
    def __init__(
        self,
        storage_options: BaseStorageOptions | None = None,
        filesystem: AbstractFileSystem | None = None,
    ):
        """Initialize handler with storage options or filesystem."""
        
    def read_parquet(
        self,
        path: str,
        columns: list[str] | None = None,
    ) -> pa.Table:
        """Read parquet file or dataset."""
        
    def write_parquet(
        self,
        table: pa.Table,
        path: str,
        compression: str = "snappy",
    ) -> None:
        """Write PyArrow table to parquet."""
        
    def execute_sql(
        self,
        query: str,
        parameters: list | None = None,
    ) -> pa.Table:
        """Execute SQL query and return results."""
        
    def __enter__(self) -> "DuckDBParquetHandler":
        """Enter context manager."""
        
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager and close connection."""
```

### Filesystem Registration Logic

```python
def _register_filesystem(self) -> None:
    """Register fsspec filesystem in DuckDB connection."""
    if self._filesystem is not None:
        self._connection.register_filesystem(self._filesystem)
```

### Directory Handling for Writes

```python
def write_parquet(self, table: pa.Table, path: str, compression: str = "snappy") -> None:
    """Write parquet with automatic directory creation."""
    from pathlib import Path
    
    # Ensure parent directory exists
    parent = Path(path).parent
    if not self._filesystem.exists(str(parent)):
        self._filesystem.makedirs(str(parent), exist_ok=True)
    
    # Use DuckDB COPY command
    query = f"COPY table TO '{path}' (FORMAT PARQUET, COMPRESSION '{compression}')"
    # ... execute with registered filesystem
```

## Risks / Trade-offs

### Risk 1: DuckDB Version Compatibility

**Risk**: DuckDB API changes between versions could break implementation.

**Mitigation**:
- Pin minimum DuckDB version (1.0.0) in dependencies
- Test against multiple DuckDB versions in CI
- Document required DuckDB version

### Risk 2: Large Dataset Memory Usage

**Risk**: Reading entire parquet datasets into PyArrow tables may consume significant memory.

**Trade-off**: Simplicity vs memory efficiency. DuckDB can stream large datasets, but PyArrow table return requires materialization.

**Mitigation**:
- Document memory considerations
- Encourage column selection to reduce data volume
- Future enhancement: Add streaming/chunked read support

### Risk 3: Remote Storage Authentication

**Risk**: Filesystem registration may fail if storage options have invalid credentials.

**Mitigation**:
- Provide clear error messages from DuckDB/fsspec
- Document authentication requirements per storage backend
- Add examples showing credential configuration

### Risk 4: Path Resolution Differences

**Risk**: DuckDB and fsspec may interpret paths differently (relative vs absolute, URI schemes).

**Mitigation**:
- Normalize paths before passing to DuckDB
- Test with various path formats
- Document expected path formats

## Migration Plan

This is a new capability with no migration required. Implementation steps:

1. **Phase 1**: Implement core class with read/write operations
2. **Phase 2**: Add SQL execution capability
3. **Phase 3**: Add context manager support
4. **Phase 4**: Complete testing and documentation
5. **Phase 5**: Validate with existing examples

Rollout: Module will be added to `fsspeckit.utils` package and exported in `__init__.py` (export statement already present).

## Open Questions

1. **Q**: Should we support DuckDB-native S3 extensions as alternative to fsspec?
   **A**: No, keep fsspec integration for consistency with fsspeckit philosophy. Users can use native DuckDB if needed.

2. **Q**: Should we expose more DuckDB configuration options (memory limit, thread count)?
   **A**: Start simple. Add configuration if users request it. Can add optional `duckdb_config` parameter later.

3. **Q**: Should we support writing to partitioned datasets (hive-style)?
   **A**: Not in initial implementation. Can be added as enhancement if needed.

4. **Q**: Should we cache DuckDB connections for reuse?
   **A**: No caching initially. Each handler instance creates its own connection. Context manager ensures cleanup.
