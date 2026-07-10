# Design Document: DuckDB Dataset Write Support

## Context

The current `DuckDBParquetHandler.write_parquet` method only supports writing to single parquet files. Many data engineering workflows require writing to parquet datasets - directory structures containing multiple parquet files. This enables incremental updates, large dataset handling, and better parallelism.

Key use cases:
- **Data pipelines**: Append new data batches without rewriting entire dataset
- **Large datasets**: Split tables into manageable file sizes
- **Concurrent processing**: Multiple processes writing to same dataset
- **Time-series data**: Add timestamped files for versioning

## Goals / Non-Goals

### Goals
- Enable writing PyArrow tables to parquet dataset directories
- Support overwrite mode (replace dataset) and append mode (add files)
- Generate unique filenames automatically to prevent collisions
- Control file size with max_rows_per_file parameter
- Maintain compatibility with existing read_parquet method
- Support remote storage (S3, GCS, Azure) through fsspec

### Non-Goals
- Implement hive-style partitioning (defer to future enhancement)
- Support transactional writes (ACID guarantees)
- Implement custom partitioning strategies
- Support schema evolution or merging
- Replace existing write_parquet method (additive only)

## Decisions

### Design Decision 1: New Method vs Extending Existing

**Decision**: Add new `write_parquet_dataset` method instead of modifying `write_parquet`.

**Rationale**:
- Clear API: Users explicitly choose single-file vs dataset writes
- No breaking changes to existing code
- Different parameter sets (mode, max_rows_per_file) specific to datasets
- Simpler implementation without complex branching logic

**Alternatives considered**:
- Extend write_parquet with optional parameters: Would make API complex and harder to document
- Auto-detect based on path: Ambiguous behavior, surprising to users

### Design Decision 2: Default Write Mode

**Decision**: Default mode is "append" (adds files without deleting existing).

**Rationale**:
- Safer default: Won't accidentally delete data
- Common use case: incremental data updates
- Matches behavior of most data processing tools (Spark, Dask)
- Users must explicitly choose "overwrite" for destructive operations

**Alternatives considered**:
- Default to "overwrite": More dangerous, easy to lose data
- No default (require explicit mode): More verbose, less ergonomic

### Design Decision 3: Filename Generation Strategy

**Decision**: Use UUID-based filenames as default with format `part-{uuid}.parquet`.

```python
# Default UUID-based
handler.write_parquet_dataset(table, path)
# -> writes to: path/part-a1b2c3d4-e5f6-7890.parquet

# Custom template
handler.write_parquet_dataset(table, path, basename_template="data_{}.parquet")
# -> writes to: path/data_001.parquet, path/data_002.parquet
```

**Rationale**:
- UUID ensures global uniqueness across concurrent writes
- No need to inspect existing files to avoid collisions
- Works reliably in distributed environments
- Simple and fast to generate

**Alternatives considered**:
- Timestamp-based: Risk of collisions with high-frequency writes
- Sequential numbering: Requires reading directory to find next number
- Random suffixes: Less unique than UUID

### Design Decision 4: File Splitting Implementation

**Decision**: Use DuckDB's native capabilities to split writes.

```python
# Pseudocode
if max_rows_per_file is not None:
    num_files = (table.num_rows + max_rows_per_file - 1) // max_rows_per_file
    for i in range(num_files):
        start_idx = i * max_rows_per_file
        end_idx = min((i + 1) * max_rows_per_file, table.num_rows)
        
        # Register slice of table
        slice_table = table.slice(start_idx, end_idx - start_idx)
        conn.register("temp_table", slice_table)
        
        # Write to unique file
        filename = generate_unique_filename()
        conn.execute(f"COPY temp_table TO '{path}/{filename}' ...")
```

**Rationale**:
- Leverages DuckDB's efficient parquet writing
- Memory efficient: processes one slice at a time
- Simple to implement and test
- Consistent with single-file write approach

**Alternatives considered**:
- PyArrow dataset write API: Different dependency, different API style
- Manual chunking: More complex, less efficient

### Design Decision 5: Overwrite Mode Behavior

**Decision**: Overwrite mode deletes only `.parquet` files, preserves others.

**Rationale**:
- Datasets may contain metadata files (_common_metadata, _metadata)
- Users may have additional files (README, schema.json)
- Safer to be conservative about deletion
- Clear and predictable behavior

**Implementation**:
```python
def _clear_dataset(self, path: str) -> None:
    """Clear parquet files from dataset directory."""
    if self._filesystem.exists(path):
        files = self._filesystem.ls(path, detail=False)
        parquet_files = [f for f in files if f.endswith('.parquet')]
        for file in parquet_files:
            self._filesystem.rm(file)
```

**Alternatives considered**:
- Delete entire directory: Too destructive, loses metadata
- Delete all files: May remove important non-data files

### Design Decision 6: Parameter Design

**Decision**: Method signature:
```python
def write_parquet_dataset(
    self,
    table: pa.Table,
    path: str,
    mode: Literal["overwrite", "append"] = "append",
    max_rows_per_file: int | None = None,
    compression: str = "snappy",
    basename_template: str = "part-{}.parquet",
) -> None:
```

**Rationale**:
- mode: Clear choice between overwrite/append semantics
- max_rows_per_file: Optional splitting for large tables
- compression: Consistent with write_parquet
- basename_template: Flexibility for custom naming

**Alternatives considered**:
- Add partition_cols now: Defer to future enhancement (complex)
- Add file_naming_strategy enum: Template is more flexible
- Add row_group_size: DuckDB handles this internally

## Implementation Details

### Core Method Structure

```python
def write_parquet_dataset(
    self,
    table: pa.Table,
    path: str,
    mode: Literal["overwrite", "append"] = "append",
    max_rows_per_file: int | None = None,
    compression: str = "snappy",
    basename_template: str = "part-{}.parquet",
) -> None:
    """Write PyArrow table to parquet dataset directory."""
    
    # Validate inputs
    if mode not in ("overwrite", "append"):
        raise ValueError(f"Invalid mode: {mode}. Must be 'overwrite' or 'append'")
    
    if max_rows_per_file is not None and max_rows_per_file <= 0:
        raise ValueError(f"max_rows_per_file must be > 0, got {max_rows_per_file}")
    
    # Ensure directory exists
    if not self._filesystem.exists(path):
        self._filesystem.makedirs(path, exist_ok=True)
    
    # Handle overwrite mode
    if mode == "overwrite":
        self._clear_dataset(path)
    
    # Split table if needed
    if max_rows_per_file is not None and table.num_rows > max_rows_per_file:
        file_count = (table.num_rows + max_rows_per_file - 1) // max_rows_per_file
        for i in range(file_count):
            start_idx = i * max_rows_per_file
            end_idx = min((i + 1) * max_rows_per_file, table.num_rows)
            slice_table = table.slice(start_idx, end_idx - start_idx)
            filename = self._generate_unique_filename(basename_template)
            self._write_single_file(slice_table, f"{path}/{filename}", compression)
    else:
        # Write single file
        filename = self._generate_unique_filename(basename_template)
        self._write_single_file(table, f"{path}/{filename}", compression)
```

### Unique Filename Generation

```python
def _generate_unique_filename(self, template: str) -> str:
    """Generate unique filename using template."""
    import uuid
    
    if "{}" in template:
        # Template with placeholder
        unique_id = str(uuid.uuid4())[:8]  # Short UUID
        return template.format(unique_id)
    else:
        # No placeholder, add UUID
        base, ext = template.rsplit(".", 1) if "." in template else (template, "parquet")
        unique_id = str(uuid.uuid4())[:8]
        return f"{base}-{unique_id}.{ext}"
```

### Dataset Clearing for Overwrite

```python
def _clear_dataset(self, path: str) -> None:
    """Clear parquet files from dataset directory."""
    if self._filesystem.exists(path):
        try:
            files = self._filesystem.ls(path, detail=False)
            parquet_files = [f for f in files if f.endswith('.parquet')]
            for file in parquet_files:
                self._filesystem.rm(file)
        except Exception as e:
            raise Exception(f"Failed to clear dataset at '{path}': {e}") from e
```

## Risks / Trade-offs

### Risk 1: Concurrent Write Collisions

**Risk**: Multiple processes writing to same dataset simultaneously might have issues.

**Mitigation**:
- UUID-based filenames prevent name collisions
- Each write creates new files atomically
- Document that append mode is safe for concurrent writes
- Overwrite mode should not be used concurrently (document this)

### Risk 2: Dataset Size Growth

**Risk**: Append mode can lead to many small files, degrading read performance.

**Trade-off**: Flexibility vs optimization. Small files are a known parquet anti-pattern.

**Mitigation**:
- Document best practices for dataset maintenance
- Recommend periodic compaction (separate operation)
- Use max_rows_per_file to control file sizes
- Future enhancement: add compaction method

### Risk 3: Overwrite Mode Data Loss

**Risk**: Users might accidentally use overwrite mode and lose data.

**Mitigation**:
- Default to append mode (safer)
- Clear documentation with warnings
- Require explicit mode="overwrite"
- Consider adding confirmation for overwrite in interactive environments (future)

### Risk 4: Memory Usage with Large Tables

**Risk**: Splitting large tables might consume significant memory.

**Trade-off**: Simplicity vs memory efficiency.

**Mitigation**:
- Use PyArrow's slice() which is zero-copy
- Process one slice at a time, not all in memory
- Document memory considerations
- Recommend max_rows_per_file for very large tables

## Migration Plan

This is additive functionality with no migration required.

**Rollout**:
1. Implement `write_parquet_dataset` method
2. Add comprehensive tests
3. Update documentation with examples
4. Add example scripts demonstrating use cases

**Backward Compatibility**:
- Existing `write_parquet` method unchanged
- Existing `read_parquet` method works with new datasets
- No API changes to existing methods

## Open Questions

1. **Q**: Should we support atomic writes (all-or-nothing)?
   **A**: Not in initial implementation. DuckDB writes are atomic per file. Document that partial writes may occur on failure.

2. **Q**: Should we add metadata file generation (_metadata, _common_metadata)?
   **A**: Not initially. Users can generate with PyArrow if needed. Consider for future enhancement.

3. **Q**: Should we support partitioning (hive-style)?
   **A**: Defer to future enhancement. Adds significant complexity. Get basic dataset writes working first.

4. **Q**: Should we support row group configuration?
   **A**: Let DuckDB handle defaults. Can expose if users request it.

5. **Q**: Should we support compaction method to merge small files?
   **A**: Valuable feature but defer to separate enhancement. Keep this change focused.
