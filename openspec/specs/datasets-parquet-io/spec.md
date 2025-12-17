# Spec: datasets-parquet-io

## Purpose
Provides backend-neutral core functionality for incremental merge operations on Parquet datasets, including file pruning, metadata analysis, and merge invariants.

## Scope
- Hive partition parsing from dataset paths
- Parquet metadata extraction for pruning (min/max/null_count with conservative fallbacks)
- Candidate file pruning using partition info and column statistics
- Affected file confirmation via key intersection scans
- Staging + atomic replace mechanics for safe per-file rewrites
- Merge invariants: full-row replacement, partition column immutability for existing keys

## Location
- Module: `src/fsspeckit/core/incremental.py`
- Supports both PyArrow and DuckDB merge implementations

## API

### Merge Operations

```python
@dataclass
class MergeFileMetadata:
    """Metadata for files affected by merge operation."""
    path: str
    row_count: int
    operation: Literal["rewritten", "inserted", "preserved"]
    size_bytes: int | None = None

@dataclass
class MergeResult:
    """Result of a merge operation."""
    strategy: str  # "insert", "update", or "upsert"
    source_count: int
    target_count_before: int
    target_count_after: int
    inserted: int
    updated: int
    deleted: int
    files: list[MergeFileMetadata]  # Files affected by the merge
    rewritten_files: list[str]  # Paths of rewritten files
    inserted_files: list[str]  # Paths of newly inserted files
    preserved_files: list[str]  # Paths of unchanged files

def merge(
    data: pa.Table,
    path: str,
    strategy: Literal["insert", "update", "upsert"],
    key_columns: Sequence[str],
    partition_columns: Sequence[str] | None = None,
    filesystem: Any = None,
    compression: str = "snappy",
    max_rows_per_file: int = 5_000_000,
    row_group_size: int = 500_000,
) -> MergeResult:
    """
    Merge data into an existing parquet dataset using incremental rewrite.
    
    Strategies:
    - insert: Append only new keys (not in target) as new files
    - update: Rewrite only files containing matching keys
    - upsert: Rewrite affected files + append new keys as new files
    
    Invariants enforced:
    - Full-row replacement (not column-level updates)
    - Partition columns cannot change for existing keys
    
    Returns:
        MergeResult with per-file metadata for rewritten and inserted files
    """
```

### File Listing and Partition Parsing

```python
def list_dataset_files(
    dataset_path: str,
    filesystem: Any = None,
) -> list[str]:
    """List all parquet files in a dataset directory."""

def parse_hive_partition_path(
    file_path: str,
    partition_columns: Sequence[str] | None = None,
) -> dict[str, Any]:
    """
    Extract partition key-value pairs from a Hive-partitioned file path.
    
    Examples:
        /data/year=2024/month=12/file.parquet -> {"year": "2024", "month": "12"}
    """
```

### Metadata Extraction

```python
@dataclass
class ParquetFileMetadata:
    """Metadata for a single parquet file."""
    path: str
    row_group_count: int
    total_rows: int
    column_stats: dict[str, dict[str, Any]]  # column -> {min, max, null_count}
    partition_values: dict[str, Any] | None = None

class ParquetMetadataAnalyzer:
    """Extract and analyze parquet file metadata for incremental rewrite planning."""
    
    def analyze_dataset_files(
        self,
        dataset_path: str,
        filesystem: Any = None,
    ) -> list[ParquetFileMetadata]:
        """Analyze all parquet files in a dataset directory."""
```

### Candidate Pruning

```python
class PartitionPruner:
    """Identify candidate files based on partition values."""
    
    def identify_candidate_files(
        self,
        file_metadata: list[ParquetFileMetadata],
        key_columns: Sequence[str],
        source_keys: Sequence[Any],
        partition_schema: pa.Schema | None = None,
    ) -> list[str]:
        """Prune files by partition values when applicable."""

class ConservativeMembershipChecker:
    """Conservative pruning using column statistics."""
    
    def file_might_contain_keys(
        self,
        file_metadata: ParquetFileMetadata,
        key_columns: Sequence[str],
        source_keys: Sequence[Any],
    ) -> bool:
        """Conservative check if file might contain any source keys."""
```

### Affected File Confirmation

```python
def confirm_affected_files(
    candidate_files: list[str],
    key_columns: Sequence[str],
    source_keys: Sequence[Any],
    filesystem: Any = None,
) -> list[str]:
    """
    Scan candidate files to confirm which actually contain source keys.
    
    Only reads key_columns from parquet files for efficient confirmation.
    """
```

### Staging and Replace

```python
class IncrementalFileManager:
    """Manage file operations for incremental rewrite."""
    
    def create_staging_directory(self, base_path: str) -> str:
        """Create a staging directory for incremental operations."""
    
    def atomic_replace_files(
        self,
        source_files: list[str],
        target_files: list[str],
        filesystem: Any = None,
    ) -> None:
        """Atomically replace target files with source files."""
    
    def cleanup_staging_files(self) -> None:
        """Clean up temporary staging files."""
```

### Merge Invariants and Validation

```python
def validate_no_null_keys(
    source_table: pa.Table,
    key_columns: Sequence[str],
) -> None:
    """
    Reject merges where source has null keys.
    
    Raises:
        ValueError: If any key column contains NULL values.
    """

def validate_partition_column_immutability(
    source_table: pa.Table,
    target_table: pa.Table,
    key_columns: Sequence[str],
    partition_columns: Sequence[str],
) -> None:
    """
    Reject merges that would change partition columns for existing keys.
    
    For existing keys (keys present in both source and target), ensures
    partition column values remain unchanged.
    
    Raises:
        ValueError: If any existing key has changed partition column values.
    """
```

### Incremental Rewrite Planning

```python
@dataclass
class IncrementalRewritePlan:
    """Plan for executing an incremental rewrite operation."""
    affected_files: list[str]      # Files needing rewrite
    unaffected_files: list[str]    # Files to preserve
    new_files: list[str]           # New files for inserts
    affected_rows: int             # Total rows in affected files

def plan_incremental_rewrite(
    dataset_path: str,
    source_keys: Sequence[Any],
    key_columns: Sequence[str],
    filesystem: Any = None,
    partition_schema: pa.Schema | None = None,
) -> IncrementalRewritePlan:
    """Plan an incremental rewrite operation based on metadata analysis."""
```

## Behavior

### Conservative Pruning
- When metadata is unavailable or insufficient, files are marked as affected (safe default)
- Partition pruning applies when partition schema is known
- Statistics pruning uses min/max values when available
- Final confirmation scans only key columns for efficiency

### Merge Invariants
1. **Full-row replacement**: Matching keys result in complete row replacement (not column-level updates)
2. **Partition immutability**: Existing keys MUST NOT move between partitions (validation rejects such attempts)
3. **NULL-free keys**: Key columns must not contain NULL values in source data

### File Operations
- Staging directory uses `.staging_<uuid>` naming convention
- Files are written to staging first, then atomically moved to target
- Cleanup happens automatically on success or best-effort on failure

## Error Handling
- Missing metadata: Conservative fallback (treat files as affected)
- NULL keys: Immediate rejection with clear error message
- Partition moves: Immediate rejection with clear error message
- Filesystem errors: Propagate to caller with context

## Testing Requirements
- Test Hive partition parsing with various path formats
- Test metadata extraction with and without statistics
- Test partition pruning with single and multi-column partitions
- Test conservative membership checking edge cases
- Test NULL key detection
- Test partition column immutability validation
- Test staging and atomic replace operations
- Test cleanup on success and failure paths

## Dependencies
- `pyarrow`: For Parquet metadata reading and dataset operations
- `fsspec`: For filesystem abstraction (optional)

## Related Specs
- `core-maintenance`: Uses incremental rewrite for maintenance operations
- `datasets-duckdb`: DuckDB merge implementation uses this core
- `utils-pyarrow`: PyArrow merge implementation uses this core
