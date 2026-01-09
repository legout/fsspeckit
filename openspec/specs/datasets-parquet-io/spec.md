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
## Requirements
### Requirement: Incremental Merge Planning Uses Partition and Statistics Pruning
Merge planning SHALL reduce candidate files using partition information and parquet metadata statistics where available.

#### Scenario: Partition pruning reduces candidate files
- **GIVEN** a hive-partitioned dataset under `path` (e.g. `day=2025-01-01/`)
- **AND** the source contains only rows for a single partition value
- **WHEN** the system plans a merge for `key_columns=["id"]`
- **THEN** the system SHALL consider only files within the matching partition(s) as candidates
- **AND** SHALL treat files with unknown partition values conservatively (not pruned)

#### Scenario: Statistics pruning excludes non-overlapping files
- **GIVEN** parquet metadata contains exact min/max statistics for `key_columns`
- **AND** a parquet file’s key range does not overlap the source key range
- **WHEN** the system plans a merge
- **THEN** that parquet file SHALL be excluded from rewrite candidates

### Requirement: Conservative Pruning Preserves Correctness
When file membership cannot be proven via partition info or metadata statistics, the system SHALL behave conservatively.

#### Scenario: Missing or inexact stats treated as unknown membership
- **WHEN** a parquet file has missing or inexact key statistics
- **THEN** the system SHALL treat that file as a candidate (not pruned)

### Requirement: Full Row Replacement Semantics
For merge operations, matching keys SHALL be updated via full-row replacement.

#### Scenario: Update replaces full rows
- **GIVEN** a target dataset row with key `id=1` and columns `{id, a, b}`
- **AND** the source provides a row for `id=1` with columns `{id, a, b}`
- **WHEN** the system merges using `key_columns=["id"]`
- **THEN** the resulting row for `id=1` SHALL equal the source row for all columns

### Requirement: Partition Columns Cannot Change (Initial Implementation)
For partitioned datasets, merge operations SHALL reject updates that would move existing keys between partitions.

#### Scenario: Reject partition movement on update
- **GIVEN** a dataset is partitioned by `["day"]`
- **AND** the target contains key `id=1` in partition `day=2025-01-01`
- **AND** the source provides key `id=1` with `day=2025-01-02`
- **WHEN** the system merges using `key_columns=["id"]` and `partition_by=["day"]`
- **THEN** the method SHALL raise `ValueError` indicating partition columns cannot change for existing keys

### Requirement: `write_dataset` Supports Append and Overwrite
The system SHALL provide `write_dataset(..., mode=...)` for parquet dataset writes with `mode in {"append","overwrite"}`.

#### Scenario: Append writes new parquet files only
- **GIVEN** a dataset directory already contains parquet files
- **WHEN** user calls `write_dataset(table, path, mode="append")`
- **THEN** the system SHALL write additional parquet file(s) under `path`
- **AND** SHALL NOT delete or rewrite existing parquet files

#### Scenario: Overwrite replaces parquet data files
- **GIVEN** a dataset directory contains parquet files and non-parquet files (e.g. `README.txt`)
- **WHEN** user calls `write_dataset(table, path, mode="overwrite")`
- **THEN** the system SHALL remove existing parquet *data* files under `path`
- **AND** MAY preserve non-parquet files
- **AND** SHALL write the new dataset contents to fresh parquet file(s)

### Requirement: Return File Metadata for Newly Written Files
`write_dataset` SHALL return metadata entries for each parquet file it writes.

#### Scenario: Append returns written file metadata
- **WHEN** user calls `write_dataset(table, path, mode="append")`
- **THEN** the result SHALL include metadata entries for each newly written file
- **AND** each entry SHALL include at least file path and row count

### Requirement: `merge` Supports Insert, Update, Upsert (DuckDB)
The DuckDB dataset IO SHALL provide `merge(..., strategy=...)` with `strategy in {"insert","update","upsert"}`.

#### Scenario: Insert appends only new keys
- **GIVEN** a target dataset exists with key `id=1`
- **WHEN** user calls `merge(source, path, strategy="insert", key_columns=["id"])` with keys `id=1` and `id=2`
- **THEN** the system SHALL write parquet file(s) containing only rows for `id=2`
- **AND** SHALL NOT rewrite existing parquet files

#### Scenario: Update preserves unaffected rows
- **GIVEN** a target dataset exists with rows for keys `id=1` and `id=2`
- **AND** the source provides only key `id=2`
- **WHEN** user calls `merge(source, path, strategy="update", key_columns=["id"])`
- **THEN** the resulting dataset SHALL still contain the row for `id=1`
- **AND** SHALL update the row for `id=2`

#### Scenario: Composite keys work for upsert
- **GIVEN** a target dataset exists with keys `(id=1, category="A")` and `(id=2, category="B")`
- **WHEN** user calls `merge(source, path, strategy="upsert", key_columns=["id","category"])`
- **THEN** the system SHALL treat the composite tuple as the key
- **AND** SHALL update matching tuples and insert only non-matching tuples

#### Scenario: Conservative pruning + key-scan confirmation
- **GIVEN** a target dataset exists with many parquet files
- **AND** the source keys touch only a subset of those files
- **WHEN** user calls `merge(source, path, strategy="update", key_columns=[...])`
- **THEN** the system SHALL prune candidate files conservatively (partition parsing + parquet stats when available)
- **AND** SHALL confirm affected files by scanning only `key_columns`
- **AND** SHALL rewrite only confirmed affected files

### Requirement: Merge Returns File Metadata (DuckDB)
`merge` SHALL return metadata for files it rewrites or writes.

#### Scenario: Upsert returns metadata for rewritten and inserted files
- **WHEN** user calls `merge(source, path, strategy="upsert", key_columns=["id"])`
- **THEN** the result SHALL include metadata for each rewritten file
- **AND** SHALL include metadata for each newly written insert file

#### Scenario: Parquet metadata is the default metadata source
- **WHEN** the system needs per-file row counts and size for `MergeFileMetadata`
- **THEN** it SHOULD prefer parquet metadata (`duckdb.parquet_metadata(...)` or `pyarrow.parquet.read_metadata`)
- **AND** MAY use `COPY ... RETURN_STATS` as an optimization but MUST NOT depend on it for correctness

### Requirement: `merge` Supports Insert, Update, Upsert (PyArrow)
The PyArrow dataset IO SHALL provide `merge(..., strategy=...)` with `strategy in {"insert","update","upsert"}`.

#### Scenario: Update rewrites only affected files
- **GIVEN** a target dataset exists with many parquet files
- **WHEN** user calls `merge(source, path, strategy="update", key_columns=["id"])`
- **THEN** the system SHALL rewrite only parquet files that actually contain keys present in `source`
- **AND** SHALL preserve all other parquet files unchanged
- **AND** SHALL NOT write inserts for keys not present in the target

### Requirement: Merge Returns File Metadata (PyArrow)
`merge` SHALL return metadata for files it rewrites or writes.

#### Scenario: Upsert returns metadata for rewritten and inserted files
- **WHEN** user calls `merge(source, path, strategy="upsert", key_columns=["id"])`
- **THEN** the result SHALL include metadata for each rewritten file
- **AND** SHALL include metadata for each newly written insert file

