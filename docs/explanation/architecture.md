# Architecture Overview

`fsspeckit` extends `fsspec` with enhanced filesystem utilities and storage option configurations for working with various data formats and storage backends. This document provides a technical reference for understanding the system's design and implementation patterns.

## Executive Overview

### Purpose and Value Proposition

`fsspeckit` provides enhanced data processing capabilities through a modular, domain-driven architecture that focuses on filesystem operations, storage configuration, and cross-framework SQL filter translation. The system enables users to work with multiple storage backends and data processing frameworks through unified APIs.

### Core Architectural Principles

1. **Domain-Driven Design**: Clear separation of concerns through domain-specific packages
2. **Backend Neutrality**: Consistent interfaces across different storage providers
3. **Practical Utilities**: Focus on implemented features rather than theoretical capabilities
4. **Backwards Compatibility**: Migration path for existing users
5. **Type Safety**: Strong typing and validation throughout the codebase

### Target Use Cases

- **Multi-Cloud Data Access**: Unified access to AWS S3, Azure Blob, Google Cloud Storage
- **Dataset Operations**: High-performance dataset operations with DuckDB and PyArrow
- **Git Integration**: Filesystem access to GitHub and GitLab repositories
- **SQL Filter Translation**: Cross-framework SQL expression conversion
- **Storage Configuration**: Environment-based storage option management

### Backwards Compatibility

- **Utils Façade**: The `fsspeckit.utils` package serves as a backwards-compatible façade that re-exports from domain packages (`datasets`, `sql`, `common`). 

#### Supported Imports
The following imports are supported for backwards compatibility:
- `setup_logging` - from `fsspeckit.common.logging`
- `run_parallel` - from `fsspeckit.common.misc` 
- `get_partitions_from_path` - from `fsspeckit.common.misc`
- `to_pyarrow_table` - from `fsspeckit.common.types`
- `dict_to_dataframe` - from `fsspeckit.common.types`
- `opt_dtype_pl` - from `fsspeckit.common.polars`
- `opt_dtype_pa` - from `fsspeckit.common.types`
- `cast_schema` - from `fsspeckit.common.types`
- `convert_large_types_to_normal` - from `fsspeckit.common.types`
- `pl` - from `fsspeckit.common.polars`
- `sync_dir` - from `fsspeckit.common.misc`
- `sync_files` - from `fsspeckit.common.misc`
- `DuckDBParquetHandler` - from `fsspeckit.datasets`
- `Progress` - from `fsspeckit.utils.misc` (shim for `rich.progress.Progress`)

#### Migration Path
- **Existing Code**: All existing `fsspeckit.utils` imports continue to work unchanged
- **New Development**: New code should import directly from domain packages for better discoverability
- **Deprecated Paths**: Deeper import paths like `fsspeckit.utils.misc.Progress` are deprecated but functional for at least one major version

#### Deprecation Notices
- `fsspeckit.utils` module is deprecated and exists only for backwards compatibility
- New implementation code should not live in `fsspeckit.utils`
- Use domain-specific imports: `fsspeckit.datasets`, `fsspeckit.sql`, `fsspeckit.common` for new development

## Architectural Decision Records (ADRs)

### ADR-001: Domain Package Architecture

**Decision**: Organize fsspeckit into domain-specific packages (core, storage_options, datasets, sql, common) rather than a monolithic structure.

**Rationale**:
- **Separation of Concerns**: Each domain has distinct responsibilities and user patterns
- **Discoverability**: Users can easily find relevant functionality without searching large modules
- **Testing**: Isolated testing for each domain with clear boundaries
- **Maintenance**: Changes to one domain don't impact others

**Migration Path**: Existing imports through `fsspeckit.utils` continue working while new code uses domain-specific imports.

### ADR-002: Backend-Neutral Planning Layer

**Decision**: Centralize merge and maintenance planning logic in the core package with backend-specific delegates.

**Rationale**:
- **Consistency**: All backends use identical merge semantics and validation
- **Maintainability**: Single source of truth for business logic
- **Performance**: Shared optimization strategies across implementations
- **Testing**: Consistent behavior validation across all backends

**Implementation**: Both DuckDB and PyArrow backends delegate to `core.merge` and `core.maintenance` for planning, validation, and statistics calculation.

### ADR-003: Storage Options Factory Pattern

**Decision**: Implement factory pattern for storage configuration with environment-based setup.

**Rationale**:
- **Portability**: Code works across different cloud providers without changes
- **Configuration**: Environment-based configuration for production deployments
- **Flexibility**: Users can override defaults for specific requirements

**Implementation Pattern**:
```python
# Protocol-agnostic approach
from fsspeckit import filesystem
from fsspeckit.storage_options import storage_options_from_env
options = storage_options_from_env("s3")

# URI-based inference
fs = filesystem("s3://bucket/path")  # Auto-detects protocol
```

## Core Architecture Deep Dive

### Domain Package Breakdown

#### `fsspeckit.core` - Foundation Layer

The core package provides fundamental filesystem APIs and path safety utilities:

**Key Components:**

- **`AbstractFileSystem`** (`core/ext.py`): Extended base class with enhanced functionality
  ```python
  class AbstractFileSystem(fsspec.AbstractFileSystem):
      """Enhanced filesystem with smart path handling and protocol inference."""
  ```

- **`DirFileSystem`**: Path-safe filesystem wrapper
  ```python
  class DirFileSystem(AbstractFileSystem):
      """Filesystem wrapper that restricts operations within specified directories."""
  ```

- **`filesystem()` function**: Enhanced filesystem creation with URI inference
  ```python
  def filesystem(protocol: str, **storage_options) -> AbstractFileSystem:
      """Create filesystem with protocol inference and validation."""
  ```

**Integration Patterns:**
- Protocol detection and inference from URIs
- Smart path normalization and validation
- Directory confinement for security

#### `fsspeckit.storage_options` - Configuration Layer

Manages storage configurations for cloud and Git providers:

**Factory Pattern Implementation:**
```python
def from_dict(protocol: str, storage_options: dict) -> BaseStorageOptions
def from_env(protocol: str) -> BaseStorageOptions
def storage_options_from_uri(uri: str) -> BaseStorageOptions
```

**Provider Implementations:**
- **`AwsStorageOptions`**: AWS S3 configuration with region, credentials, and endpoint settings
- **`GcsStorageOptions`**: Google Cloud Storage setup
- **`AzureStorageOptions`**: Azure Blob Storage configuration
- **`GitHubStorageOptions`**: GitHub repository access with token authentication
- **`GitLabStorageOptions`**: GitLab repository configuration

**Key Features:**
- YAML serialization for persistent configuration
- Environment variable auto-configuration
- Protocol inference from URIs
- Unified interface across all providers

#### `fsspeckit.datasets` - Data Processing Layer

High-performance dataset operations for large-scale data processing:

**DuckDB Implementation:**
```python
class DuckDBDatasetIO:
    """High-performance dataset operations with atomic guarantees."""

    def __init__(self, storage_options=None):
        self.storage_options = storage_options

    def write_dataset(self, data, path, mode="append", **kwargs) -> WriteDatasetResult:
        """Write datasets with metadata tracking."""

    def merge(self, data, path, strategy, key_columns, **kwargs) -> MergeResult:
        """Incremental merge operations with file-level rewrite."""
```

**DuckDB Handler for SQL:**
```python
class DuckDBDatasetHandler:
    """SQL execution and advanced operations."""

    def execute_sql(self, query, **kwargs):
        """Parameterized SQL execution with fsspec integration."""
```

**PyArrow Implementation:**
```python
class PyarrowDatasetIO:
    """PyArrow dataset operations with merge support."""

    def write_dataset(self, data, path, mode="append", **kwargs) -> WriteDatasetResult:
        """Write datasets with metadata tracking."""

    def merge(self, data, path, strategy, key_columns, **kwargs) -> MergeResult:
        """Incremental merge operations with partition pruning."""

# Handler wrapper for maintenance operations
class PyarrowDatasetHandler:
    """Context manager for dataset operations and maintenance."""
    
    def compact_parquet_dataset(self, path, **kwargs):
        """Dataset compaction with atomic operations."""
        
    def optimize_parquet_dataset(self, path, **kwargs):
        """Z-ordering and file size optimization."""
```

**Backend Integration:**
- Shared merge logic from `core.merge`
- Common maintenance operations from `core.maintenance`
- Consistent statistics and validation across backends

#### `fsspeckit.sql` - Query Translation Layer

SQL-to-filter translation for cross-framework compatibility:

**Core Functions:**
```python
def sql2pyarrow_filter(string: str, schema: pa.Schema) -> pc.Expression:
    """Convert SQL WHERE clause to PyArrow filter expression."""

def sql2polars_filter(string: str, schema: pl.Schema) -> pl.Expr:
    """Convert SQL WHERE clause to Polars filter expression."""
```

**Integration Points:**
- Cross-framework SQL expression translation
- Schema-aware filter generation
- Unified SQL parsing using sqlglot
- Table name extraction for validation

#### `fsspeckit.common` - Shared Utilities Layer

Cross-cutting utilities used across all domains:

**Parallel Processing:**
```python
def run_parallel(
    func: Callable,
    data: Sequence[Any],
    max_workers: Optional[int] = None,
    progress: bool = True
) -> List[Any]:
    """Parallel execution with progress tracking and error handling."""
```

**Type Conversion:**
```python
def convert_large_types_to_normal(table: pa.Table) -> pa.Table:
    """Convert large string types to normal string types for compatibility."""

def dict_to_dataframe(data: Dict[str, Any], library: str = "polars"):
    """Convert dictionaries to Polars/Pandas DataFrames."""
```

**File Operations:**
```python
def sync_dir(src_fs, dst_fs, src_path: str, dst_path: str, **kwargs):
    """Synchronize directories between filesystems."""

def extract_partitions(path: str, **kwargs) -> Dict[str, str]:
    """Extract partition information from file paths."""
```

#### `fsspeckit.utils` - Backwards Compatibility Façade

Re-exports selected helpers from domain packages for backwards compatibility:

```python
# Re-exports for backwards compatibility
from ..common.misc import run_parallel
from ..common.datetime import timestamp_from_string
from ..common.types import dict_to_dataframe, to_pyarrow_table
```

**Migration Strategy:**
- Immediate compatibility with existing code
- Gradual migration to domain-specific imports
- Deprecation warnings for discouraged patterns

## Integration Patterns

### Cross-Domain Communication

**Import Patterns:**
```python
# Core → Storage Options
from fsspeckit.storage_options.base import BaseStorageOptions

# Datasets → Core Merge Logic
from fsspeckit.core.merge import (
    MergeStrategy, validate_merge_inputs,
    calculate_merge_stats, check_null_keys
)

# Storage Options → Core Filesystem
from fsspeckit import filesystem
```

**Configuration Flow:**
```python
# Environment-based configuration
from fsspeckit import filesystem
from fsspeckit.storage_options import storage_options_from_env
options = storage_options_from_env("s3")
fs = filesystem("s3", storage_options=options.to_dict())

# URI-based configuration
fs = filesystem("s3://bucket/path")  # Auto-detects and configures
```

### Error Handling Architecture

**Consistent Exception Types:**
- `ValueError` for configuration and validation errors
- `FileNotFoundError` for missing resources
- `PermissionError` for access control issues
- Custom exceptions for domain-specific errors

### Security Architecture

`fsspeckit` implements security best practices through the `fsspeckit.common.security` module, providing utilities to prevent common vulnerabilities in data processing workflows.

**Core Security Helpers:**

1. **Path Validation**: Prevent path traversal attacks and ensure operations stay within allowed directories
   - `validate_path()`: Validates filesystem paths and enforces base directory confinement
   - Integration with `DirFileSystem` for path-safe operations

2. **Credential Scrubbing**: Protect sensitive information in logs and error messages
   - `scrub_credentials()`: Removes credential-like values from strings
   - `scrub_exception()`: Safely formats exceptions without exposing secrets
   - `safe_format_error()`: Creates secure error messages for production logging

3. **Compression Safety**: Prevent codec injection attacks
   - `validate_compression_codec()`: Ensures only safe codecs (snappy, gzip, lz4, zstd, brotli) are used

4. **Column Validation**: Prevent column injection in SQL-like operations
   - `validate_columns()`: Validates requested columns exist in schema

**Production Security Patterns:**

The security helpers are integrated throughout fsspeckit's architecture:

```python
# Filesystem operations use path validation
safe_fs = DirFileSystem(fs=base_fs, path="/data/allowed")

# Dataset operations validate inputs
handler.compact_parquet_dataset(
    path=validate_path(dataset_path, base_dir="/data/allowed"),
    compression=validate_compression_codec(user_codec)
)

# Error messages are scrubbed before logging
logger.error(safe_format_error("read file", path=path, error=e))
```

**Security in Production:**

For production deployments, the architecture emphasizes:
- Credential scrubbing in all error paths
- Path validation for all filesystem operations
- Safe error formatting for observability
- Integration with centralized logging systems
- Multi-tenant isolation through `DirFileSystem`

These security measures are particularly important for:
- Multi-cloud deployments with sensitive credentials
- Multi-tenant environments requiring strict isolation
- Compliance requirements (SOC2, PCI-DSS, etc.)
- Centralized logging and monitoring systems

**Data Flow Patterns**

**Typical Data Processing Pipeline:**
```python
# 1. Configuration Setup
from fsspeckit import filesystem
from fsspeckit.storage_options import storage_options_from_env
from fsspeckit.datasets import DuckDBDatasetIO, DuckDBDatasetHandler
import polars as pl

storage_options = storage_options_from_env("s3")
fs = filesystem("s3", storage_options=storage_options.to_dict())

# 2. Data Processing
io = DuckDBDatasetIO(storage_options=storage_options.to_dict())

# Data ingestion
data = pl.DataFrame({"region": ["US", "EU"], "amount": [100, 200]})
result = io.write_dataset(data, "s3://bucket/raw/", mode="append")
print(f"Wrote {result.total_rows} rows")

# SQL analytics
handler = DuckDBDatasetHandler()
result = handler.execute_sql("""
    SELECT region, SUM(amount) as total
    FROM parquet_scan('s3://bucket/raw/')
    GROUP BY region
""")

# Output
output_result = io.write_dataset(result, "s3://bucket/processed/", mode="overwrite")
```

**Cross-Storage Operations:**
```python
# Sync between cloud providers
from fsspeckit import filesystem
from fsspeckit.common import sync_dir

src_fs = filesystem("s3", storage_options=s3_options)
dst_fs = filesystem("az", storage_options=azure_options)

sync_dir(
    src_fs, dst_fs,
    "s3://bucket/data/",
    "az://container/data/",
    progress=True
)
```

## Performance and Scalability Architecture

### Caching Strategy

**Filesystem Level Caching:**
- Support for fsspec's built-in caching mechanisms
- Optional directory structure preservation
- Configurable cache size and location

### Parallel Processing Architecture

**Worker Pool Management:**
```python
from fsspeckit.common import run_parallel

def process_file(file_path):
    # Process individual file
    return processed_data

# Parallel execution with automatic resource management
results = run_parallel(process_file, file_list, max_workers=8)
```

**Resource Optimization:**
- Automatic worker count detection based on CPU cores
- Memory-aware chunking for large datasets
- Progress tracking and error handling

### Memory Management

**Efficient Data Processing:**
- Streaming operations for large files
- Chunked processing with configurable batch sizes
- Type conversion for PyArrow compatibility

## Extension Points and Customization

### Adding New Storage Providers

**Custom Storage Options:**
```python
from fsspeckit.storage_options.base import BaseStorageOptions

class CustomStorageOptions(BaseStorageOptions):
    """Custom storage provider configuration."""

    provider: str = "custom"
    custom_endpoint: Optional[str] = None

    def to_filesystem(self) -> AbstractFileSystem:
        """Create filesystem instance."""
        return CustomFileSystem(
            endpoint=self.custom_endpoint,
            **self.get_storage_options()
        )
```

### Custom Processing Backends

**Extending Dataset Operations:**
```python
class CustomDatasetHandler:
    """Custom dataset processing backend."""

    def __init__(self, storage_options=None):
        self.storage_options = storage_options

    def write_dataset(self, data, path, **kwargs):
        """Custom dataset writing logic."""
        pass

    def read_dataset(self, path, **kwargs):
        """Custom dataset reading logic."""
        pass
```

## Migration Guide

For details on historical changes between versions, consult the project changelog and release notes.

### Quick Reference

**Step 1: Update Imports**
```python
# Old imports (still work via utils façade)
from fsspec_utils import run_parallel

# New recommended imports
from fsspeckit.common import run_parallel
```

**Step 2: Update Configuration**
```python
# Old configuration style
storage_options = {"key": "value", "secret": "secret"}

# New configuration style
from fsspeckit import AwsStorageOptions
storage_options = AwsStorageOptions(
    access_key="key",
    secret_key="secret"
)
```

**Step 3: Update Filesystem Creation**
```python
# Old method
fs = fsspec.filesystem("s3", **storage_options)

# New method
from fsspeckit import filesystem
fs = filesystem("s3", storage_options=storage_options.to_dict())
```

## Future Features (Not Yet Implemented)

The following features are planned but not yet implemented:

- **Performance Tracking**: Built-in performance monitoring and metrics collection
- **Plugin Registry**: Dynamic plugin discovery and registration system
- **Circuit Breaker Patterns**: Advanced resilience patterns for distributed systems
- **Delta Lake Integration**: Delta Lake write helpers and compatibility
- **Advanced Monitoring**: Comprehensive observability and health checking

## Conclusion

The fsspeckit architecture provides a practical foundation for data processing across multiple storage backends and processing frameworks. The domain-driven design ensures clear separation of concerns while maintaining consistent interfaces and behavior across all components.

The modular architecture enables easy extension and customization while maintaining backwards compatibility for existing users. Built-in performance optimizations and cross-framework compatibility make fsspeckit suitable for data processing workflows.

For specific implementation details and code examples, refer to the individual domain package documentation.
