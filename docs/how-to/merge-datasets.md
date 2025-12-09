# Merge Datasets

This guide covers comprehensive dataset merging strategies using fsspeckit's merge-aware write functionality for both PyArrow and DuckDB backends.

> **Package Structure Note:** fsspeckit has been refactored to use package-based structure. DuckDB functionality is under `datasets.duckdb` and PyArrow under `datasets.pyarrow`, while legacy imports still work.

## Merge Strategies Overview

### INSERT Strategy
**Use Case:** Append-only scenarios where you never want to modify existing data
- Event logs and audit trails
- Incremental data loads where duplicates should be ignored
- Time-series data where order matters

**Behavior:** Only inserts records with keys that don't exist in the target dataset

```python
# PyArrow Function-based
fs.insert_dataset(new_events, "events/", key_columns="event_id")

# PyArrow Class-based
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO, PyarrowDatasetHandler

io = PyarrowDatasetIO()
io.insert_dataset(new_events, "events/", key_columns="event_id")

# Or with handler wrapper
with PyarrowDatasetHandler() as handler:
    handler.insert_dataset(new_events, "events/", key_columns="event_id")

# DuckDB  
handler.insert_dataset(new_events, "events/", key_columns=["event_id"])
```

### UPSERT Strategy
**Use Case:** Change Data Capture (CDC) and synchronization scenarios
- Customer data synchronization
- Product catalog updates
- Any scenario where you need to insert new records and update existing ones

**Behavior:** Inserts new records and updates existing records based on key columns

```python
# PyArrow Function-based
fs.upsert_dataset(customer_updates, "customers/", key_columns="customer_id")

# PyArrow Class-based
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO, PyarrowDatasetHandler

io = PyarrowDatasetIO()
io.upsert_dataset(customer_updates, "customers/", key_columns="customer_id")

# Or with handler wrapper
with PyarrowDatasetHandler() as handler:
    handler.upsert_dataset(customer_updates, "customers/", key_columns="customer_id"])

# DuckDB
handler.upsert_dataset(customer_updates, "customers/", key_columns=["customer_id"])
```

### UPDATE Strategy
**Use Case:** Dimension table updates where you only want to modify existing data
- Product price updates
- User profile changes
- Status updates where new records should be rejected

**Behavior:** Only updates existing records, ignores records with new keys

```python
# PyArrow
fs.update_dataset(price_updates, "products/", key_columns="product_id")

# DuckDB
handler.update_dataset(price_updates, "products/", key_columns=["product_id"])
```

### FULL_MERGE Strategy
**Use Case:** Complete dataset replacement scenarios
- Inventory snapshots
- Full synchronization from source of truth
- Dataset rebuild operations

**Behavior:** Replaces entire dataset with source data

```python
# PyArrow
fs.write_pyarrow_dataset(
    data=full_snapshot,
    path="inventory/",
    strategy="full_merge"
)

# DuckDB
handler.write_parquet_dataset(
    data=full_snapshot,
    path="inventory/",
    strategy="full_merge"
)
```

### DEDUPLICATE Strategy
**Use Case:** Data deduplication and cleanup scenarios
- Transaction log cleanup
- Contact list deduplication
- Removing duplicate records from data pipelines

**Behavior:** Removes duplicates based on key columns or exact row matching

```python
# PyArrow
fs.deduplicate_dataset(
    data=transactions,
    path="clean_transactions/",
    key_columns="transaction_id",
    dedup_order_by=["timestamp"]  # Keep latest record
)

# DuckDB
handler.deduplicate_dataset(
    data=transactions,
    path="clean_transactions/",
    key_columns=["transaction_id"],
    dedup_order_by=["timestamp"]
)
```

## Backend Selection Guidance

### PyArrow Backend
**Best For:**
- In-memory operations and smaller datasets
- Cloud storage operations (S3, GCS, Azure)
- Schema flexibility and evolution
- Cross-platform compatibility
- Class-based API consistency with DuckDB

**Performance Characteristics:**
- Excellent for in-memory processing
- Strong compression and format support
- Good for streaming operations
- Lower memory overhead for small-to-medium datasets
- Full API parity with DuckDB handler

**Use When:**
- Dataset fits in memory
- Need maximum format compatibility
- Working with cloud storage
- Schema evolution is important
- Want class-based API with context manager support
- Need advanced maintenance operations (compact, optimize)

### DuckDB Backend
**Best For:**
- Large datasets that don't fit in memory
- Complex analytics and aggregations
- SQL-heavy workflows
- High-performance query requirements

**Performance Characteristics:**
- Excellent for out-of-core processing
- Superior query performance
- Efficient memory usage for large datasets
- Strong SQL optimization

**Use When:**
- Dataset exceeds available memory
- Need complex SQL operations
- Query performance is critical
- Working with very large datasets

## Advanced Merge Features

### Composite Keys
Use multiple columns as merge keys for complex scenarios:

```python
# Composite key for customer orders
composite_key = ["customer_id", "order_date", "product_id"]

# PyArrow
fs.upsert_dataset(
    data=order_updates,
    path="orders/",
    key_columns=composite_key
)

# DuckDB
handler.upsert_dataset(
    data=order_updates,
    path="orders/",
    key_columns=composite_key
)
```

### Custom Deduplication Ordering
Control which record to keep during deduplication:

```python
# Keep latest record per user based on timestamp
fs.deduplicate_dataset(
    data=user_activities,
    path="deduped_activities/",
    key_columns="user_id",
    dedup_order_by=["timestamp", "version"]  # Prioritize latest
)

# Keep highest value per product
fs.deduplicate_dataset(
    data=price_quotes,
    path="latest_prices/",
    key_columns="product_id",
    dedup_order_by=["price", "timestamp"]  # Prioritize highest price
)
```

### Performance Optimization

#### PyArrow Optimization
```python
# Optimize for large datasets
fs.write_pyarrow_dataset(
    data=large_dataset,
    path="optimized_dataset/",
    strategy="upsert",
    key_columns=["id"],
    max_rows_per_file=1000000,  # Larger files for better compression
    compression="zstd",           # Good compression ratio
    row_group_size=250000,         # Optimal for most queries
    verbose=True                   # Monitor progress
)
```

#### DuckDB Optimization
```python
# Optimize for DuckDB's strengths
handler.write_parquet_dataset(
    data=large_dataset,
    path="optimized_dataset/",
    strategy="upsert",
    key_columns=["id"],
    max_rows_per_file=5000000,   # Larger files for DuckDB
    compression="snappy",          # Faster compression for analytics
    verbose=True                   # Monitor progress
)
```

## Error Handling and Validation

### Key Column Validation
```python
def safe_merge_operation(data, path, strategy, key_columns=None):
    """Safe merge operation with proper error handling."""
    try:
        if strategy in ["upsert", "insert", "update"] and not key_columns:
            raise ValueError("Key columns are required for relational merge strategies")
        
        fs.write_pyarrow_dataset(
            data=data,
            path=path,
            strategy=strategy,
            key_columns=key_columns
        )
        print(f"Successfully completed {strategy} operation")
        
    except ValueError as e:
        print(f"Validation error: {e}")
        return False
    except Exception as e:
        print(f"Merge operation failed: {e}")
        return False
    
    return True
```

### Schema Compatibility
```python
# Ensure schema compatibility before merge
def validate_schema_compatibility(source_data, target_path, fs):
    """Validate that source data is compatible with target dataset."""
    # Read target schema
    target_dataset = fs.pyarrow_dataset(target_path)
    target_schema = target_dataset.schema
    
    # Check for missing columns
    source_schema = source_data.schema
    missing_columns = set(target_schema.names) - set(source_schema.names)
    
    if missing_columns:
        print(f"Warning: Source data missing columns: {missing_columns}")
        return False
    
    return True
```

## Real-World Examples

### Customer Data Synchronization (UPSERT)
```python
import pyarrow as pa
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()

# Daily customer updates from CRM system
def sync_customer_data(crm_updates):
    """Synchronize customer data using UPSERT strategy."""
    
    # Validate required columns
    required_columns = ["customer_id", "email", "name", "updated_at"]
    if not all(col in crm_updates.column_names for col in required_columns):
        raise ValueError("Missing required customer data columns")
    
    # Perform UPSERT merge
    fs.upsert_dataset(
        data=crm_updates,
        path="customers/",
        key_columns="customer_id",
        compression="zstd",
        max_rows_per_file=2000000
    )
    
    print(f"Synced {len(crm_updates)} customer records")

# Usage
daily_updates = pa.table({
    "customer_id": [1001, 1002, 1003],
    "email": ["new.email@example.com", "updated@example.com", "new.user@example.com"],
    "name": ["New Customer", "Updated Name", "Another New"],
    "updated_at": ["2024-01-15T10:30:00Z"] * 3
})

sync_customer_data(daily_updates)
```

### Transaction Log Deduplication
```python
import polars as pl
from fsspeckit.datasets import DuckDBParquetHandler

# Initialize DuckDB handler for large transaction logs
handler = DuckDBParquetHandler()

def deduplicate_transaction_logs(transactions):
    """Deduplicate transaction logs keeping latest records."""
    
    # Sort by timestamp to ensure proper ordering
    transactions = transactions.sort("timestamp")
    
    # Deduplicate using transaction_id, keep latest by timestamp
    handler.deduplicate_dataset(
        data=transactions,
        path="clean_transactions/",
        key_columns=["transaction_id"],
        dedup_order_by=["timestamp"],
        max_rows_per_file=5000000,
        compression="snappy"
    )
    
    print(f"Deduplicated {len(transactions)} transaction records")

# Usage with large dataset
large_transactions = pl.DataFrame({
    "transaction_id": [f"TXN_{i:06d}" for i in range(1000000)],
    "amount": [100.0 + (i % 1000) for i in range(1000000)],
    "timestamp": [f"2024-01-{(i % 30) + 1:02d}T{(i % 24):02d}:00Z" for i in range(1000000)]
})

deduplicate_transaction_logs(large_transactions)
```

## Best Practices

### 1. Choose the Right Strategy
- **INSERT**: For append-only data where existing records should never change
- **UPSERT**: For synchronization scenarios where new records are added and existing ones updated
- **UPDATE**: When you only want to modify existing records and reject new ones
- **FULL_MERGE**: For complete dataset replacement operations
- **DEDUPLICATE**: When you need to remove duplicate records

### 2. Key Column Selection
- Use stable, unique identifiers as key columns
- For composite keys, ensure the combination is unique
- Consider query patterns when choosing key columns
- Index key columns in target datasets for better performance

### 3. Performance Considerations
- Use appropriate file sizes for your backend (larger for DuckDB, moderate for PyArrow)
- Choose compression based on use case (snappy for speed, zstd for space)
- Monitor memory usage, especially with PyArrow for large datasets
- Use verbose mode for production monitoring

### 4. Error Handling
- Always validate key columns for relational strategies
- Handle schema compatibility issues gracefully
- Implement proper logging and monitoring
- Use try-catch blocks for production operations

### 5. Testing Strategy
- Test merge operations on small samples first
- Validate results before applying to production data
- Use dry-run modes when available
- Monitor performance characteristics in your environment

## Troubleshooting

### Common Issues

**"Key columns are required" Error**
- Cause: Using relational strategy without specifying key_columns
- Solution: Add appropriate key_columns parameter

**"Dataset not found" Error**
- Cause: Target dataset doesn't exist for update/upsert operations
- Solution: Create initial dataset or use insert strategy

**Memory Issues with PyArrow**
- Cause: Dataset too large for available memory
- Solution: Use DuckDB backend or process in smaller batches

**Performance Issues**
- Cause: Inappropriate file sizes or compression
- Solution: Optimize file sizes and choose appropriate compression

### Debug Mode
```python
# Enable verbose logging for debugging
fs.write_pyarrow_dataset(
    data=test_data,
    path="debug_dataset/",
    strategy="upsert",
    key_columns=["id"],
    verbose=True  # Shows detailed progress information
)
```

This comprehensive guide should help you effectively use fsspeckit's merge-aware write functionality across both PyArrow and DuckDB backends for various real-world scenarios.