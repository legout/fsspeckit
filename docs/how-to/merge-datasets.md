# Merge Datasets

This guide covers dataset merge operations using fsspeckit's `merge()` method for both PyArrow and DuckDB backends.

> **Package Structure Note:** fsspeckit uses a package-based structure. DuckDB functionality is under `datasets.duckdb` and PyArrow under `datasets.pyarrow`.

## Understanding merge()

The `merge()` method performs incremental dataset updates by selectively rewriting only the files affected by key matches. This is more efficient than full dataset rewrites for incremental updates.

**Key Concepts:**
- **Incremental file-level rewrite**: Only files containing matching keys are rewritten
- **Partition-aware**: Automatically prunes partitions based on source data
- **Result tracking**: Returns detailed `MergeResult` with operation statistics

## Merge Strategies

### INSERT Strategy
**Use Case:** Append-only scenarios where you never want to modify existing data

- Event logs and audit trails
- Incremental data loads where duplicates should be ignored
- Time-series data where order matters

**Behavior:** Only inserts records with keys that don't exist in the target dataset

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
import pyarrow as pa

io = PyarrowDatasetIO()

# Create initial dataset
initial = pa.table({"id": [1, 2], "value": ["a", "b"]})
io.write_dataset(initial, "events/", mode="overwrite")

# Insert only new records
new_events = pa.table({"id": [2, 3, 4], "value": ["b_dup", "c", "d"]})
result = io.merge(
    data=new_events,
    path="events/",
    strategy="insert",
    key_columns=["id"]
)

print(f"Inserted: {result.inserted}, Ignored: {result.source_count - result.inserted}")
# Output: Inserted: 2, Ignored: 1 (id=2 already exists)
```

### UPSERT Strategy
**Use Case:** Change Data Capture (CDC) and synchronization scenarios

- Customer data synchronization
- Product catalog updates
- Any scenario where you need to insert new records and update existing ones

**Behavior:** Inserts new records and updates existing records based on key columns

```python
# Create initial customer dataset
customers = pa.table({
    "customer_id": [1, 2, 3],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
    "status": ["active", "active", "inactive"]
})
io.write_dataset(customers, "customers/", mode="overwrite")

# UPSERT: Update existing + insert new
updates = pa.table({
    "customer_id": [2, 3, 4],
    "email": ["bob.new@example.com", "charlie@example.com", "diana@example.com"],
    "status": ["active", "active", "active"]
})
result = io.merge(
    data=updates,
    path="customers/",
    strategy="upsert",
    key_columns=["customer_id"]
)

print(f"Inserted: {result.inserted}, Updated: {result.updated}")
# Output: Inserted: 1 (id=4), Updated: 2 (id=2, id=3)
```

### UPDATE Strategy
**Use Case:** Dimension table updates where you only want to modify existing data

- Product price updates
- User profile changes
- Status updates where new records should be rejected

**Behavior:** Only updates existing records, ignores records with new keys

```python
# Create product catalog
products = pa.table({
    "product_id": [101, 102, 103],
    "name": ["Widget A", "Widget B", "Widget C"],
    "price": [19.99, 29.99, 39.99]
})
io.write_dataset(products, "products/", mode="overwrite")

# UPDATE: Only modify existing products
price_updates = pa.table({
    "product_id": [102, 103, 104],  # 104 doesn't exist
    "name": ["Widget B", "Widget C", "Widget D"],
    "price": [24.99, 34.99, 49.99]
})
result = io.merge(
    data=price_updates,
    path="products/",
    strategy="update",
    key_columns=["product_id"]
)

print(f"Updated: {result.updated}, Ignored: {result.source_count - result.updated}")
# Output: Updated: 2 (id=102, id=103), Ignored: 1 (id=104 doesn't exist)
```

## Using MergeResult

The `merge()` method returns a `MergeResult` object with detailed operation statistics:

```python
result = io.merge(
    data=updates,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"]
)

# Row counts
print(f"Source rows: {result.source_count}")
print(f"Target before: {result.target_count_before}")
print(f"Target after: {result.target_count_after}")

# Operation counts
print(f"Inserted: {result.inserted}")
print(f"Updated: {result.updated}")
print(f"Deleted: {result.deleted}")

# File operations
print(f"Rewritten files: {len(result.rewritten_files)}")
print(f"Inserted files: {len(result.inserted_files)}")
print(f"Preserved files: {len(result.preserved_files)}")

# Detailed file metadata
for file_meta in result.files:
    print(f"{file_meta.operation}: {file_meta.path} ({file_meta.row_count} rows)")
```

**MergeResult fields:**
- `strategy`: Merge strategy used ('insert', 'update', 'upsert')
- `source_count`: Number of rows in source data
- `target_count_before`: Target dataset rows before merge
- `target_count_after`: Target dataset rows after merge
- `inserted`: Number of rows inserted
- `updated`: Number of rows updated
- `deleted`: Number of rows deleted
- `files`: List of `MergeFileMetadata` objects
- `rewritten_files`: Paths of files that were rewritten
- `inserted_files`: Paths of new files created
- `preserved_files`: Paths of files left unchanged

## Advanced Features

### Composite Keys

Use multiple columns to uniquely identify records:

```python
# Order items uniquely identified by order_id + line_number
orders = pa.table({
    "order_id": [1001, 1001, 1002],
    "line_number": [1, 2, 1],
    "product": ["Widget A", "Widget B", "Widget C"],
    "quantity": [2, 1, 3]
})
io.write_dataset(orders, "orders/", mode="overwrite")

# Update using composite key
updates = pa.table({
    "order_id": [1001, 1002],
    "line_number": [2, 1],
    "product": ["Widget B", "Widget C"],
    "quantity": [5, 10]  # Updated quantities
})
result = io.merge(
    data=updates,
    path="orders/",
    strategy="upsert",
    key_columns=["order_id", "line_number"]
)

print(f"Updated: {result.updated}")
```

### Partition-Aware Merges

Merge operations automatically prune partitions for efficiency:

```python
# Partitioned dataset by year and month
data_2024_01 = pa.table({
    "year": [2024] * 3,
    "month": [1] * 3,
    "id": [1, 2, 3],
    "value": [100, 200, 300]
})
io.write_dataset(
    data_2024_01,
    "partitioned_data/",
    mode="overwrite",
    partition_by=["year", "month"]
)

# More data in different partition
data_2024_02 = pa.table({
    "year": [2024] * 2,
    "month": [2] * 2,
    "id": [4, 5],
    "value": [400, 500]
})
io.write_dataset(
    data_2024_02,
    "partitioned_data/",
    mode="append",
    partition_by=["year", "month"]
)

# Merge only affects year=2024/month=1 partition
updates = pa.table({
    "year": [2024] * 2,
    "month": [1] * 2,
    "id": [2, 3],
    "value": [999, 888]  # Updated values
})
result = io.merge(
    data=updates,
    path="partitioned_data/",
    strategy="upsert",
    key_columns=["id"],
    partition_columns=["year", "month"]
)

print(f"Preserved files: {len(result.preserved_files)}")  # year=2024/month=2 files preserved
print(f"Rewritten files: {len(result.rewritten_files)}")  # year=2024/month=1 files rewritten
```

### Schema Evolution

Merge operations support schema evolution:

```python
# Initial dataset with basic fields
initial = pa.table({
    "id": [1, 2],
    "name": ["Alice", "Bob"]
})
io.write_dataset(initial, "users/", mode="overwrite")

# Add new column during merge
updates = pa.table({
    "id": [2, 3],
    "name": ["Bob Updated", "Charlie"],
    "email": ["bob@example.com", "charlie@example.com"]  # New column
})

# Provide schema for new files
schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("email", pa.string())
])

result = io.merge(
    data=updates,
    path="users/",
    strategy="upsert",
    key_columns=["id"],
    schema=schema  # New files will use this schema
)
```

## DuckDB Backend

DuckDB provides the same `merge()` API with optimizations for large datasets and native MERGE statement support:

### Native MERGE Statement (DuckDB 1.4.0+)

DuckDB 1.4.0 introduced native MERGE statement support, providing better performance and atomicity:

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO
import polars as pl

io = DuckDBDatasetIO()

# Create initial dataset
initial = pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})
io.write_dataset(initial, "dataset/", mode="overwrite")

# Merge with DuckDB using native MERGE (auto-detected for DuckDB >= 1.4.0)
updates = pl.DataFrame({"id": [2, 3], "value": ["updated", "c"]})
result = io.merge(
    data=updates,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"]
)

print(f"Inserted: {result.inserted}, Updated: {result.updated}")
# Output: Inserted: 1, Updated: 1
```

**Benefits of Native MERGE:**
- 20-40% faster than UNION ALL approach
- Fully ACID-compliant operations
- Accurate tracking of inserted vs updated rows
- More efficient memory usage

### Controlling Merge Implementation

The `use_merge` parameter lets you control which merge implementation is used:

```python
# Auto-detect (default) - uses MERGE if DuckDB >= 1.4.0, falls back to UNION ALL
result = io.merge(
    data=updates,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"],
    use_merge=None  # Auto-detect (default)
)

# Force native MERGE (requires DuckDB >= 1.4.0)
result = io.merge(
    data=updates,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"],
    use_merge=True
)

# Force UNION ALL fallback
result = io.merge(
    data=updates,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"],
    use_merge=False
)
```

**Parameter Guide:**
- `use_merge=None` (default): Auto-detect based on DuckDB version
- `use_merge=True`: Force native MERGE, raises error if DuckDB < 1.4.0
- `use_merge=False`: Always use UNION ALL fallback

### Version Requirements

- **DuckDB >= 1.4.0**: Native MERGE statement supported (recommended)
- **DuckDB < 1.4.0**: Automatic fallback to UNION ALL approach
- **No action required**: Auto-detection ensures compatibility

### Backward Compatibility

The merge API maintains full backward compatibility:
- Existing code works without modification
- Automatic fallback to UNION ALL for older DuckDB versions
- No breaking changes to API or behavior

```python
# This works the same as before, with improved performance if DuckDB >= 1.4.0
result = io.merge(
    data=updates,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"]
)
```

## Backend Selection Guidance

### PyArrow Backend
**Best For:**
- In-memory operations and moderate-sized datasets
- Cloud storage operations (S3, GCS, Azure)
- Schema flexibility and evolution
- Cross-platform compatibility

**Use When:**
- Dataset fits in memory
- Need maximum format compatibility
- Working with cloud storage
- Schema evolution is important

### DuckDB Backend
**Best For:**
- Large datasets that don't fit in memory
- Complex analytics and aggregations
- SQL-heavy workflows
- High-performance query requirements
- Native MERGE statement operations (DuckDB >= 1.4.0)

**Use When:**
- Dataset exceeds available memory
- Need complex SQL operations
- Query performance is critical
- Working with very large datasets
- Want MERGE performance benefits (use DuckDB >= 1.4.0)

## Performance Optimization

### File Size Tuning

```python
# Optimize for PyArrow (moderate file sizes)
result = io.merge(
    data=updates,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"],
    max_rows_per_file=1_000_000,
    row_group_size=250_000,
    compression="snappy"
)

# Optimize for DuckDB (larger file sizes)
result = io.merge(
    data=updates,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"],
    max_rows_per_file=5_000_000,
    row_group_size=500_000,
    compression="snappy"
)
```

### Compression Selection

**snappy**: Fast compression/decompression, moderate compression ratio
- Best for: Frequent reads, query workloads
- Use with: DuckDB, interactive analytics

**zstd**: Slower compression, higher compression ratio
- Best for: Storage optimization, archival
- Use with: PyArrow, long-term storage

**gzip**: Balanced compression and compatibility
- Best for: Data exchange, compatibility
- Use with: Cross-platform scenarios

## When to Use MERGE vs UNION ALL

**Native MERGE (DuckDB 1.4.0+)**
- **Best for:**
  - Production workloads
  - Large datasets (>1M rows)
  - Performance-critical operations
  - Need accurate insert/update counts
  - ACID compliance requirements

- **Advantages:**
  - 20-40% faster than UNION ALL
  - Fully ACID-compliant operations
  - Single-pass execution (better query plan)
  - Accurate tracking of inserted vs updated rows
  - Declarative SQL (easier to understand)

- **When to use:**
  - DuckDB >= 1.4.0 installed (default auto-detected)
  - `use_merge=True` to force MERGE
  - Performance is critical
  - Large datasets (>1M rows)

- **Trade-offs:**
  - Requires DuckDB >= 1.4.0
  - Different execution plan than UNION ALL (may affect query optimization)

**UNION ALL (DuckDB < 1.4.0)**
- **Best for:**
  - Debugging and troubleshooting
  - Understanding execution plans
  - Testing with older DuckDB versions
  - Fallback when MERGE unavailable

- **Advantages:**
  - Compatible with DuckDB < 1.4.0
  - Simple, well-understood execution pattern
  - Easier to debug (transparent queries)

- **When to use:**
  - DuckDB < 1.4.0 installed (auto-detected fallback)
  - `use_merge=False` to force UNION ALL
  - Debugging MERGE behavior issues
  - Need simpler execution plan analysis

- **Trade-offs:**
  - Multiple execution passes (less efficient)
  - No built-in atomicity (requires explicit transactions)
  - Limited auditability (harder to track insert vs update)
  - Expected 20-40% slower than MERGE

**Guidance:**
- **Default behavior:** Use MERGE when available (DuckDB >= 1.4.0), fallback to UNION ALL otherwise
- **Force MERGE:** Use `use_merge=True` for best performance (requires DuckDB >= 1.4.0)
- **Force UNION ALL:** Use `use_merge=False` for debugging or if MERGE issues occur
- **Auto-detect:** Use `use_merge=None` (default) to let version detection decide

**Performance Comparison:**
| Dataset Size | MERGE | UNION ALL | Improvement |
|-------------|--------|-----------|------------|
| Small (<100K rows) | ~1.0x | ~1.2x | 20% |
| Medium (100K-1M rows) | ~1.3x | ~2.0x | 35% |
| Large (>1M rows) | ~1.4x | ~2.2x | 40% |

*Note: Actual performance varies based on dataset characteristics, hardware, and DuckDB version.*

## Error Handling

```python
def safe_merge(io, data, path, strategy, key_columns):
    """Perform merge with comprehensive error handling."""
    try:
        # Validate inputs
        if strategy in ["insert", "update", "upsert"] and not key_columns:
            raise ValueError(f"key_columns required for {strategy} strategy")
        
        # Perform merge
        result = io.merge(
            data=data,
            path=path,
            strategy=strategy,
            key_columns=key_columns
        )
        
        # Log success
        print(f"Merge completed: {result.inserted} inserted, {result.updated} updated")
        return result
        
    except FileNotFoundError:
        print(f"Dataset not found at {path}, creating new dataset")
        return io.write_dataset(data, path, mode="overwrite")
        
    except ValueError as e:
        print(f"Validation error: {e}")
        raise
        
    except Exception as e:
        print(f"Merge failed: {e}")
        raise

# Usage
result = safe_merge(
    io=io,
    data=updates,
    path="dataset/",
    strategy="upsert",
    key_columns=["id"]
)
```

## Real-World Examples

### Customer Data Synchronization

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
import pyarrow as pa

io = PyarrowDatasetIO()

def sync_customers(crm_updates):
    """Synchronize customer data from CRM system."""
    result = io.merge(
        data=crm_updates,
        path="s3://bucket/customers/",
        strategy="upsert",
        key_columns=["customer_id"],
        compression="zstd",
        max_rows_per_file=2_000_000
    )
    
    print(f"Synced customers:")
    print(f"  New: {result.inserted}")
    print(f"  Updated: {result.updated}")
    print(f"  Total: {result.target_count_after}")
    
    return result

# Daily CRM sync
daily_updates = pa.table({
    "customer_id": [1001, 1002, 1003],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
    "status": ["active", "active", "inactive"],
    "updated_at": ["2024-01-15T10:30:00Z"] * 3
})

sync_customers(daily_updates)
```

### Incremental Event Log

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO
import polars as pl

io = DuckDBDatasetIO()

def append_events(new_events):
    """Append events, ignoring duplicates."""
    result = io.merge(
        data=new_events,
        path="s3://bucket/events/",
        strategy="insert",
        key_columns=["event_id"],
        max_rows_per_file=5_000_000,
        compression="snappy"
    )
    
    print(f"Events processed:")
    print(f"  New: {result.inserted}")
    print(f"  Duplicates ignored: {result.source_count - result.inserted}")
    
    return result

# Batch event ingestion
events = pl.DataFrame({
    "event_id": [f"evt_{i}" for i in range(10000)],
    "user_id": [f"user_{i % 1000}" for i in range(10000)],
    "timestamp": ["2024-01-15T10:00:00Z"] * 10000,
    "event_type": ["page_view"] * 10000
})

append_events(events)
```

## Best Practices

1. **Choose the Right Strategy**
   - **INSERT**: Append-only data (event logs, audit trails)
   - **UPSERT**: CDC and synchronization (customer data, product catalogs)
   - **UPDATE**: Dimension updates (prices, statuses)

2. **Key Column Selection**
   - Use stable, unique identifiers
   - Consider query patterns
   - Use composite keys when needed

3. **Performance Optimization**
   - Tune file sizes for your backend
   - Choose appropriate compression
   - Monitor merge statistics
   - Use partition pruning when possible

4. **Error Handling**
   - Validate key columns
   - Handle missing datasets gracefully
   - Log merge results for monitoring
   - Implement retry logic for transient failures

5. **Monitoring**
   - Track merge result statistics
   - Monitor file counts and sizes
   - Log operation duration
   - Alert on anomalies

## Troubleshooting

### Common Issues

**"NULL values are not allowed in key column"**
- Cause: Source data contains NULL values in key columns
- Solution: Filter or fill NULL values before merge

**"Partition column values cannot change for existing keys"**
- Cause: Attempting to change partition column values during merge
- Solution: Ensure partition columns remain constant for existing keys

**High memory usage**
- Cause: Dataset or merge operation too large for available memory
- Solution: Use DuckDB backend or process in smaller batches

**Slow merge performance**
- Cause: Inappropriate file sizes or too many small files
- Solution: Compact dataset or tune max_rows_per_file parameter

For more information, see [Read and Write Datasets](read-and-write-datasets.md).
