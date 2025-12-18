# Merge Operations - Usage Examples

This document provides comprehensive examples of the fully implemented merge operations in fsspeckit. All merge strategies (insert, update, upsert) are now working with proper implementations.

## Overview

The merge functionality has been significantly improved with:

- ✅ **DuckDB backend**: Fully implemented `_merge_upsert()`, `_merge_update()`, and `_extract_inserted_rows()` methods
- ✅ **PyArrow backend**: Optimized with vectorized operations and performance improvements
- ✅ **Performance optimizations**: Eliminated Python loops and `.to_pylist()` bottlenecks
- ✅ **Clean implementation**: Removed duplicate code and improved maintainability

## DuckDB Merge Operations

### UPSERT Operations

The UPSERT strategy combines INSERT and UPDATE operations - it adds new records and updates existing ones based on key columns.

```python
from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO
import pyarrow.parquet as pq

# Initialize DuckDB handler
duckdb_handler = DuckDBDatasetIO()

# Load existing and new data
existing_data = pq.read_table("customers.parquet")
new_data = pq.read_table("customer_updates.parquet")

# Perform UPSERT merge
upsert_result = duckdb_handler._merge_upsert(
    existing_data=existing_data,
    source_data=new_data,
    key_columns=["customer_id"]
)

# Result contains both updated existing records and new records
print(f"Total records after UPSERT: {len(upsert_result)}")
```

**Use Cases:**
- Customer data synchronization
- CDC (Change Data Capture) operations
- Incremental data updates
- Data warehouse ETL processes

### UPDATE Operations

The UPDATE strategy only updates existing records - new records are ignored.

```python
# Perform UPDATE merge (only existing products will be updated)
update_result = duckdb_handler._merge_update(
    existing_data=existing_products,
    source_data=product_updates,
    key_columns=["product_id"]
)

# Result contains only updated existing records, new products ignored
print(f"Total products after UPDATE: {len(update_result)}")
```

**Use Cases:**
- Price updates for existing products
- Status changes for existing records
- Dimension table maintenance
- Reference data updates

### Extracting Inserted Rows

Extract only the newly inserted rows from a merge operation:

```python
# Identify newly inserted records
inserted_rows = duckdb_handler._extract_inserted_rows(
    existing_data=existing_data,
    source_data=new_data,
    key_columns=["customer_id"]
)

# Result contains only new records that weren't in existing data
print(f"New records inserted: {len(inserted_rows)}")
```

**Use Cases:**
- Auditing new records
- Trigger-based processing
- Change tracking
- Notification systems

## PyArrow Merge Operations

The PyArrow backend now includes optimized merge operations with vectorized performance improvements:

```python
from fsspeckit.datasets.pyarrow.io import PyarrowDatasetHandler

# Initialize PyArrow handler
pyarrow_handler = PyarrowDatasetHandler()

# Perform merge with optimized operations
merge_result = pyarrow_handler.merge(
    data=updates_data,
    path="dataset_path",
    strategy="upsert",
    key_columns=["customer_id"]
)

print(f"Inserted: {merge_result.inserted}")
print(f"Updated: {merge_result.updated}")
print(f"Total files affected: {len(merge_result.files)}")
```

### Performance Optimizations

The PyArrow implementation includes several key optimizations:

1. **Vectorized Operations**: Uses `pc.is_in()` instead of Python loops
2. **Optimized Key Matching**: Efficient set operations with PyArrow arrays
3. **Reduced Conversions**: Minimized `.to_pylist()` calls
4. **Memory Efficiency**: Better memory usage patterns

## Strategy Selection Guide

### INSERT Strategy
- **Purpose**: Add new records only
- **Use Cases**: Event logs, audit trails, incremental loads
- **Behavior**: Ignores existing records, adds only new ones

```python
pyarrow_handler.merge(
    data=new_events,
    path="events/",
    strategy="insert",
    key_columns=["event_id"]
)
```

### UPSERT Strategy  
- **Purpose**: Add new records and update existing ones
- **Use Cases**: Customer sync, CDC, data synchronization
- **Behavior**: Updates existing + inserts new records

```python
pyarrow_handler.merge(
    data=customer_updates,
    path="customers/",
    strategy="upsert", 
    key_columns=["customer_id"]
)
```

### UPDATE Strategy
- **Purpose**: Update existing records only
- **Use Cases**: Price updates, status changes, dimension tables
- **Behavior**: Updates existing records, ignores new ones

```python
pyarrow_handler.merge(
    data=price_updates,
    path="products/",
    strategy="update",
    key_columns=["product_id"]
)
```

## Key Column Best Practices

### Good Key Column Examples
- `customer_id` - Unique customer identifier
- `transaction_id` - Unique transaction number  
- `email + timestamp` - Composite key for user events
- `order_id + line_item_id` - Composite key for order details

### Key Column Requirements
- Must uniquely identify records
- Should be stable (not change over time)
- Cannot contain NULL values
- For composite keys, combination must be unique

### Composite Key Example
```python
# Use multiple columns as composite key
pyarrow_handler.merge(
    data=order_items,
    path="order_details/",
    strategy="upsert",
    key_columns=["order_id", "line_item_id"]
)
```

## Performance Considerations

### DuckDB Backend
- **Best for**: Complex queries, large datasets, memory efficiency
- **Advantages**: SQL-based operations, good for analytical workloads
- **Use when**: Need complex merge logic or working with very large datasets

### PyArrow Backend  
- **Best for**: Simple merges, file-based operations, streaming scenarios
- **Advantages**: Vectorized operations, optimized I/O, incremental processing
- **Use when**: Performance is critical and merge logic is straightforward

### Performance Improvements Made
- **Eliminated Python loops**: Replaced with vectorized PyArrow operations
- **Optimized file matching**: Uses `pc.is_in()` instead of set intersections
- **Reduced memory usage**: Minimized data conversions between formats
- **Better scalability**: Improved performance for large datasets

## Real-World Examples

### Customer Data Synchronization
```python
# Sync customer data from external system
customer_updates = load_customer_updates()
result = pyarrow_handler.merge(
    data=customer_updates,
    path="customer_database/",
    strategy="upsert",
    key_columns=["customer_id"]
)

print(f"Synced {result.inserted} new customers")
print(f"Updated {result.updated} existing customers")
```

### Product Price Updates
```python
# Update product prices from supplier
price_updates = load_supplier_prices()
result = pyarrow_handler.merge(
    data=price_updates,
    path="product_catalog/",
    strategy="update",
    key_columns=["product_id"]
)

print(f"Updated {result.updated} product prices")
```

### Event Log Deduplication
```python
# Add new events, remove duplicates
new_events = load_new_events()
result = pyarrow_handler.merge(
    data=new_events,
    path="event_logs/",
    strategy="insert",
    key_columns=["event_id"]
)

print(f"Added {result.inserted} new events")
```

## Error Handling

### Common Issues and Solutions

1. **Key Column Not Found**
   ```
   ValueError: Key column 'customer_id' not found in data
   ```
   **Solution**: Verify key column names match exactly

2. **NULL Values in Key Columns**
   ```
   ValueError: NULL values found in key columns
   ```
   **Solution**: Clean data to remove NULLs from key columns

3. **Non-existent Target for UPDATE**
   ```
   ValueError: UPDATE strategy requires an existing target dataset
   ```
   **Solution**: Use INSERT or UPSERT for new datasets

### Validation Example
```python
def validate_merge_result(original_data, updated_data, key_columns):
    # Check for duplicate keys
    key_values = updated_data.column(key_columns[0]).to_pylist()
    unique_keys = set(key_values)
    
    if len(key_values) != len(unique_keys):
        raise ValueError(f"Duplicate keys found in {key_columns}")
    
    return True
```

## Testing and Verification

### Simple Test Cases
```python
# Test data
existing = pa.Table.from_pydict({
    "id": [1, 2, 3],
    "name": ["A", "B", "C"]
})

updates = pa.Table.from_pydict({
    "id": [2, 4],  # Update 2, add 4
    "name": ["B_updated", "D"]
})

# UPSERT should result in: [1, 2_updated, 3, 4]
result = duckdb_handler._merge_upsert(existing, updates, ["id"])
assert len(result) == 4
```

### Performance Testing
```python
import time

# Test with large dataset
large_data = create_large_dataset(1000000)
start_time = time.time()

result = pyarrow_handler.merge(
    data=large_data,
    path="large_dataset/",
    strategy="upsert",
    key_columns=["id"]
)

elapsed = time.time() - start_time
print(f"Processed {len(large_data)} records in {elapsed:.2f} seconds")
```

## Migration Guide

If you have existing code using the old merge functionality:

### Before (Not Implemented)
```python
# These would have been NotImplementedError
duckdb_handler._merge_upsert(data, existing, keys)  # ❌ Failed
duckdb_handler._merge_update(data, existing, keys)  # ❌ Failed
```

### After (Fully Implemented)
```python
# These now work properly
result = duckdb_handler._merge_upsert(existing, data, keys)  # ✅ Works
result = duckdb_handler._merge_update(existing, data, keys)  # ✅ Works
inserted = duckdb_handler._extract_inserted_rows(existing, data, keys)  # ✅ Works
```

## Summary

The merge operations are now fully functional with:

- **Complete DuckDB implementation** with proper UPSERT/UPDATE semantics
- **Optimized PyArrow operations** with vectorized performance improvements  
- **Comprehensive strategy support** (INSERT, UPDATE, UPSERT)
- **Better performance** for large datasets
- **Clean, maintainable code** without duplicates

All merge strategies work correctly across both backends and are ready for production use.