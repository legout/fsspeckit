# Migration Guide: Dataset Write Modes Default to Append

## Overview

Starting with this version, the `write_parquet_dataset()` method now defaults to **append mode** instead of overwrite mode for improved safety. This change prevents accidental data loss when writing datasets without explicitly specifying the mode.

## What Changed

### Before (Previous Behavior)
```python
# Previously: Defaulted to overwrite mode (dangerous)
io.write_parquet_dataset(data, "/path/to/dataset/")
# Would delete existing parquet files and replace with new data
```

### After (New Behavior)
```python
# Now: Defaults to append mode (safer)
io.write_parquet_dataset(data, "/path/to/dataset/")
# Will add new files without deleting existing ones
```

## Breaking Changes

### Default Behavior Change
- **Old default**: `mode="overwrite"` (dangerous - deleted existing data)
- **New default**: `mode="append"` (safer - preserves existing data)

### Mode/Strategy Compatibility
The new implementation includes validation to prevent incompatible mode/strategy combinations:

- **`mode="append"` + rewrite strategies**: Now explicitly rejected
  - Rewrite strategies: `upsert`, `update`, `full_merge`, `deduplicate`
  - Error: "Invalid mode: append mode is incompatible with rewrite strategies"
  - Solution: Use `mode="overwrite"` or switch to `strategy="insert"`

- **`mode="append"` + `strategy="insert"`**: Allowed and optimized
  - This combination provides optimal append-only behavior

## Migration Steps

### 1. Identify Existing Code Using Default Mode

Find code that relies on the previous overwrite default:

```bash
# Search for write_parquet_dataset calls without explicit mode
rg "write_parquet_dataset\(" --type py
```

### 2. Review Each Usage

For each `write_parquet_dataset()` call, determine the intended behavior:

#### If you WANTED overwrite behavior (old default):
```python
# Before (implicit overwrite)
io.write_parquet_dataset(data, "/path/")

# After (explicit overwrite)
io.write_parquet_dataset(data, "/path/", mode="overwrite")
```

#### If you WANTED append behavior (safer):
```python
# Before
io.write_parquet_dataset(data, "/path/")

# After (explicit, but same as new default)
io.write_parquet_dataset(data, "/path/", mode="append")
# or simply rely on the new default
io.write_parquet_dataset(data, "/path/")
```

#### If you were using merge strategies:
```python
# Before
io.write_parquet_dataset(data, "/path/", strategy="upsert", key_columns=["id"])

# After - must specify mode="overwrite" for merge strategies
io.write_parquet_dataset(
    data,
    "/path/",
    strategy="upsert",
    key_columns=["id"],
    mode="overwrite"  # Required for rewrite strategies
)
```

## Common Migration Scenarios

### Scenario 1: Batch Data Processing
If you have a daily batch job that processes fresh data:

```python
# Old code (dangerous)
def daily_batch_process():
    fresh_data = load_daily_data()
    io.write_parquet_dataset(fresh_data, "/data/daily/")

# New code (safe - appends to existing data)
def daily_batch_process():
    fresh_data = load_daily_data()
    io.write_parquet_dataset(fresh_data, "/data/daily/")  # Now safe by default

# If you actually want to replace data:
def daily_batch_process():
    fresh_data = load_daily_data()
    io.write_parquet_dataset(fresh_data, "/data/daily/", mode="overwrite")
```

### Scenario 2: Real-time Data Ingestion
For append-only data streams:

```python
# Old code
def ingest_stream(data):
    io.write_parquet_dataset(data, "/data/stream/")  # Was dangerous

# New code - now safe by default
def ingest_stream(data):
    io.write_parquet_dataset(data, "/data/stream/")  # Safe append
```

### Scenario 3: Data Replacement Workflows
For workflows that need to replace entire datasets:

```python
# Old code - relied on implicit overwrite
def refresh_dataset():
    new_data = generate_fresh_data()
    io.write_parquet_dataset(new_data, "/data/main/")

# New code - must be explicit about overwrite intent
def refresh_dataset():
    new_data = generate_fresh_data()
    io.write_parquet_dataset(new_data, "/data/main/", mode="overwrite")
```

## Benefits of This Change

### 1. **Safety by Default**
- Prevents accidental data loss from forgotten mode parameters
- New users can't accidentally destroy existing datasets
- Backward compatibility maintained through explicit mode specification

### 2. **Clearer Intent**
- Code explicitly shows whether append or overwrite is intended
- No ambiguity about what the code is doing
- Easier code review and maintenance

### 3. **Better Error Handling**
- Mode/strategy incompatibility is caught early
- Clear error messages guide users to correct usage
- Prevents subtle bugs from incompatible combinations

## Testing Your Migration

After updating your code, verify the behavior:

```python
import tempfile
import pyarrow as pa
from fsspeckit.datasets.duckdb import DuckDBDatasetIO
from fsspeckit.datasets.duckdb.connection import create_duckdb_connection

def test_migration():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test 1: Append mode (new default)
        conn = create_duckdb_connection()
        io = DuckDBDatasetIO(conn)

        # Write initial data
        data1 = pa.table({'id': [1, 2], 'value': ['a', 'b']})
        io.write_parquet_dataset(data1, tmpdir)
        print("✓ Initial write successful")

        # Write more data (should append)
        data2 = pa.table({'id': [3, 4], 'value': ['c', 'd']})
        io.write_parquet_dataset(data2, tmpdir)
        print("✓ Append write successful")

        # Test 2: Overwrite mode
        data3 = pa.table({'id': [5, 6], 'value': ['e', 'f']})
        io.write_parquet_dataset(data3, tmpdir, mode="overwrite")
        print("✓ Overwrite write successful")

        print("✓ Migration test completed successfully")

test_migration()
```

## Compatibility Matrix

| Scenario | Old Behavior | New Behavior | Action Required |
|----------|-------------|--------------|-----------------|
| No mode specified | Overwrite (dangerous) | Append (safe) | Review if you wanted overwrite |
| `mode="append"` | Append | Append (same) | None |
| `mode="overwrite"` | Overwrite | Overwrite (same) | None |
| `strategy="insert"` | Depends on mode | Depends on mode | None |
| `strategy="upsert/update/full_merge/deduplicate"` | Depends on mode | Now requires `mode="overwrite"` | Add `mode="overwrite"` |

## Getting Help

If you encounter issues during migration:

1. **Check error messages**: They now provide clear guidance on mode/strategy compatibility
2. **Review the examples**: Updated docstrings show proper usage patterns
3. **Run tests**: The comprehensive test suite validates the new behavior
4. **Consult documentation**: See the updated API documentation for detailed parameter descriptions

## Summary

This change makes fsspeckit safer by default while maintaining full backward compatibility through explicit mode specification. The migration is straightforward: review your code to determine if you intended overwrite behavior (and add `mode="overwrite"` if so) or if the new append default is actually what you wanted.

The result is more robust, safer code that's less prone to accidental data loss.