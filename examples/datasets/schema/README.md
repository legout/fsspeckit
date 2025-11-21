# Schema Management Examples

This directory contains examples demonstrating schema management and optimization techniques for datasets using fsspeckit.

## Overview

Schema management is crucial for:
- Combining data from multiple sources with different structures
- Optimizing storage efficiency and query performance
- Handling data evolution and versioning
- Ensuring data consistency across pipelines
- Reducing memory and storage costs

## Examples

### 1. `schema_basics.py`
**Level**: Beginner
**Focus**: Fundamental schema operations

Learn how to:
- Inspect and analyze dataset schemas
- Validate schema consistency
- Handle null values in schemas
- Perform basic type conversion
- Work with different PyArrow data types

**Run**: `python schema_basics.py`

### 2. `schema_unification.py`
**Level**: Intermediate
**Focus**: Combining datasets with different schemas

Learn how to:
- Detect schema differences between datasets
- Handle missing columns and type conflicts
- Map column names between different conventions
- Unify schemas from multiple data sources
- Work with partitioned datasets with schema evolution

**Run**: `python schema_unification.py`

### 3. `type_optimization.py`
**Level**: Advanced
**Focus**: Performance and storage optimization

Learn how to:
- Analyze current data types for optimization opportunities
- Optimize integer types based on value ranges
- Apply dictionary encoding for low-cardinality strings
- Choose optimal floating-point precision
- Benchmark performance impact of optimizations
- Select appropriate compression codecs

**Run**: `python type_optimization.py`

## Key Concepts

### Schema Types

#### Arrow Schema
```python
import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string()),
    pa.field("value", pa.float32())
])
```

#### Schema Evolution
- **Additive**: Adding new columns (safe)
- **Destructive**: Removing or changing columns (requires migration)
- **Type changes**: Modifying data types (may require conversion)

### Data Type Optimization

#### Integer Types
| Type | Range | Storage |
|------|-------|---------|
| int8 | -128 to 127 | 1 byte |
| int16 | -32,768 to 32,767 | 2 bytes |
| int32 | -2.1B to 2.1B | 4 bytes |
| int64 | -9.2E18 to 9.2E18 | 8 bytes |

#### Floating-Point Types
| Type | Precision | Storage |
|------|-----------|---------|
| float32 | ~7 decimal digits | 4 bytes |
| float64 | ~16 decimal digits | 8 bytes |

#### Dictionary Encoding
- **Best for**: Low cardinality strings (< 50% unique values)
- **Benefits**: Reduced memory usage, faster comparisons
- **Trade-offs**: Slight encoding/decoding overhead

### Schema Operations

#### Type Casting
```python
from fsspeckit.datasets import cast_schema

# Cast entire table to new schema
new_schema = pa.schema([
    pa.field("id", pa.int32()),
    pa.field("price", pa.float32())
])

casted_table = cast_schema(original_table, new_schema)
```

#### Type Optimization
```python
from fsspeckit.datasets import opt_dtype_pa

# Optimize individual types
optimized_type = opt_dtype_pa(original_type, data_values)
```

## Performance Guidelines

### Memory Optimization
1. **Use appropriate integer types** based on value ranges
2. **Apply dictionary encoding** for repetitive string values
3. **Consider float32** when high precision isn't needed
4. **Use nullable types** only when nulls are expected

### Storage Optimization
1. **Choose compression based on use case**:
   - **Snappy**: Fast compression/decompression
   - **Gzip**: Better compression, slower
   - **ZSTD**: Best compression ratio, modern
   - **Brotli**: Good for text data

2. **Partitioning strategy**:
   - Partition on frequently filtered columns
   - Keep partition cardinality manageable
   - Consider query patterns

### Query Performance
1. **Filter order matters**: Most selective conditions first
2. **Column pruning**: Only select needed columns
3. **Predicate pushdown**: Apply filters early in the pipeline

## Common Schema Patterns

### Time Series Data
```python
time_series_schema = pa.schema([
    pa.field("timestamp", pa.timestamp('s'), nullable=False),
    pa.field("metric_name", pa.dictionary(pa.int32(), pa.string())),
    pa.field("value", pa.float64()),
    pa.field("tags", pa.struct([
        pa.field("source", pa.string()),
        pa.field("environment", pa.string())
    ]))
])
```

### Event Data
```python
event_schema = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("event_type", pa.dictionary(pa.int16(), pa.string())),
    pa.field("user_id", pa.string()),
    pa.field("timestamp", pa.timestamp('us')),
    pa.field("properties", pa.struct([]))  # Flexible schema
])
```

### Master Data
```python
master_schema = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("name", pa.string()),
    pa.field("category", pa.dictionary(pa.int8(), pa.string())),
    pa.field("is_active", pa.bool_()),
    pa.field("created_at", pa.timestamp('s')),
    pa.field("updated_at", pa.timestamp('s'))
])
```

## Integration with fsspeckit

These schema examples integrate with other fsspeckit examples:

- **DuckDB Examples**: Apply schema optimization before analytics
- **PyArrow Examples**: Use optimized schemas for better performance
- **SQL Filters**: Apply filters to consistently structured data
- **Batch Processing**: Process data with unified schemas

## Error Handling

Always handle potential schema conflicts:

```python
try:
    # Try to cast to target schema
    unified_table = cast_schema(data, target_schema)
except Exception as e:
    print(f"Schema cast failed: {e}")
    # Apply manual column mapping or type conversion
```

## Testing Schema Operations

```python
def validate_schema_conversion(original, converted, target_schema):
    """Validate that schema conversion preserved data integrity."""

    # Check schema matches
    assert converted.schema.equals(target_schema)

    # Check row count preserved
    assert len(original) == len(converted)

    # Validate data integrity for key columns
    for field in target_schema:
        if not field.nullable:
            assert converted.column(field.name).null_count == 0
```

## Best Practices

1. **Schema versioning**: Track schema changes over time
2. **Documentation**: Document schema decisions and constraints
3. **Testing**: Validate schema operations with realistic data
4. **Monitoring**: Track schema drift and data quality issues
5. **Migration planning**: Plan schema changes carefully for production data

## Troubleshooting

### Common Issues

1. **Type conversion failures**: Check for incompatible types or data values
2. **Memory issues**: Optimize types for large datasets
3. **Performance problems**: Analyze and optimize frequently accessed schemas
4. **Schema conflicts**: Use careful column mapping and type reconciliation

### Debugging Tips

- Use `table.schema` to inspect current structure
- Check `column.null_count` for unexpected nulls
- Profile memory usage with `table.nbytes`
- Test with small samples before full datasets

## Dependencies

```bash
pip install fsspeckit[datasets] pyarrow pandas
```

Optional for advanced analysis:
```bash
pip install pyarrow-dev  # Development version with latest features
```