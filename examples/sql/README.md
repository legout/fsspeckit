# SQL Filter Examples

This directory contains examples demonstrating how to use SQL filters with fsspeckit across different data processing backends.

## Overview

The SQL filter functionality in fsspeckit allows you to:
- Write SQL WHERE clauses that work with PyArrow, Polars, and DuckDB
- Convert SQL expressions to platform-specific filter syntaxes
- Maintain backend-agnostic filter logic in your applications
- Leverage SQL familiarity for complex data filtering

## Examples

### 1. `sql_filter_basic.py`
**Level**: Beginner
**Focus**: Fundamental SQL filter usage

Learn how to:
- Convert basic SQL WHERE clauses to PyArrow and Polars filters
- Use common comparison operators (=, >, <, >=, <=, !=)
- Apply string patterns with LIKE
- Handle date comparisons and boolean conditions
- Ensure consistent results across platforms

**Run**: `python sql_filter_basic.py`

### 2. `sql_filter_advanced.py`
**Level**: Advanced
**Focus**: Complex filtering scenarios

Learn how to:
- Build complex boolean logic with AND/OR conditions
- Use IN clauses and BETWEEN ranges
- Handle NULL values and nested conditions
- Apply mathematical expressions in filters
- Optimize filter performance

**Run**: `python sql_filter_advanced.py`

### 3. `cross_platform_filters.py`
**Level**: Intermediate
**Focus**: Backend consistency and best practices

Learn how to:
- Write backend-agnostic SQL filters
- Compare results across PyArrow, Polars, and DuckDB
- Handle platform-specific limitations
- Choose appropriate backends for different scenarios
- Apply cross-platform development best practices

**Run**: `python cross_platform_filters.py`

## Supported SQL Features

### ✅ Fully Supported
- **Basic comparisons**: `=`, `!=`, `>`, `<`, `>=`, `<=`
- **Logical operators**: `AND`, `OR`, `NOT`
- **Range filters**: `BETWEEN`
- **List filters**: `IN`
- **String patterns**: `LIKE` with `%` wildcards
- **NULL checks**: `IS NULL`, `IS NOT NULL`
- **Boolean values**: `true`, `false`
- **Date/time comparisons**
- **Mathematical expressions**: `+`, `-`, `*`, `/`

### ⚠️ Limited Support
- **String functions**: `LOWER()`, `UPPER()`, `LENGTH()` (platform-dependent)
- **Date functions**: Platform-specific implementations
- **Complex nested expressions**: May vary by platform

### ❌ Not Supported
- **Aggregate functions** in WHERE clauses
- **Window functions**
- **Subqueries**
- **JOIN operations** in filters

## Performance Tips

1. **Use indexed columns first** in multi-column filters
2. **Prefer range filters** (`BETWEEN`) over multiple inequalities
3. **Use IN clauses** instead of multiple OR conditions
4. **Place highly selective conditions** early in complex filters
5. **Consider partition pruning** with partition columns
6. **Profile performance** on your specific data and backend

## Backend Considerations

### PyArrow
- Excellent for columnar data operations
- Strong integration with Arrow memory format
- Good for large-scale data processing
- Some SQL function limitations

### Polars
- Very fast for in-memory operations
- Rich expression system
- Excellent for data manipulation
- Good SQL filter support

### DuckDB
- Full SQL support beyond filters
- Excellent for analytical queries
- Strong for complex aggregations
- Can handle larger-than-memory datasets

## Usage Patterns

### Basic Filtering
```python
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

sql_filter = "age > 30 AND salary > 50000"

# PyArrow
pyarrow_filter = sql2pyarrow_filter(sql_filter)
filtered_table = dataset.to_table(filter=pyarrow_filter)

# Polars
polars_filter = sql2polars_filter(sql_filter)
filtered_df = df.filter(polars_filter)
```

### Complex Filtering
```python
sql_filter = """
    (department IN ('Engineering', 'Sales') AND salary > 80000) OR
    (performance_score > 4.5 AND years_experience > 5)
"""
filter_expr = sql2pyarrow_filter(sql_filter)
```

## Integration Examples

These SQL filters integrate seamlessly with other fsspeckit examples:

- **DuckDB Examples**: Use SQL filters with DuckDBParquetHandler
- **PyArrow Examples**: Combine with dataset optimization
- **Storage Options**: Apply filters to cloud-based datasets
- **Batch Processing**: Filter data before batch operations

## Error Handling

Always wrap filter conversion in try-except blocks:

```python
try:
    filter_expr = sql2pyarrow_filter(sql_filter)
    filtered_data = dataset.to_table(filter=filter_expr)
except Exception as e:
    print(f"Filter conversion failed: {e}")
    # Fallback or alternative approach
```

## Testing

Each example includes comprehensive test scenarios with realistic data. Run the examples to verify:

1. Filter conversion works correctly
2. Results are consistent across backends
3. Performance meets your requirements
4. Edge cases are handled properly

## Dependencies

```bash
pip install fsspeckit[sql] pyarrow polars
```

For DuckDB support:
```bash
pip install fsspeckit[datasets]
```

## Troubleshooting

### Common Issues

1. **Filter not supported**: Check the supported features list
2. **Performance issues**: Try simplifying complex filters
3. **Inconsistent results**: Verify data types and null handling
4. **Import errors**: Ensure all dependencies are installed

### Getting Help

- Check the fsspeckit documentation for detailed API reference
- Review the advanced examples for complex scenarios
- Test with simple filters before building complex ones
- Use the cross-platform examples to understand differences