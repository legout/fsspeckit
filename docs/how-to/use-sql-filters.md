# Use SQL Filters

This guide shows you how to use fsspeckit's SQL filter translation to convert SQL WHERE clauses into framework-specific filter expressions for PyArrow and Polars.

## Overview

SQL filter translation allows you to:
- Write filters once using familiar SQL syntax
- Apply the same logic across different data frameworks
- Maintain consistent filtering logic across your codebase

## PyArrow Filter Translation

### Basic Usage

```python
import pyarrow as pa
from fsspeckit.sql.filters import sql2pyarrow_filter

# Define schema
schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("category", pa.string()),
    ("value", pa.float64()),
    ("timestamp", pa.timestamp("us"))
])

# Convert SQL to PyArrow filter
sql_filter = "id > 100 AND category IN ('A', 'B', 'C')"
pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)
print(f"PyArrow filter: {pyarrow_filter}")

# Apply filter to dataset
import pyarrow.parquet as pq
dataset = pq.ParquetDataset("data.parquet")
filtered_table = dataset.to_table(filter=pyarrow_filter)
print(f"Filtered rows: {len(filtered_table)}")
```

### Complex SQL Filters

```python
# Complex conditions
sql_filters = [
    "id > 100 AND category IN ('A', 'B', 'C')",
    "value LIKE 'prefix%' AND amount > 1000.0",
    "timestamp >= '2023-01-01' AND timestamp <= '2023-12-31'",
    "category = 'IMPORTANT' AND (amount BETWEEN 100 AND 1000)",
    "(name LIKE 'test%' OR name LIKE 'demo%') AND value > 50"
]

for sql_filter in sql_filters:
    pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)
    print(f"SQL: {sql_filter}")
    print(f"PyArrow: {pyarrow_filter}")
    print()
```

### Real-world Dataset Filtering

```python
import pandas as pd
import pyarrow.parquet as pq
from fsspeckit.sql.filters import sql2pyarrow_filter

# Load a dataset
dataset = pq.ParquetDataset("s3://bucket/large-dataset/")
table = dataset.to_table()

# Define your schema
schema = table.schema

# Use SQL to create filters
sql_conditions = [
    "category = 'HIGH_PRIORITY'",
    "amount > 50000",
    "timestamp >= '2023-06-01'",
    "status IN ('ACTIVE', 'PENDING')"
]

# Apply filters incrementally
filtered_data = table
for condition in sql_conditions:
    filter_expr = sql2pyarrow_filter(condition, schema)
    filtered_data = filtered_data.filter(filter_expr)

print(f"Original rows: {len(table)}")
print(f"Filtered rows: {len(filtered_data)}")
```

## Polars Filter Translation

### Basic Usage

```python
import polars as pl
from fsspeckit.sql.filters import sql2polars_filter

# Define schema
schema = pl.Schema({
    "id": pl.Int64,
    "name": pl.String,
    "category": pl.String,
    "value": pl.Float64,
    "timestamp": pl.Datetime
})

# Convert SQL to Polars filter
sql_filter = "value LIKE 'prefix%' AND timestamp >= '2023-01-01'"
polars_filter = sql2polars_filter(sql_filter, schema)
print(f"Polars filter: {polars_filter}")

# Apply filter to DataFrame
df = pl.read_parquet("data.parquet")
filtered_df = df.filter(polars_filter)
print(f"Filtered rows: {len(filtered_df)}")
```

### Complex Polars Filters

```python
# Multiple conditions
sql_filters = [
    "id > 100 AND name = 'test'",
    "value BETWEEN 10 AND 100 OR category = 'SPECIAL'",
    "timestamp >= '2023-01-01' AND (status IN ('ACTIVE', 'PENDING'))",
    "name LIKE '%test%' AND value > 0",
    "category IN ('A', 'B', 'C') AND timestamp <= '2023-12-31'"
]

polars_schema = pl.Schema({
    "id": pl.Int64,
    "name": pl.String,
    "category": pl.String,
    "value": pl.Float64,
    "timestamp": pl.Datetime,
    "status": pl.String
})

for sql_filter in sql_filters:
    polars_filter = sql2polars_filter(sql_filter, polars_schema)
    print(f"SQL: {sql_filter}")
    print(f"Polars: {polars_filter}")
    print()
```

### DataFrame Filtering

```python
# Load DataFrame
df = pl.read_parquet("large_dataset.parquet")

# Define schema from DataFrame
schema = df.schema

# Apply multiple filters
filters = [
    "amount > 1000",
    "category = 'PREMIUM'",
    "timestamp >= '2023-01-01'"
]

filtered_df = df
for filter_sql in filters:
    filter_expr = sql2polars_filter(filter_sql, schema)
    filtered_df = filtered_df.filter(filter_expr)

print(f"Original rows: {len(df)}")
print(f"Filtered rows: {len(filtered_df)}")
```

## Cross-Framework Compatibility

### Same SQL, Different Frameworks

```python
import pyarrow as pa
import polars as pl
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter

# Define schemas
pyarrow_schema = pa.schema([
    ("id", pa.int64()),
    ("category", pa.string()),
    ("value", pa.float64()),
    ("timestamp", pa.timestamp("us"))
])

polars_schema = pl.Schema({
    "id": pl.Int64,
    "category": pl.String,
    "value": pl.Float64,
    "timestamp": pl.Datetime
})

# Same SQL filter for both frameworks
sql_filter = "category IN ('A', 'B') AND value > 100.0 AND timestamp >= '2023-01-01'"

# Convert to both frameworks
pyarrow_filter = sql2pyarrow_filter(sql_filter, pyarrow_schema)
polars_filter = sql2polars_filter(sql_filter, polars_schema)

print(f"SQL: {sql_filter}")
print(f"PyArrow: {pyarrow_filter}")
print(f"Polars: {polars_filter}")

# Apply to data
# PyArrow
import pyarrow.parquet as pq
dataset = pq.ParquetDataset("data.parquet")
arrow_result = dataset.to_table(filter=pyarrow_filter)

# Polars
df = pl.read_parquet("data.parquet")
polars_result = df.filter(polars_filter)

print(f"PyArrow result: {len(arrow_result)} rows")
print(f"Polars result: {len(polars_result)} rows")
```

## Advanced SQL Features

### Supported SQL Operators

```python
# Comparison operators
sql_filters = [
    "id = 100",                    # Equals
    "id != 100",                   # Not equals
    "id > 100",                    # Greater than
    "id >= 100",                   # Greater than or equal
    "id < 100",                    # Less than
    "id <= 100",                   # Less than or equal
    "value BETWEEN 10 AND 100"      # Between
]

# Logical operators
sql_filters.extend([
    "id > 100 AND name = 'test'",           # AND
    "id > 100 OR name = 'test'",            # OR
    "NOT (id > 100)",                      # NOT
    "(id > 100 AND name = 'test') OR value = 50"  # Complex logic
])

# String operations
sql_filters.extend([
    "name LIKE 'prefix%'",                   # LIKE with wildcard
    "name LIKE '%suffix'",                   # LIKE with wildcard
    "name LIKE '%middle%'",                  # LIKE with wildcard
    "name IN ('value1', 'value2', 'value3')",  # IN clause
    "name NOT IN ('bad1', 'bad2')"         # NOT IN clause
])

# Null operations
sql_filters.extend([
    "name IS NULL",                          # IS NULL
    "name IS NOT NULL",                      # IS NOT NULL
])
```

### Date and Time Operations

```python
# Date/time filters
datetime_filters = [
    "timestamp >= '2023-01-01'",
    "timestamp > '2023-01-01 10:30:00'",
    "timestamp BETWEEN '2023-01-01' AND '2023-12-31'",
    "DATE(timestamp) = '2023-01-15'",
    "YEAR(timestamp) = 2023",
    "MONTH(timestamp) = 6",
    "DAY(timestamp) = 15"
]

# Apply to dataset
for filter_sql in datetime_filters:
    filter_expr = sql2pyarrow_filter(filter_sql, schema)
    filtered_data = dataset.to_table(filter=filter_expr)
    print(f"{filter_sql}: {len(filtered_data)} rows")
```

## Practical Examples

### E-commerce Analytics

```python
# E-commerce dataset filtering
ecommerce_filters = [
    # High-value customers
    "total_spent > 1000 AND customer_type = 'PREMIUM'",
    
    # Recent active users
    "last_purchase >= '2023-06-01' AND login_count > 10",
    
    # Specific product categories
    "category IN ('Electronics', 'Books', 'Home') AND price > 50",
    
    # Geographic filtering
    "country IN ('US', 'CA', 'UK') AND order_total > 100",
    
    # Time-based campaigns
    "order_date >= '2023-11-01' AND order_date <= '2023-11-30' AND campaign_id IS NOT NULL"
]

for filter_sql in ecommerce_filters:
    filter_expr = sql2pyarrow_filter(filter_sql, ecommerce_schema)
    results = dataset.to_table(filter=filter_expr)
    print(f"{filter_sql}: {len(results)} orders")
```

### IoT Data Processing

```python
# IoT sensor data filtering
iot_filters = [
    # Sensor health checks
    "sensor_type = 'temperature' AND value BETWEEN -40 AND 125",
    
    # Anomaly detection
    "value > (avg_value + 3 * std_dev) OR value < (avg_value - 3 * std_dev)",
    
    # Device status
    "device_status = 'ACTIVE' AND battery_level > 20",
    
    # Time windows
    "timestamp >= '2023-01-01' AND timestamp < '2023-01-02' AND location = 'warehouse_a'",
    
    # Data quality
    "signal_strength > 0.8 AND error_rate < 0.01"
]

for filter_sql in iot_filters:
    filter_expr = sql2polars_filter(filter_sql, iot_schema)
    filtered_df = iot_df.filter(filter_expr)
    print(f"{filter_sql}: {len(filtered_df)} readings")
```

### Financial Data Analysis

```python
# Financial transaction filtering
financial_filters = [
    # Large transactions
    "amount > 10000 AND transaction_type = 'WIRE'",
    
    # Suspicious patterns
    "amount > 5000 AND time_of_day BETWEEN 2 AND 5 AND country != 'US'",
    
    # Regulatory reporting
    "transaction_type IN ('WIRE', 'ACH', 'INTERNATIONAL') AND amount > 1000",
    
    # Risk assessment
    "risk_score > 80 AND customer_age < 25 AND amount > 5000",
    
    # Compliance checks
    "sanctioned_country = FALSE AND amount > 100000 AND verified_customer = TRUE"
]

for filter_sql in financial_filters:
    filter_expr = sql2pyarrow_filter(filter_sql, financial_schema)
    compliance_data = dataset.to_table(filter=filter_expr)
    print(f"{filter_sql}: {len(compliance_data)} transactions")
```

## Performance Considerations

### Filter Optimization

```python
# Good: Specific filters that can be pushed down
good_filters = [
    "id = 100",                    # Equality on indexed column
    "category = 'IMPORTANT'",        # Low cardinality column
    "timestamp >= '2023-01-01'"     # Range filter
]

# Less optimal: Complex expressions
complex_filters = [
    "UPPER(name) LIKE 'TEST%'",      # Function on column
    "value * 1.1 > 100",           # Calculation on column
    "SUBSTRING(description, 1, 5) = 'ERROR'"  # String function
]

# Use specific filters when possible
for filter_sql in good_filters:
    filter_expr = sql2pyarrow_filter(filter_sql, schema)
    filtered_data = dataset.to_table(filter=filter_expr)
```

### Batch Filtering

```python
# Apply multiple filters efficiently
def apply_multiple_filters(dataset, filters, schema):
    """Apply multiple filters with early exit"""
    result = dataset.to_table()
    
    for i, filter_sql in enumerate(filters):
        filter_expr = sql2pyarrow_filter(filter_sql, schema)
        result = result.filter(filter_expr)
        
        print(f"Filter {i+1} ({filter_sql}): {len(result)} rows")
        
        # Early exit if no data
        if len(result) == 0:
            break
    
    return result

# Usage
filters = [
    "category = 'ACTIVE'",
    "timestamp >= '2023-01-01'",
    "value > 100"
]

result = apply_multiple_filters(dataset, filters, schema)
```

## Error Handling

### Schema Validation

```python
def safe_filter_conversion(sql_filter, schema):
    """Convert SQL filter with error handling"""
    try:
        filter_expr = sql2pyarrow_filter(sql_filter, schema)
        return filter_expr, None
    except Exception as e:
        return None, str(e)

# Test with invalid filters
test_filters = [
    "id > 100",                    # Valid
    "invalid_column = 100",          # Invalid column
    "id > 'not_a_number'",          # Type mismatch
    "id > 100 AND"                 # Invalid SQL syntax
]

for filter_sql in test_filters:
    filter_expr, error = safe_filter_conversion(filter_sql, schema)
    if error:
        print(f"Error in '{filter_sql}': {error}")
    else:
        print(f"Valid filter: {filter_sql}")
```

### Fallback Strategies

```python
def apply_filter_with_fallback(dataset, sql_filter, schema):
    """Apply filter with fallback options"""
    try:
        # Try PyArrow filter
        filter_expr = sql2pyarrow_filter(sql_filter, schema)
        return dataset.to_table(filter=filter_expr)
    except Exception as e:
        print(f"PyArrow filter failed: {e}")
        
        try:
            # Fallback to Polars
            import polars as pl
            df = pl.from_arrow(dataset.to_table())
            polars_filter = sql2polars_filter(sql_filter, df.schema)
            return df.filter(polars_filter).to_arrow()
        except Exception as e2:
            print(f"Polars filter also failed: {e2}")
            
            # Final fallback: load all data and filter manually
            print("Loading all data and filtering manually")
            return dataset.to_table()

# Usage
result = apply_filter_with_fallback(dataset, "id > 100", schema)
```

## Best Practices

1. **Use Specific Columns**: Filter on indexed or low-cardinality columns when possible
2. **Avoid Functions**: Don't use functions on columns in WHERE clauses (e.g., `UPPER(name)`)
3. **Type Consistency**: Ensure filter values match column data types
4. **Schema Validation**: Always provide accurate schema for conversion
5. **Error Handling**: Implement proper error handling for invalid SQL
6. **Performance Testing**: Test filter performance on large datasets
7. **Incremental Filtering**: Apply multiple simple filters instead of one complex filter

For more information on dataset operations, see [Read and Write Datasets](read-and-write-datasets.md).