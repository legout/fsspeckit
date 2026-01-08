# Getting Started with Datasets

This directory contains beginner-friendly examples that introduce the fundamental concepts of working with datasets using fsspeckit.

## Overview

The examples in this directory are designed to help you:
- Understand basic dataset operations
- Learn when to use DuckDB vs PyArrow approaches
- Master essential data merging techniques
- Build a foundation for more advanced workflows

## Learning Path

We recommend following the examples in numerical order:

### 1. `01_duckdb_basics.py` - DuckDB Fundamentals
**Prerequisites**: Basic Python knowledge
**Time**: 15-20 minutes

**What you'll learn:**
- Basic DuckDBParquetHandler usage
- Simple SQL queries on parquet files
- Context managers for resource management
- Error handling patterns
- Performance basics

**Key concepts:**
- Database-style querying of parquet files
- Resource management best practices
- SQL filtering and aggregation

### 2. `02_pyarrow_basics.py` - PyArrow Dataset Operations
**Prerequisites**: Basic Python knowledge, completion of DuckDB basics
**Time**: 20-25 minutes

**What you'll learn:**
- PyArrow table operations
- Dataset optimization techniques
- Data compaction strategies
- Memory-efficient processing
- Performance comparison with DuckDB

**Key concepts:**
- In-memory columnar operations
- Z-ordering for query optimization
- File organization and compaction
- Memory management strategies

### 3. `03_simple_merges.py` - Dataset Merging
**Prerequisites**: Completion of DuckDB and PyArrow basics
**Time**: 25-30 minutes

**What you'll learn:**
- Basic dataset merging techniques
- Schema consistency handling
- Duplicate detection and removal
- Performance considerations
- Merge strategy selection

**Key concepts:**
- Schema alignment for merging
- Deduplication strategies
- Memory-efficient merging
- Performance trade-offs

### 4. `04_pyarrow_merges.py` - Merge-Aware Writes
**Prerequisites**: Completion of PyArrow basics
**Time**: 20-30 minutes

**What you'll learn:**
- Insert, update, and upsert strategies with `PyarrowDatasetIO.merge`
- Key column selection and merge semantics
- Practical merge workflows for incremental data

**Key concepts:**
- Merge strategies (`insert`, `update`, `upsert`)
- Key column best practices
- Incremental dataset updates

### 5. `05_duckdb_upserts.py` - DuckDB UPSERT Operations
**Prerequisites**: Completion of DuckDB basics
**Time**: 20-30 minutes

**What you'll learn:**
- `INSERT ... ON CONFLICT DO UPDATE` patterns
- Batched upserts and conflict resolution
- Comparing DuckDB UPSERTs to PyArrow merges

**Key concepts:**
- SQL-based upserts
- Conflict resolution strategies
- Batch processing

### 6. `06_pyarrow_maintenance.py` - Dataset Maintenance
**Prerequisites**: Completion of PyArrow basics
**Time**: 15-20 minutes

**What you'll learn:**
- Dataset stats collection
- Compaction and optimization workflows

**Key concepts:**
- Maintenance operations
- File count reduction
- Optimization with compaction

## Key Differences: DuckDB vs PyArrow

### Use DuckDB When:
- You need complex SQL queries
- You have analytical workloads
- You prefer database-style operations
- You need advanced aggregations
- Memory is abundant and you want convenience

### Use PyArrow When:
- You need in-memory performance
- You have simple filtering and transformations
- Memory is constrained
- You want to avoid database overhead
- You need fine-grained control over operations

## Code Patterns

### Basic DuckDB Pattern
```python
from fsspeckit.datasets import DuckDBParquetHandler

with DuckDBParquetHandler() as handler:
    # Write data
    handler.write_parquet(data, "output.parquet")

    # Register and query
    handler.register_dataset("my_data", "data.parquet")
    result = handler.execute_sql("SELECT * FROM my_data WHERE condition")
```

### Basic PyArrow Pattern
```python
import pyarrow.parquet as pq
from fsspeckit.datasets import optimize_parquet_dataset_pyarrow

# Simple operations
data = pq.read_table("data.parquet")
filtered = data.filter(condition)

# Optimization
optimize_parquet_dataset_pyarrow("dataset_path", zorder_columns=["col1", "col2"])
```

### Basic Merge Pattern
```python
import pyarrow as pa

# Ensure schemas match
aligned_tables = [align_schema(table) for table in datasets]

# Merge datasets
merged = pa.concat_tables(aligned_tables)

# Handle duplicates
deduplicated = remove_duplicates(merged)
```

## Common Questions

### Q: When should I choose DuckDB over PyArrow?
**A:** Choose DuckDB for complex analytical queries and when you prefer SQL. Choose PyArrow for simple operations and when you need maximum performance with minimal overhead.

### Q: How do I handle memory issues with large datasets?
**A:** Use PyArrow's streaming capabilities, process data in chunks, or use DuckDB which handles larger-than-memory datasets more gracefully.

### Q: What's the best way to merge datasets with different schemas?
**A:** Align schemas by adding missing columns with null values, or use DuckDB's UNION operations which handle schema differences automatically.

### Q: How can I optimize performance for repeated queries?
**A:** Use PyArrow's Z-ordering on frequently filtered columns, create appropriate indexes in DuckDB, and consider caching intermediate results.

## Next Steps

After completing these examples, you'll be ready for:

### Intermediate Examples
- **Workflow Examples**: Real-world data processing scenarios
- **Performance Optimization**: Advanced tuning techniques
- **Cloud Integration**: Working with remote datasets

### Advanced Examples
- **Large Scale Processing**: Enterprise-grade workflows
- **Real-time Analytics**: Streaming data processing
- **Multi-cloud Operations**: Cross-platform data management

### Integration Examples
- **Cross-Domain Workflows**: Combining multiple fsspeckit packages
- **End-to-End Pipelines**: Complete data processing solutions
- **Production Deployments**: Operational best practices

## Prerequisites Installation

```bash
# Recommended (from repo root)
python -m venv .venv
source .venv/bin/activate
pip install -e ".[datasets]"

# If you use uv instead of pip
uv sync

# Development dependencies (optional)
pip install jupyter matplotlib seaborn
```

This installs the required runtime dependencies for the examples, including
`pyarrow`, `duckdb`, `pandas`, and `polars`.

When using uv, run the examples with `uv run python examples/datasets/getting_started/01_duckdb_basics.py`
to ensure the managed environment is used.

## Getting Help

- **Documentation**: Check the main fsspeckit documentation
- **Examples**: Look at more advanced examples in parent directories
- **Community**: Join discussions in the project repository
- **Issues**: Report bugs or request features on GitHub

## Tips for Success

1. **Start Small**: Test operations with sample data before scaling up
2. **Monitor Resources**: Keep an eye on memory and CPU usage
3. **Validate Results**: Always verify data integrity after operations
4. **Profile Performance**: Measure and optimize for your specific use case
5. **Plan Ahead**: Choose the right approach (DuckDB vs PyArrow) for your needs

## Troubleshooting

### Common Issues

**Memory Errors**
- Reduce dataset size or use chunked processing
- Try DuckDB for large datasets
- Optimize data types to reduce memory usage

**Performance Issues**
- Check if you're using the optimal approach (DuckDB vs PyArrow)
- Consider data optimization (Z-ordering, compression)
- Profile your specific workload

**Schema Mismatches**
- Align schemas before merging
- Use DuckDB's flexible UNION operations
- Handle missing columns appropriately

### Debugging Tips

- Use small sample datasets to test operations
- Print intermediate results to verify correctness
- Monitor system resources during processing
- Check fsspeckit logs for additional information

Remember: These examples are your foundation. Master these concepts before moving on to more complex scenarios!
