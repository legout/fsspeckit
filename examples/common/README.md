# Common Utilities Examples

This directory contains examples demonstrating the cross-cutting utilities available in fsspeckit.common for common data processing tasks.

## Overview

The common utilities provide essential functionality for:
- Logging and monitoring data processing workflows
- Parallel processing for improved performance
- Type conversion between different data formats
- Cross-platform compatibility and portability

## Examples

### 1. `logging_setup.py`
**Level**: Beginner to Intermediate
**Focus**: Configuring comprehensive logging

Learn how to:
- Set up basic and advanced logging configurations
- Log to multiple outputs (console, files, JSON)
- Use structured logging with contextual information
- Handle different log levels and filtering
- Integrate logging with fsspeckit operations

**Run**: `python logging_setup.py`

### 2. `parallel_processing.py`
**Level**: Intermediate
**Focus**: Multi-core data processing

Learn how to:
- Use `run_parallel()` for CPU-bound operations
- Process multiple files and datasets in parallel
- Handle errors and failures in parallel workflows
- Monitor performance and resource usage
- Choose optimal worker configurations

**Run**: `python parallel_processing.py`

### 3. `type_conversion.py`
**Level**: Intermediate to Advanced
**Focus**: Data format conversion and optimization

Learn how to:
- Convert between PyArrow, Pandas, and Polars
- Apply type-safe conversion patterns
- Optimize memory usage with type conversions
- Handle problematic data gracefully
- Choose optimal data types for performance

**Run**: `python type_conversion.py`

## Key Concepts

### Logging Architecture

#### Configuration Levels
```python
from fsspeckit.common.logging import setup_logging

# Basic configuration
logger = setup_logging(level="INFO")

# Advanced configuration
logger = setup_logging(
    level="DEBUG",
    log_file="app.log",
    json_format=True,
    max_file_size="10MB",
    backup_count=5
)
```

#### Structured Logging
```python
logger.info("Processing data", extra={
    "job_id": "job_123",
    "dataset": "sales_2024",
    "record_count": 50000,
    "processing_time": 45.2
})
```

### Parallel Processing Patterns

#### Basic Parallel Execution
```python
from fsspeckit.common.misc import run_parallel

def process_file(file_path):
    # Process single file
    return result

# Process multiple files in parallel
files = ["file1.parquet", "file2.parquet", "file3.parquet"]
results = run_parallel(process_file, files, max_workers=4)
```

#### Error Handling
```python
def safe_process(file_path):
    try:
        return process_file(file_path)
    except Exception as e:
        return {"error": str(e), "file": file_path}

results = run_parallel(safe_process, files)
successful = [r for r in results if "error" not in r]
```

### Type Conversion Strategies

#### Format Selection Guidelines
- **PyArrow**: Best for columnar analytics, large datasets, memory efficiency
- **Pandas**: Best for data manipulation, statistical analysis, machine learning
- **Polars**: Best for high-performance data processing, complex queries

#### Conversion Functions
```python
from fsspeckit.common.types import to_pyarrow_table, to_pandas_df, to_polars_df

# Convert between formats
arrow_table = create_arrow_table()
pandas_df = to_pandas_df(arrow_table)
polars_df = to_polars_df(arrow_table)
```

## Performance Guidelines

### Logging Performance
1. **Use appropriate log levels** - avoid excessive debug logging in production
2. **Structured logging overhead** - JSON format has slightly more overhead
3. **File rotation** - Configure appropriate file sizes and retention
4. **Async logging** - Consider async handlers for high-throughput applications

### Parallel Processing
1. **CPU-bound vs I/O-bound** - Parallel processing works best for CPU-bound tasks
2. **Worker count** - Use `min(cpu_count(), reasonable_limit)` for optimal performance
3. **Chunk size** - Balance between overhead and parallelism benefits
4. **Memory management** - Monitor memory usage with large datasets

### Type Conversion
1. **Choose optimal types** - Use smallest sufficient integer/float types
2. **Dictionary encoding** - Apply to low-cardinality strings
3. **Early filtering** - Reduce data size before expensive conversions
4. **Batch processing** - Process large datasets in chunks

## Integration Patterns

### End-to-End Workflow
```python
import logging
from fsspeckit.common.logging import setup_logging
from fsspeckit.common.misc import run_parallel
from fsspeckit.common.types import to_pyarrow_table

# Setup logging
logger = setup_logging(
    level="INFO",
    log_file="pipeline.log",
    json_format=True
)

# Process files in parallel
def process_dataset(file_path):
    logger.info(f"Processing {file_path}")

    # Load and convert data
    raw_data = load_data(file_path)
    arrow_data = to_pyarrow_table(raw_data)

    # Process data
    result = transform_data(arrow_data)

    logger.info(f"Completed {file_path}", extra={
        "file": file_path.name,
        "records": len(result)
    })

    return result

# Run pipeline
files = list_files("data/")
results = run_parallel(process_dataset, files, max_workers=4)
```

### Error Recovery
```python
def robust_processing(file_path):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return process_file(file_path)
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {file_path}",
                         extra={"error": str(e), "attempt": attempt + 1})
            if attempt == max_retries - 1:
                logger.error(f"Failed to process {file_path}", exc_info=True)
                return {"error": str(e), "file": file_path}
            time.sleep(2 ** attempt)  # Exponential backoff
```

## Best Practices

### Logging
1. **Use structured data** for machine-readable logs
2. **Include context** like job IDs, dataset names, record counts
3. **Log at appropriate levels** - debug for development, info for operations
4. **Monitor log volume** and implement sampling if needed

### Parallel Processing
1. **Test with different worker counts** to find optimal configuration
2. **Handle failures gracefully** and continue processing other items
3. **Monitor resource usage** (CPU, memory, I/O)
4. **Consider streaming patterns** for very large datasets

### Type Conversion
1. **Validate data quality** before expensive conversions
2. **Use type-safe patterns** for unreliable data sources
3. **Optimize types** based on your data characteristics
4. **Profile memory usage** for large datasets

## Dependencies

```bash
# Core dependencies
pip install fsspeckit[common] pyarrow

# Optional for pandas support
pip install pandas

# Optional for polars support
pip install polars

# Development dependencies
pip install psutil  # For resource monitoring
```

## Troubleshooting

### Common Issues

**Logging Problems**
- Permission issues with log files
- Disk space exhaustion from large logs
- Performance impact from excessive logging

**Parallel Processing Issues**
- Deadlocks or race conditions
- Memory exhaustion with too many workers
- I/O bottlenecks overwhelming the system

**Type Conversion Issues**
- Incompatible data types between formats
- Memory pressure during large conversions
- Loss of precision in numeric conversions

### Debugging Tips

- Use smaller datasets when testing
- Monitor system resources during processing
- Enable debug logging to trace issues
- Test conversions with sample data first

## Advanced Usage

### Custom Logging Handlers
```python
import logging
from fsspeckit.common.logging import setup_logging

# Add custom handler for specific requirements
class CustomHandler(logging.Handler):
    def emit(self, record):
        # Custom processing logic
        pass

logger = setup_logging(level="INFO")
logger.addHandler(CustomHandler())
```

### Dynamic Worker Scaling
```python
import multiprocessing

# Adaptive worker count based on dataset size
def determine_workers(file_count):
    base_workers = min(multiprocessing.cpu_count(), file_count)
    return max(1, base_workers // 2)  # Conservative estimate

files = glob.glob("data/*.parquet")
workers = determine_workers(len(files))
results = run_parallel(process_file, files, max_workers=workers)
```

### Memory-Efficient Conversion
```python
def convert_large_dataset(source_path, target_format):
    chunk_size = 10000
    results = []

    for chunk in read_in_chunks(source_path, chunk_size):
        # Convert chunk to target format
        converted = convert_chunk(chunk, target_format)
        results.append(converted)

        # Cleanup original chunk
        del chunk

    return combine_results(results)
```