# Workflow Examples

This directory contains intermediate-level examples that demonstrate real-world workflows and advanced dataset operations using fsspeckit.

## Overview

The workflow examples build on the foundational knowledge from the getting started section and show how to apply fsspeckit in practical, production scenarios.

## Prerequisites

Before diving into these examples, you should have completed:
- Basic DuckDB and PyArrow operations
- Understanding of fundamental dataset merging
- Familiarity with SQL filtering and schema management
- Basic understanding of cloud storage concepts

## Available Workflows

### 1. `cloud_datasets.py` - Cloud Storage Workflows
**Prerequisites**: Getting Started completion
**Time**: 30-35 minutes
**Complexity**: Intermediate

**What you'll learn:**
- Working with S3, Azure Blob, and Google Cloud Storage
- Configuring authentication and security
- Optimizing for cloud performance and cost
- Implementing robust error handling and retry logic
- Data organization strategies for cloud environments

**Key concepts:**
- Cloud storage providers and their differences
- Partitioning strategies for cloud queries
- Authentication patterns and security best practices
- Cost optimization techniques
- Error handling for network operations

### 2. `performance_optimization.py` - High-Performance Operations
**Prerequisites**: Getting Started completion
**Time**: 35-45 minutes
**Complexity**: Intermediate to Advanced

**What you'll learn:**
- Query performance optimization techniques
- Memory management for large datasets
- Parallel processing strategies
- I/O optimization and file organization
- Performance monitoring and profiling
- Scaling considerations

**Key concepts:**
- Query planning and optimization
- Memory-efficient data types
- Parallel processing patterns
- Compression and file sizing
- Resource monitoring and profiling

## Learning Path

### Recommended Order

1. **Cloud Datasets** - Start here if you work with cloud storage
2. **Performance Optimization** - Essential for large-scale processing

### Choosing Your Focus

**For Cloud Environments:**
- Focus on `cloud_datasets.py`
- Learn authentication and security patterns
- Understand cost optimization strategies

**For On-Premise or Large-Scale:**
- Focus on `performance_optimization.py`
- Master memory management and parallel processing
- Learn profiling and monitoring techniques

## Integration Patterns

### Cloud + Performance Optimization

```python
# Combine cloud access with performance techniques
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.storage_options import AwsStorageOptions

# Configure optimized cloud access
aws_options = AwsStorageOptions(
    region="us-east-1",
    use_ssl=True,
    compression="snappy"
)

with DuckDBParquetHandler() as handler:
    # Register cloud dataset with optimized queries
    handler.register_dataset("cloud_sales", "s3://bucket/data/")

    # Perform optimized query
    result = handler.execute_sql("""
        SELECT region, SUM(net_amount) as total
        FROM cloud_sales
        WHERE year = 2024
        GROUP BY region
    """)
```

### Performance + Cloud Optimization

```python
# Optimize data before cloud upload
from fsspeckit.datasets import optimize_parquet_dataset_pyarrow

# Optimize for cloud queries
optimize_parquet_dataset_pyarrow(
    "/local/data/",
    zorder_columns=["region", "quarter", "channel"],
    target_file_size_mb=128,
    compression="snappy"
)

# Upload optimized data
upload_to_cloud("/local/data/", "s3://bucket/optimized/")
```

## Advanced Techniques Covered

### Cloud Storage Patterns

1. **Multi-Cloud Strategies**
   - Working with different providers simultaneously
   - Cross-cloud data migration
   - Provider-specific optimizations

2. **Security Best Practices**
   - IAM roles and policies
   - Encrypted data transfer
   - Access control patterns

3. **Cost Optimization**
   - Data lifecycle management
   - Tiered storage strategies
   - Query optimization for cloud costs

### Performance Patterns

1. **Memory Management**
   - Type optimization strategies
   - Streaming large datasets
   - Memory leak detection and prevention

2. **Parallel Processing**
   - CPU-bound vs I/O-bound operations
   - Worker count optimization
   - Load balancing strategies

3. **I/O Optimization**
   - File organization and partitioning
   - Compression codec selection
   - Batch vs. stream processing

## Real-World Applications

### Business Intelligence Workflows

```python
# Typical BI pipeline
def bi_pipeline():
    # 1. Extract from cloud storage
    cloud_data = load_from_s3("sales-data/")

    # 2. Transform and optimize
    optimized_data = transform_and_optimize(cloud_data)

    # 3. Load into analytics engine
    load_to_analytics_engine(optimized_data)
```

### Machine Learning Workflows

```python
# ML data preparation
def ml_data_preparation():
    # 1. Load large dataset with memory optimization
    raw_data = load_large_dataset_memory_efficient("training-data/")

    # 2. Parallel feature engineering
    features = engineer_features_parallel(raw_data)

    # 3. Optimize for ML training
    ml_data = optimize_for_training(features)
```

## Performance Benchmarks

### Expected Performance Metrics

**Cloud Operations:**
- File read: 10-50 MB/s (depending on provider and region)
- Query latency: 100-500ms (for optimized queries)
- Throughput: 100-1000 records/second

**Optimized Operations:**
- Memory reduction: 30-70% (with type optimization)
- Query speedup: 2-10x (with proper indexing)
- Parallel efficiency: 60-80% (with optimal worker count)

## Monitoring and Observability

### Key Metrics to Track

**Performance Metrics:**
- Query execution time
- Memory usage patterns
- CPU utilization
- I/O throughput
- Network bandwidth (cloud)

**Business Metrics:**
- Data processing latency
- Cost per query/operation
- Success/error rates
- Data freshness

### Monitoring Setup

```python
# Add performance monitoring to your workflows
import time
import psutil

def monitor_operation(operation_func, name):
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss

    result = operation_func()

    end_time = time.time()
    end_memory = psutil.Process().memory_info().rss

    log_metrics({
        "operation": name,
        "duration": end_time - start_time,
        "memory_delta": end_memory - start_memory
    })

    return result
```

## Troubleshooting

### Common Performance Issues

**Memory Issues:**
- Out of memory errors with large datasets
- Memory leaks in long-running processes
- Inefficient data types

**Cloud Issues:**
- Authentication and connectivity problems
- Transfer speed bottlenecks
- Cost overruns

**Performance Issues:**
- Slow query performance
- Poor parallel scaling
- I/O bottlenecks

### Debugging Strategies

1. **Profiling**
   ```python
   # Profile slow operations
   import cProfile
   cProfile.run('slow_function()')
   ```

2. **Memory Profiling**
   ```python
   # Memory leak detection
   from pympler import summary, muppy
   summary.print_(muppy.get_objects())
   ```

3. **Query Analysis**
   ```python
   # Analyze query plans
   with DuckDBParquetHandler() as handler:
       handler.execute_sql("EXPLAIN ANALYZE complex_query")
   ```

## Best Practices

### Performance Best Practices

1. **Design for Scale**
   - Plan for growth from the start
   - Use appropriate data structures
   - Consider multi-node solutions

2. **Monitor Continuously**
   - Track key performance metrics
   - Set up alerts for anomalies
   - Regular performance reviews

3. **Optimize Iteratively**
   - Start simple, then optimize
   - Measure before and after changes
   - Focus on real bottlenecks

### Cloud Best Practices

1. **Security First**
   - Use IAM roles instead of access keys
   - Enable encryption in transit and at rest
   - Regular security audits

2. **Cost Management**
   - Monitor usage closely
   - Use lifecycle policies
   - Choose appropriate storage classes

3. **Data Governance**
   - Implement data quality checks
   - Version your datasets
   - Document data lineage

## Next Steps

After completing these intermediate workflows, you'll be ready for:

### Advanced Examples
- **Large Scale Processing**: Enterprise-grade solutions
- **Real-time Analytics**: Streaming data processing
- **Multi-cloud Operations**: Complex cross-provider strategies

### Integration Examples
- **End-to-End Pipelines**: Complete data solutions
- **Production Deployments**: Operational excellence
- **Cross-Domain Workflows**: Full-stack integration

## Dependencies

```bash
# Core dependencies
pip install fsspeckit[datasets] pyarrow

# Cloud-specific dependencies
pip install boto3 azure-storage-blob google-cloud-storage

# Performance monitoring
pip install psutil pympler cProfile

# Advanced analytics
pip install duckdb pandas polars
```

## Getting Help

- **Documentation**: Complete fsspeckit API reference
- **Community**: GitHub discussions and issues
- **Support**: Commercial support options
- **Training**: Workshops and tutorials

Remember: These workflows are designed to be building blocks for your own production solutions. Adapt them to your specific requirements and constraints!