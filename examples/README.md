# fsspeckit Examples

Welcome to the fsspeckit examples repository! This comprehensive collection demonstrates the full power and capabilities of the fsspeckit ecosystem through practical, real-world examples.

## 🎯 Overview

fsspeckit is a modular Python ecosystem for data processing that provides powerful tools for working with datasets, SQL operations, common utilities, and cloud storage options. These examples are designed to help you master fsspeckit through hands-on learning, from basic concepts to enterprise-grade production deployments.

## 📚 Learning Path

The examples are organized into a progressive learning path that takes you from beginner to expert:

### 🟢 Getting Started (Beginner)
*Perfect for newcomers to fsspeckit*

**Location:** `datasets/getting_started/`

1. **DuckDB Basics** - Learn database-style operations with parquet files
2. **PyArrow Basics** - Master in-memory columnar data processing
3. **Simple Merges** - Understand dataset combination techniques

**Time:** 1-2 hours | **Prerequisites:** Basic Python knowledge

---

### 🟡 Intermediate (Building Skills)
*Develop practical data processing skills*

**Location:** `datasets/workflows/`

1. **Cloud Datasets** - Work with S3, Azure, and Google Cloud Storage
2. **Performance Optimization** - Learn memory management and parallel processing

**Location:** `sql/`
1. **SQL Filter Basics** - Convert SQL to PyArrow/Polars filters
2. **Advanced SQL** - Complex queries and performance benchmarking
3. **Cross-Platform Filters** - Backend-agnostic SQL operations

**Location:** `datasets/schema/`
1. **Schema Basics** - Fundamental schema operations
2. **Schema Unification** - Combine datasets with different schemas
3. **Type Optimization** - Advanced memory and performance optimization

**Location:** `common/`
1. **Logging Setup** - Comprehensive structured logging
2. **Parallel Processing** - Multi-core data processing patterns
3. **Type Conversion** - Format conversion between data libraries

**Time:** 8-12 hours | **Prerequisites:** Getting Started completion

---

### 🔴 Advanced (Expert Skills)
*Master enterprise-grade data processing*

**Location:** `datasets/advanced/`

1. **Real-Time Analytics** - Stream processing and sliding window analytics
2. **Large-Scale Processing** - Distributed processing for big data
3. **Multi-Cloud Operations** - Cross-cloud replication and disaster recovery

**Time:** 10-15 hours | **Prerequisites:** All intermediate examples

---

### 🚀 Cross-Domain Integration (Production Ready)
*Build complete, enterprise-grade solutions*

**Location:** `cross_domain/`

1. **End-to-End Pipeline** - Complete data pipeline orchestration
2. **Production Patterns** - Deployment, monitoring, and operational excellence

**Time:** 8-12 hours | **Prerequisites:** All advanced examples

## 🏗️ Package Integration

### Core Packages

| Package | Purpose | Key Features | Examples |
|---------|---------|--------------|----------|
| **datasets** | Core data processing | DuckDB integration, PyArrow optimization, schema management | All examples |
| **sql** | SQL operations | Query optimization, cross-platform filters, validation | sql/ directory |
| **common** | Shared utilities | Parallel processing, logging, type conversion | common/ directory |
| **storage_options** | Cloud storage | Multi-cloud support, authentication, cost optimization | workflows/cloud_datasets.py |

### Integration Patterns

```python
# Pattern 1: Data Processing + SQL
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.sql import optimize_sql_query

# Optimize queries for better performance
query = optimize_sql_query("SELECT * FROM data WHERE category = 'A'")
with DuckDBParquetHandler() as handler:
    result = handler.execute_sql(query)
```

```python
# Pattern 2: Cloud + Performance
from fsspeckit.datasets import optimize_parquet_dataset_pyarrow
from fsspeckit.storage_options import AwsStorageOptions

# Optimize cloud data for fast queries
optimize_parquet_dataset_pyarrow(
    "s3://bucket/data/",
    zorder_columns=["date", "region"],
    target_file_size_mb=128
)
```

## 🚀 Quick Start

### Installation

```bash
# Install core packages
pip install fsspeckit[datasets,sql,common,storage_options]

# Install additional dependencies
pip install pyarrow duckdb pandas polars

# For cloud operations
pip install boto3 azure-storage-blob google-cloud-storage

# For production monitoring
pip install prometheus-client structlog
```

### Your First Example

```python
# Start with DuckDB basics
from fsspeckit.datasets import DuckDBParquetHandler

# Process data with SQL
with DuckDBParquetHandler() as handler:
    # Write data
    handler.write_parquet(data, "output.parquet")

    # Register and query
    handler.register_dataset("my_data", "data.parquet")
    result = handler.execute_sql("SELECT * FROM my_data WHERE value > 100")
```

### Choose Your Path

**🎯 For Data Analysts:** Start with `datasets/getting_started/01_duckdb_basics.py`
- 🔄 **For Merge Operations:** Try `datasets/getting_started/03_simple_merges.py`

**🚀 For Data Engineers:** Begin with `datasets/workflows/performance_optimization.py`

**☁️ For Cloud Architects:** Jump to `datasets/workflows/cloud_datasets.py`

**🏭 For Production Teams:** Explore `cross_domain/production_patterns.py`

## 📊 Example Categories

### 📁 Directory Structure

```
examples/
├── README.md                    # This file - main guide
├── requirements.txt             # All example dependencies
│
├── datasets/                    # Core data processing examples
│   ├── getting_started/         # Beginner tutorials (3 examples)
│   │   ├── 01_duckdb_basics.py
│   │   ├── 02_pyarrow_basics.py
│   │   ├── 03_simple_merges.py
│   │   └── 04_pyarrow_merges.py
│   ├── workflows/               # Intermediate workflows (2 examples)
│   ├── schema/                  # Schema management (3 examples)
│   └── advanced/                # Advanced processing (3 examples)
│
├── sql/                         # SQL operations (3 examples)
│   ├── sql_filter_basic.py
│   ├── sql_filter_advanced.py
│   └── cross_platform_filters.py
│
├── common/                      # Common utilities (3 examples)
│   ├── logging_setup.py
│   ├── parallel_processing.py
│   └── type_conversion.py
│
└── cross_domain/                # Production integrations (2 examples)
    ├── end_to_end_pipeline.py
    ├── production_patterns.py
    └── README.md
```

## 🎯 Learning by Use Case

### **Data Analysis & BI**
- Start with: `datasets/getting_started/`
- Focus on: DuckDB operations, SQL queries, data filtering
- Goal: Interactive data exploration and reporting

### **Big Data Processing**
- Start with: `datasets/advanced/large_scale_processing.py`
- Focus on: Parallel processing, memory management, optimization
- Goal: Process terabyte-scale datasets efficiently

### **Cloud Data Engineering**
- Start with: `datasets/workflows/cloud_datasets.py`
- Focus on: Multi-cloud operations, cost optimization, security
- Goal: Build cloud-native data pipelines

### **Real-Time Analytics**
- Start with: `datasets/advanced/real_time_analytics.py`
- Focus on: Stream processing, windowing, monitoring
- Goal: Build real-time dashboards and alerting

### **Production Deployment**
- Start with: `cross_domain/production_patterns.py`
- Focus on: Docker, Kubernetes, monitoring, security
- Goal: Deploy reliable, scalable production systems

## 💡 Best Practices

### 🔧 Development Best Practices

1. **Start Simple, Scale Up**
   ```python
   # Begin with basic operations
   result = handler.execute_sql("SELECT * FROM data LIMIT 1000")

   # Then optimize and scale
   optimized_query = optimize_sql_query("SELECT * FROM data WHERE category = 'A'")
   ```

2. **Use Context Managers**
   ```python
   # Always use context managers for resource management
   with DuckDBParquetHandler() as handler:
       result = handler.execute_sql(query)
   # Resources automatically cleaned up
   ```

3. **Handle Large Data Efficiently**
   ```python
   # Process in chunks for memory efficiency
   for batch in data_frame.take_batches(batch_size=10000):
       process_batch(batch)
   ```

### 🚀 Performance Best Practices

1. **Optimize Data Layout**
   ```python
   # Use Z-ordering for better query performance
   optimize_parquet_dataset_pyarrow(
       "data/",
       zorder_columns=["date", "region", "category"]
   )
   ```

2. **Choose the Right Tool**
   ```python
   # Use DuckDB for complex SQL
   # Use PyArrow for simple transformations
   # Use polars for high-performance operations
   ```

3. **Parallel Processing**
   ```python
   # Leverage multiple cores for heavy operations
   from fsspeckit.common import run_parallel

   results = run_parallel(process_data, data_chunks, max_workers=4)
   ```

### 🏭 Production Best Practices

1. **Configuration Management**
   ```python
   # Use environment-specific configurations
   config = load_config(environment="production")
   ```

2. **Comprehensive Logging**
   ```python
   # Use structured logging for debugging
   logger = setup_structured_logging("my_app", include_timestamp=True)
   logger.info("Processing complete", extra={"record_count": len(result)})
   ```

3. **Error Handling**
   ```python
   # Implement retry logic for resilience
   from fsspeckit.common import retry_with_backoff

   @retry_with_backoff(max_retries=3)
   def process_data(data):
       # Your processing logic
       pass
   ```

## 🛠️ Environment Setup

### Development Environment

```bash
# Clone the repository
git clone https://github.com/your-org/fsspeckit.git
cd fsspeckit/examples

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### IDE Configuration

**VSCode:**
```json
{
    "python.defaultInterpreterPath": "./venv/bin/python",
    "python.linting.enabled": true,
    "python.formatting.provider": "black"
}
```

**PyCharm:**
- Set Python interpreter to `./venv/bin/python`
- Enable code completion and type checking
- Configure run configurations for examples

## 🧪 Testing

### Running Tests

```bash
# Run all example tests
python -m pytest tests/ -v

# Run specific category tests
python -m pytest tests/test_datasets.py -v
python -m pytest tests/test_sql.py -v

# Run with coverage
python -m pytest tests/ --cov=fsspeckit --cov-report=html
```

### Test Data

Many examples include sample data generation. For examples requiring external data:

```bash
# Download test datasets (optional)
python scripts/download_test_data.py

# Or use built-in sample data generation
python datasets/getting_started/01_duckdb_basics.py --generate-sample
```

## 📈 Performance Benchmarks

### Expected Performance

| Operation | Dataset Size | Time | Memory | Notes |
|-----------|--------------|------|--------|-------|
| Basic SQL Query | 1M rows | <1s | <100MB | DuckDB optimized |
| Parallel Processing | 10M rows | 5-10s | 1-2GB | 4-core parallel |
| Cloud Upload | 100MB | 10-30s | 50MB | Varies by provider |
| Large-Scale Processing | 100M rows | 2-5min | 8-16GB | With optimization |

### Benchmarking Examples

```python
# Benchmark your own operations
import time
import psutil

def benchmark_operation(operation, *args):
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss

    result = operation(*args)

    end_time = time.time()
    end_memory = psutil.Process().memory_info().rss

    return {
        "result": result,
        "duration": end_time - start_time,
        "memory_delta": end_memory - start_memory
    }
```

## 🔍 Troubleshooting

### Common Issues

**Memory Errors:**
```python
# Reduce batch size or use streaming
handler = DuckDBParquetHandler(memory_limit="2GB")
```

**Import Errors:**
```bash
# Install missing dependencies
pip install fsspeckit[datasets,sql,common,storage_options]
```

**Performance Issues:**
```python
# Enable query optimization
optimized_query = optimize_sql_query(query)
```

### Getting Help

- **Documentation**: [Complete API Reference](https://fsspeckit.readthedocs.io/)
- **Examples**: This repository and inline code comments
- **Community**: [GitHub Discussions](https://github.com/your-org/fsspeckit/discussions)
- **Issues**: [GitHub Issues](https://github.com/your-org/fsspeckit/issues)
- **Support**: Commercial support available for enterprise deployments

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Adding Examples

1. **Choose the right category** (getting_started, workflows, advanced, etc.)
2. **Follow the template** in existing examples
3. **Include comprehensive documentation**
4. **Add tests** for your example
5. **Update this README** if adding new categories

### Example Template

```python
"""
Example Title and Description

This example demonstrates...
- Key concept 1
- Key concept 2
- Key concept 3

Prerequisites: List prerequisites
Time: Estimated completion time
Complexity: Beginner/Intermediate/Advanced
"""

# Imports with comments explaining each
import pyarrow as pa
from fsspeckit.datasets import DuckDBParquetHandler

def main():
    """Main example function."""
    print("🚀 Starting example...")

    # Your example code here
    # Include comments explaining each step

    print("✅ Example completed!")

if __name__ == "__main__":
    main()
```

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../LICENSE) file for details.

## 🙏 Acknowledgments

- **PyArrow Team** - For the amazing columnar data processing library
- **DuckDB Team** - For the fast analytical database
- **Cloud Providers** - For robust storage and compute services
- **Community Contributors** - For feedback, suggestions, and contributions

---

## 🎉 Start Your Journey

Ready to master fsspeckit? Choose your starting point:

- 🟢 **New to fsspeckit?** Start with `datasets/getting_started/01_duckdb_basics.py`
- 🔄 **DuckDB Merge Operations?** See `duckdb/duckdb_merge_example.py` for comprehensive merge strategies
- 🚀 **Have experience?** Jump to `datasets/workflows/performance_optimization.py`
- 🏭 **Production ready?** Explore `cross_domain/production_patterns.py`
- 📚 **Want to learn everything?** Follow the complete learning path above

Happy coding! 🚀