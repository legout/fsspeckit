# fsspeckit Examples

Welcome to the fsspeckit examples! This collection demonstrates the core capabilities of the fsspeckit ecosystem through practical, runnable examples that work offline without requiring credentials.

## 🎯 Overview

fsspeckit is a modular Python ecosystem for data processing that provides powerful tools for working with datasets, SQL operations, common utilities, and cloud storage options. These examples are designed to help you learn fsspeckit through hands-on practice.

## 📚 Learning Path

### 🟢 Getting Started (Beginner)
*Perfect for newcomers to fsspeckit*

**Location:** `datasets/getting_started/`

1. **DuckDB Basics** - Learn database-style operations with parquet files
2. **PyArrow Basics** - Master in-memory columnar data processing
3. **Simple Merges** - Understand dataset combination techniques
4. **PyArrow Merges** - Advanced merge operations with PyArrow

**Time:** 1-2 hours | **Prerequisites:** Basic Python knowledge

---

### 🟡 Intermediate (Building Skills)
*Develop practical data processing skills*

**Location:** `datasets/workflows/`
1. **Cloud Datasets** - Work with S3, Azure, and Google Cloud Storage (works offline)
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

**Time:** 6-8 hours | **Prerequisites:** Getting Started completion

---

### 🔧 Functional Examples

**Location:** `batch_processing/`
- Large-scale batch data processing patterns

**Location:** `caching/`
- Efficient data caching strategies

**Location:** `dir_file_system/`
- Directory and filesystem operations

**Location:** `read_folder/`
- Folder reading and data ingestion patterns

**Location:** `storage_options/`
- Cloud storage configuration (graceful offline operation)

## 🚀 Quick Start

### Installation

```bash
# Install fsspeckit with core packages
pip install fsspeckit[datasets,sql,common,storage_options]

# Install basic dependencies for examples
pip install pyarrow duckdb pandas polars
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

**🔧 For Application Developers:** Explore `common/` and `batch_processing/`

## 📁 Directory Structure

```
examples/
├── README.md                    # This file - main guide
├── requirements.txt             # Minimal example dependencies
│
├── datasets/                    # Core data processing examples
│   ├── getting_started/         # Beginner tutorials (4 examples)
│   │   ├── 01_duckdb_basics.py
│   │   ├── 02_pyarrow_basics.py
│   │   ├── 03_simple_merges.py
│   │   └── 04_pyarrow_merges.py
│   ├── workflows/               # Intermediate workflows (2 examples)
│   └── schema/                  # Schema management (3 examples)
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
├── batch_processing/            # Batch processing patterns (3 examples)
├── caching/                     # Caching strategies (3 examples)
├── dir_file_system/             # Filesystem operations (3 examples)
├── read_folder/                 # Data ingestion patterns (3 examples)
└── storage_options/             # Cloud storage config (3 examples)
```

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

### Running Examples

```bash
# Run a specific example
python datasets/getting_started/01_duckdb_basics.py

# Run all examples
python run_examples.py

# Test examples
python test_examples.py
```

## 🧪 Testing

### Running Tests

```bash
# Run example tests
python -m pytest test_examples.py -v

# Run specific category tests
python -m pytest test_examples.py::test_datasets -v
```

### Test Data

Examples include built-in sample data generation and use local test files:

```bash
# Use built-in sample data generation
python datasets/getting_started/01_duckdb_basics.py

# Examples work offline with included test data
ls local:/          # Sample parquet files
ls temp_multicloud_data/  # Multi-cloud test data
```

## 📈 Performance Benchmarks

### Expected Performance

| Operation | Dataset Size | Time | Memory | Notes |
|-----------|--------------|------|--------|-------|
| Basic SQL Query | 1M rows | <1s | <100MB | DuckDB optimized |
| Parallel Processing | 10M rows | 5-10s | 1-2GB | 4-core parallel |
| Schema Optimization | 100M rows | 2-5min | 8-16GB | With optimization |

## 🔍 Troubleshooting

### Common Issues

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

**Memory Errors:**
```python
# Reduce batch size or use streaming
handler = DuckDBParquetHandler(memory_limit="2GB")
```

### Getting Help

- **Documentation**: [Complete API Reference](https://fsspeckit.readthedocs.io/)
- **Examples**: This repository and inline code comments
- **Community**: [GitHub Discussions](https://github.com/your-org/fsspeckit/discussions)
- **Issues**: [GitHub Issues](https://github.com/your-org/fsspeckit/issues)

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](../CONTRIBUTING.md) for details.

### Adding Examples

1. **Choose the right category** (getting_started, workflows, etc.)
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
- **Community Contributors** - For feedback, suggestions, and contributions

---

## 🎉 Start Your Journey

Ready to master fsspeckit? Choose your starting point:

- 🟢 **New to fsspeckit?** Start with `datasets/getting_started/01_duckdb_basics.py`
- 🔄 **DuckDB Merge Operations?** See `datasets/getting_started/03_simple_merges.py`
- 🚀 **Have experience?** Jump to `datasets/workflows/performance_optimization.py`
- 🏭 **Production ready?** Explore `common/parallel_processing.py`
- 📚 **Want to learn everything?** Follow the complete learning path above

Happy coding! 🚀