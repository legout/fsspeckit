# fsspeckit Examples

This collection demonstrates fsspeckit's capabilities through practical, runnable
examples. Every example works offline without cloud credentials.

## Start here first

Before running these examples, work through the
[Local Dataset Lifecycle tutorial](../docs/tutorials/local-dataset-lifecycle.md).
It is a single, copyable script that covers the canonical workflow: configure a
local filesystem, write a Parquet dataset, read it back, verify the result, and
clean up. The examples below build on that foundation; they do not replace it.

## Install

```bash
pip install "fsspeckit[datasets]"
```

For the full extras matrix, see
[Installation and optional extras](../docs/installation.md).

## Example categories

### Getting started

Location: `datasets/getting_started/`

Hands-on introductions to dataset operations. Start here after the tutorial.

1. `01_duckdb_basics.py` - DuckDB dataset operations with Parquet
2. `02_pyarrow_basics.py` - in-memory columnar processing with PyArrow
3. `03_simple_merges.py` - dataset combination techniques
4. `04_pyarrow_merges.py` - merge operations with the PyArrow backend
5. `05_duckdb_upserts.py` - upsert workflows with DuckDB
6. `06_pyarrow_maintenance.py` - dataset maintenance with PyArrow

### Workflows

Location: `datasets/workflows/`

- `performance_optimization.py` - memory management and parallel processing
- `cloud_datasets.py` - working with cloud backends (runs offline with local fallbacks)

### Schema

Location: `datasets/schema/`

- `schema_basics.py` - fundamental schema operations
- `schema_unification.py` - combining datasets with different schemas
- `type_optimization.py` - memory and performance optimization through typing

### SQL filters

Location: `sql/`

- `sql_filter_basic.py` - convert SQL to PyArrow and Polars filters
- `sql_filter_advanced.py` - complex queries and performance benchmarking
- `cross_platform_filters.py` - backend-agnostic SQL filter operations

### Common utilities

Location: `common/`

- `logging_setup.py` - structured logging configuration
- `parallel_processing.py` - multi-core data processing patterns
- `type_conversion.py` - format conversion between data libraries

### Other categories

- `batch_processing/` - large-scale batch data processing patterns
- `caching/` - data caching strategies
- `dir_file_system/` - directory and filesystem operations
- `read_folder/` - folder reading and data ingestion
- `storage_options/` - storage configuration (graceful offline operation)
- `maintenance/` - dataset deduplication and maintenance

## Running examples

```bash
# Run a specific example
python datasets/getting_started/01_duckdb_basics.py

# Run all examples
python run_examples.py

# Run the example test suite
python -m pytest test_examples.py -v
```

Examples include built-in sample data generation and work offline with local
test files.

## Contributing

To add an example, choose the right category, follow the patterns in existing
examples, use canonical imports from the domain packages, and add a test. See
the [Contributing Guide](../docs/contributing.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the
[LICENSE](../LICENSE) file for details.
