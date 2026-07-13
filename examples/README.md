# fsspeckit Examples

This collection demonstrates fsspeckit's capabilities through practical, runnable
examples. Most use local or generated data. Configure provider credentials and
a real resource before performing cloud operations.

## Start here first

Before running these examples, work through the
[Local Dataset Lifecycle tutorial](../docs/tutorials/local-dataset-lifecycle.md).
It is a single, copyable script that covers the canonical workflow: configure a
local filesystem, write a Parquet dataset, read it back, verify the result, and
clean up. The examples below build on that foundation; they do not replace it.

## Install

```bash
pip install "fsspeckit[datasets,sql]"

# Or install the local example environment from this directory.
pip install -r requirements.txt
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
- `dir_file_system/` - local directory operations and an optional S3 demonstration
- `read_folder/` - folder reading and data ingestion
- `storage_options/` - storage configuration; cloud operations need provider extras and credentials
- `maintenance/` - dataset deduplication and maintenance

## Running examples

Run commands from this directory:

```bash
cd examples

# Run a specific local example
python datasets/getting_started/01_duckdb_basics.py

# Run the catalog runner. Cloud examples may require credentials.
python run_examples.py

# Validate catalog paths and module imports without running example main functions.
python run_examples.py --validate

# Run syntax, import, and style checks.
python test_examples.py

# Include subprocess runtime checks with a 30-second timeout.
python test_examples.py --include-runtime --timeout 30

# Run runtime checks with the five-second smoke timeout.
python test_examples.py --include-runtime --smoke-run
```

Most examples include local sample data. The S3 directory demonstration runs
when `s3fs` is installed, which `fsspeckit[aws]` provides. Configure usable AWS
credentials and a real bucket before performing S3 operations.

## Contributing

To add an example, choose the right category, follow the patterns in existing
examples, use canonical imports from the domain packages, and add a test. See
the [Contributing Guide](../docs/contributing.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the
[LICENSE](../LICENSE) file for details.
