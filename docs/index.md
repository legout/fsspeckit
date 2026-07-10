# fsspeckit

`fsspeckit` enhances `fsspec` with utilities for multi-format I/O, storage
configuration, and dataset processing. It gives you one interface for reading
and writing datasets across local and cloud storage, with SQL-based filtering
and DuckDB-backed analytics built in.

## Start here

New to fsspeckit? Work through the
[Local Dataset Lifecycle tutorial](tutorials/local-dataset-lifecycle.md).
It is a single, copyable, offline script: configure a local filesystem, write a
Parquet dataset, read it back, verify the result, and clean up a named sandbox.
This is the recommended first path for anyone using fsspeckit as a library.

## Where to go next

Once you have the basics, pick a lane:

- **[How-to Guides](how-to/index.md)** - practical recipes for specific tasks
- **[Reference](reference/api-guide.md)** - the complete API tour; see also [Installation](installation.md) for setup and the dependency matrix
- **[Explanation](explanation/index.md)** - the concepts and architecture behind fsspeckit
- **[Migration](migration/dataset-module-refactor.md)** - moving from older module layouts
- **Integrations** - wire up cloud backends in [Configure Cloud Storage](how-to/configure-cloud-storage.md) and query layers in [Use SQL Filters](how-to/use-sql-filters.md); check [Installation](installation.md) for the full extras matrix

## Key features

- **Multi-cloud support**: unified interface for AWS S3, Azure Blob Storage, and Google Cloud Storage
- **Dataset operations**: Parquet processing with DuckDB and PyArrow backends
- **SQL filter translation**: write filters once and run them across PyArrow and Polars
- **Enhanced filesystem API**: extended I/O methods with automatic batching and threading
- **Path safety by default**: built-in protection against directory traversal attacks
- **Domain package architecture**: organized APIs for discoverability and type safety

## Contributing

fsspeckit is open source. Read the [Contributing Guide](contributing.md) if you
would like to help out, and file bugs or feature requests on
[GitHub](https://github.com/legout/fsspeckit).

## Badges

[![GitHub](https://img.shields.io/badge/GitHub-fsspeckit-blue?logo=github)](https://github.com/legout/fsspeckit)
[![PyPI](https://img.shields.io/badge/PyPI-fsspeckit-blue?logo=pypi)](https://pypi.org/project/fsspeckit)
