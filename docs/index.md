# fsspeckit

`fsspeckit` enhances `fsspec` with advanced utilities for multi-format I/O, cloud storage configuration, and high-performance data processing. It gives you one interface for reading and writing datasets across local and cloud storage, with SQL-based filtering and DuckDB-backed analytics built in.

## Start Here

New to fsspeckit? Work through the [Local Dataset Lifecycle tutorial](tutorials/local-dataset-lifecycle.md). It walks you through installing the library, connecting local and cloud storage, and running your first dataset operations. This is the recommended first path for anyone using fsspeckit as a library.

## Where to Go Next

Once you have the basics, pick a lane:

- **[How-to Guides](how-to/index.md)** - practical recipes for specific tasks
- **[Reference](reference/api-guide.md)** - the complete API tour; see also [Installation](installation.md) for setup and the dependency matrix
- **[Explanation](explanation/index.md)** - the concepts and architecture behind fsspeckit
- **[Migration](migration/dataset-module-refactor.md)** - moving from older module layouts
- **Integrations** - wire up cloud backends in [Configure Cloud Storage](how-to/configure-cloud-storage.md) and query layers in [Use SQL Filters](how-to/use-sql-filters.md); check [Installation](installation.md) for the full extras matrix

## Key Features

- **Multi-Cloud Support**: Unified interface for AWS S3, Azure Blob Storage, and Google Cloud Storage
- **Dataset Operations**: High-performance Parquet processing with DuckDB integration
- **SQL Filter Translation**: Write filters once and run them across PyArrow and Polars
- **Enhanced Filesystem API**: Extended I/O methods with automatic batching and threading
- **Path Safety by Default**: Built-in protection against directory traversal attacks
- **Domain Package Architecture**: Organized APIs for discoverability and type safety

## Contributing

fsspeckit is open source. Read the [Contributing Guide](contributing.md) if you would like to help out, and file bugs or feature requests on [GitHub](https://github.com/legout/fsspeckit).

## Badges

[![GitHub](https://img.shields.io/badge/GitHub-fsspeckit-blue?logo=github)](https://github.com/legout/fsspeckit)
[![PyPI](https://img.shields.io/badge/PyPI-fsspeckit-blue?logo=pypi)](https://pypi.org/project/fsspeckit)
