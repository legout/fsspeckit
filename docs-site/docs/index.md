# fsspeckit

`fsspeckit` enhances `fsspec` with advanced utilities for multi-format I/O, cloud storage configuration, and high-performance data processing.

## Quick Start

**New to fsspeckit?** Start with our [Getting Started](tutorials/getting-started.md) tutorial for a complete walkthrough.

**Looking for specific tasks?** Browse our [How-to Guides](how-to/index.md) for practical recipes:

- [Configure Cloud Storage](how-to/configure-cloud-storage.md) - AWS, GCP, Azure setup
- [Work with Filesystems](how-to/work-with-filesystems.md) - Local and remote operations  
- [Read and Write Datasets](how-to/read-and-write-datasets.md) - JSON, CSV, Parquet operations
- [Use SQL Filters](how-to/use-sql-filters.md) - Cross-framework filtering
- [Sync and Manage Files](how-to/sync-and-manage-files.md) - File synchronization
- [Optimize Performance](how-to/optimize-performance.md) - Caching and parallel processing
- [Multi-Key Usage Examples](how-to/multi-key-examples.md) - Composite key operations with 10-100x performance gains
- [Multi-Key Performance Guide](how-to/multi-key-performance.md) - Performance characteristics and optimization

## Key Features

- **Multi-Cloud Support**: Unified interface for AWS S3, Azure Blob Storage, Google Cloud Storage
- **Advanced Dataset Operations**: High-performance Parquet processing with DuckDB integration
- **SQL Filter Translation**: Write filters once, use across PyArrow and Polars
- **Enhanced Filesystem API**: Extended I/O methods with automatic batching and threading
- **Path Safety by Default**: Built-in protection against directory traversal attacks
- **Domain Package Architecture**: Organized APIs for better discoverability and type safety

## Documentation

### Learning Paths

**Beginners**: Start with [Getting Started](tutorials/getting-started.md) for hands-on learning

**Practical Users**: Jump to [How-to Guides](how-to/index.md) for specific task solutions

**Developers**: Reference [API Guide](reference/api-guide.md) for capability overview

**Architects**: Understand design decisions in [Architecture & Concepts](explanation/index.md)

### Reference Materials

- **[Installation](installation.md)** - Setup and dependency management
- **[API Reference](reference/api-guide.md)** - Complete API documentation
- **[Multi-Key API Reference](reference/multi-key-api.md)** - PyArrow vectorized multi-key functions
- **[Utils Reference](reference/utils.md)** - Backwards compatibility guide

## Architecture Overview

fsspeckit is organized into domain-specific packages:

- **`fsspeckit.core`** - Filesystem creation and extended I/O
- **`fsspeckit.storage_options`** - Cloud provider configuration
- **`fsspeckit.datasets`** - Large-scale dataset operations
- **`fsspeckit.sql`** - Cross-framework SQL translation
- **`fsspeckit.common`** - Shared utilities and helpers
- **`fsspeckit.utils`** - Backwards compatibility façade

## Getting Help

**Contributing**: See our [Contributing Guide](contributing.md) to help improve fsspeckit

**Issues**: Report bugs and request features on [GitHub](https://github.com/legout/fsspeckit)

**Community**: Join discussions and connect with other users

## Badges

[![GitHub](https://img.shields.io/badge/GitHub-fsspeckit-blue?logo=github)](https://github.com/legout/fsspeckit)
[![PyPI](https://img.shields.io/badge/PyPI-fsspeckit-blue?logo=pypi)](https://pypi.org/project/fsspeckit)
