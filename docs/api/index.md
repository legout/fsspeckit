# `fsspeckit` API Reference

Welcome to the `fsspeckit` API reference documentation. This section provides detailed information on the various modules, classes, and functions available in the library.

> **Package Structure**: fsspeckit is organized into domain-specific packages for better discoverability. See [Architecture](../explanation/architecture.md) for details.

## Domain Packages (Primary API)

### Dataset Operations
*   [`fsspeckit.datasets`](fsspeckit.datasets.md) - Dataset-level operations (DuckDB & PyArrow helpers)
*   [`fsspeckit.datasets.pyarrow.memory`](fsspeckit.datasets.pyarrow.memory.md) - Enhanced PyArrow memory monitoring

### SQL Utilities
*   [`fsspeckit.sql.filters`](fsspeckit.sql.filters.md) - SQL-to-filter translation helpers

### Common Utilities
*   [`fsspeckit.common`](fsspeckit.common.md) - Cross-cutting utilities (logging, parallelism, type conversion)

## Core Infrastructure

### Core Modules
*   [`fsspeckit.core.base`](fsspeckit.core.base.md) - Base classes and interfaces
*   [`fsspeckit.core.ext`](fsspeckit.core.ext.md) - Extended filesystem methods
*   [`fsspeckit.core.filesystem`](fsspeckit.core.filesystem.md) - Filesystem factory functions
*   [`fsspeckit.core.maintenance`](fsspeckit.core.maintenance.md) - Dataset maintenance utilities
*   [`fsspeckit.core.merge`](fsspeckit.core.merge.md) - Dataset merging operations

### Storage Options
*   [`fsspeckit.storage_options.base`](fsspeckit.storage_options.base.md) - Base storage options
*   [`fsspeckit.storage_options.cloud`](fsspeckit.storage_options.cloud.md) - Cloud storage configurations
*   [`fsspeckit.storage_options.core`](fsspeckit.storage_options.core.md) - Core storage utilities
*   [`fsspeckit.storage_options.git`](fsspeckit.storage_options.git.md) - Git-based storage options

## Backwards Compatibility (`fsspeckit.utils`)

The `fsspeckit.utils` module provides a backwards-compatible façade that re-exports selected helpers from the domain packages. **New code should import directly from domain packages** for better discoverability.

*   [`fsspeckit.utils.datetime`](fsspeckit.utils.datetime.md) - Date and time utilities
*   [`fsspeckit.utils.logging`](fsspeckit.utils.logging.md) - Logging configuration and utilities
*   [`fsspeckit.utils.misc`](fsspeckit.utils.misc.md) - Miscellaneous utility functions
*   [`fsspeckit.utils.polars`](fsspeckit.utils.polars.md) - Polars DataFrame utilities
*   [`fsspeckit.utils.pyarrow`](fsspeckit.utils.pyarrow.md) - PyArrow utilities and integrations
*   [`fsspeckit.utils.sql`](fsspeckit.utils.sql.md) - SQL query and filter utilities
*   [`fsspeckit.utils.types`](fsspeckit.utils.types.md) - Type definitions and utilities

> **Migration Tip**: For new code, prefer importing directly from domain packages:
> - `from fsspeckit.datasets import DuckDBParquetHandler` instead of `from fsspeckit.utils import DuckDBParquetHandler`
> - `from fsspeckit.common.logging import setup_logging` instead of `from fsspeckit.utils import setup_logging`
