# `fsspeckit` API Reference

Welcome to the generated `fsspeckit` API reference. This section renders
docstrings and signatures directly from source for each approved public module.
For guidance on which import to choose, backend and configuration decisions,
and result interpretation, see the curated
[API Guide](../reference/api-guide.md) and the
[Public API Inventory](../reference/public-api-inventory.md).

## Root / core

- [fsspeckit.core](fsspeckit.core.md) - Filesystem factory functions and classes
- [fsspeckit.core.merge](fsspeckit.core.merge.md) - Merge strategy, planning, and execution
- [fsspeckit.core.maintenance](fsspeckit.core.maintenance.md) - Compaction, optimization, and deduplication planning

## Datasets / backends

- [fsspeckit.datasets](fsspeckit.datasets.md) - Dataset exceptions, path helpers, and PyArrow/Polars utilities
- [fsspeckit.datasets.duckdb](fsspeckit.datasets.duckdb.md) - DuckDB dataset I/O and connection management
- [fsspeckit.datasets.pyarrow](fsspeckit.datasets.pyarrow.md) - PyArrow dataset I/O, memory monitoring, and adaptive key tracking

## Storage options

- [fsspeckit.storage_options](fsspeckit.storage_options.md) - Cloud, local, and git storage configuration

## Common utilities

- [fsspeckit.common](fsspeckit.common.md) - Cross-cutting utilities (datetime, logging, partitions, security, parallelism, sync)

## SQL

- [fsspeckit.sql.filters](fsspeckit.sql.filters.md) - SQL-to-filter expression translation
