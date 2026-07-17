# How-to Guides

Use these guides when you know what you want to achieve and need a concrete
recipe. Each guide uses canonical imports and links to the
[Reference](../reference/index.md) for exact signatures and detailed behavior.

## Filesystem and storage

- [Work with Filesystems](work-with-filesystems.md) - create filesystems, path
  confinement, caching, and extended I/O methods.
- [Configure Cloud Storage](configure-cloud-storage.md) - set up AWS, GCP, and
  Azure providers from environment variables, structured classes, or URIs.

## Datasets

- [Read and Write Datasets](read-and-write-datasets.md) - read and write JSON,
  CSV, and Parquet files, plus dataset handlers for partitioned parquet.
- [Merge Datasets](merge-datasets.md) - incremental `insert`, `update`, and
  `upsert` merges across the DuckDB and PyArrow backends.
- [Maintain Parquet Datasets](maintain-parquet-datasets.md) - compaction,
  deduplication, repartitioning, and optimization with typed plans and
  results.
- [Merge Operations Examples](merge-operations-examples.md) - end-to-end merge
  scenarios and result interpretation.

## Performance and memory

- [Optimize Performance](optimize-performance.md) - caching, parallel
  processing, and type optimization.
- [Memory-Constrained Environments](memory-constrained-environments.md) -
  memory monitoring, chunked processing, and streaming merge controls.
- [Adaptive Key Tracking](adaptive-key-tracking.md) - tiered, memory-bounded
  key tracking for deduplication and merge.

## Multi-key operations

- [Multi-Key Examples](multi-key-examples.md) - composite-key merge and
  deduplication recipes.
- [Multi-Key Performance](multi-key-performance.md) - how composite keys
  behave and when to use them.

## Queries and file management

- [Use SQL Filters](use-sql-filters.md) - translate SQL `WHERE` clauses into
  PyArrow and Polars filter expressions.
- [Sync and Manage Files](sync-and-manage-files.md) - synchronize files and
  directories across filesystems.
