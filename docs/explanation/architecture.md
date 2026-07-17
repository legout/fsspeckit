# Architecture Overview

This page explains how fsspeckit is organized today and why the public API
looks the way it does. It is reference material for understanding the system,
not a tutorial. For step-by-step workflows, start with the
[Local Dataset Lifecycle](../tutorials/local-dataset-lifecycle.md) tutorial.

## Design goals

fsspeckit extends [fsspec](https://filesystem-spec.readthedocs.io/) with four
concerns:

1. **Filesystem access** with path safety, protocol inference, and storage-option
   configuration for cloud and Git providers.
2. **Dataset operations** (write, read, merge, optimize) over Parquet datasets
   using DuckDB or PyArrow as the compute engine.
3. **SQL filter translation** so a single SQL `WHERE` clause can drive
   PyArrow or Polars predicate pushdown.
4. **Shared utilities** (parallel execution, filesystem sync, partition
   parsing, logging, security validation) that have no heavy optional
   dependencies.

The package is split into domain packages so each concern can be understood,
tested, and depended on independently.

## Package layout

```
fsspeckit/
    common/          stdlib + fsspec shared utilities (no heavy deps)
    core/            filesystem factory, merge/maintenance planning, ext registration
    storage_options/ per-provider configuration objects
    datasets/        backend-specific I/O (pyarrow, duckdb), schema, types, polars
    sql/             SQL-to-filter translation
    utils/           backwards-compatibility facade (not for new code)
```

### Dependency flow

```
        fsspeckit.common                         (stdlib + fsspec only)
            ^
            |
   +--------+--------+-------------------+
   |                 |                   |
 fsspeckit.core   fsspeckit.sql    fsspeckit.storage_options
   ^            (common only)       (per-provider config;
   |                                  core uses it)
 fsspeckit.datasets
 (pyarrow, duckdb; core + common)
```

`fsspeckit.sql` depends on `common` only and never imports `datasets`.
`fsspeckit.storage_options` is a sibling package that `core` and `datasets`
import from. `fsspeckit.datasets` builds on `core` and `common`, not on `sql`.

`fsspeckit.utils` sits outside this graph. It re-exports symbols from the
domain packages so old import paths keep working. It is a compatibility facade,
not a layer new code should import from. For the full mapping, see
[Deprecation and Legacy Imports](../reference/legacy-imports.md).

## Domain packages

### `fsspeckit.common`

Stdlib-and-fsspec-only utilities with no hard dependency on `pyarrow`, `numpy`,
or `polars`. Importing `fsspeckit.common` works in a clean environment without
any optional extras installed.

Key modules:

| Module | Responsibility |
|--------|---------------|
| `common.parallel` | `run_parallel` and worker-pool helpers |
| `common.sync` | `sync_dir`, `sync_files`, cross-filesystem copy |
| `common.partitions` | `get_partitions_from_path`, partition glob helpers |
| `common.logging` | `setup_logging` |
| `common.security` | path validation, credential scrubbing, codec validation |
| `common.optional` | lazy optional-dependency loading |

### `fsspeckit.core`

The filesystem factory and backend-neutral planning layer.

```python
from fsspeckit import filesystem

fs = filesystem("s3://bucket/path")  # protocol inference from URI
```

`core.merge` and `core.maintenance` contain the planning functions
(`validate_merge_inputs`, `plan_compaction_groups`, `plan_optimize_groups`)
that both backend handlers delegate to. Planning is testable without backend
writes.

`core.ext` registers fsspec monkey-patch methods that wire concrete dataset
backends onto `AbstractFileSystem`. It is architecturally above `datasets`,
not below it, despite living under the `core/` directory.

### `fsspeckit.storage_options`

Per-provider configuration objects with a common `BaseStorageOptions` base.

```python
from fsspeckit.storage_options import (
    AwsStorageOptions,
    GcsStorageOptions,
    AzureStorageOptions,
    storage_options_from_uri,
)

options = AwsStorageOptions(access_key_id="...", secret_access_key="...")
```

Factory functions (`from_dict`, `from_env`, `storage_options_from_uri`) build
the right subclass from a protocol string or URI. See
[Storage Options](../reference/storage-options.md) for the provider matrix.

### `fsspeckit.datasets`

Backend-specific dataset I/O built on two handlers that share a common
`BaseDatasetHandler` contract:

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection
```

Both expose `write_dataset`, `read_parquet`, `merge`, and maintenance methods
(`compact_parquet_dataset`, `optimize_parquet_dataset`). They delegate merge
and maintenance planning to `core.merge` and `core.maintenance` so that
validation, statistics, and group planning are identical across backends.

Schema and type utilities live here (not in `common`) because they require
`pyarrow` or `polars`:

```python
from fsspeckit.datasets.schema import cast_schema, opt_dtype as opt_dtype_pa
from fsspeckit.datasets.types import to_pyarrow_table, dict_to_dataframe
from fsspeckit.datasets.polars import opt_dtype as opt_dtype_pl
```

For handler selection and result interpretation, see
[Dataset Handlers](../dataset-handlers.md).

### `fsspeckit.sql`

Cross-framework SQL filter translation:

```python
from fsspeckit.sql import sql2pyarrow_filter, sql2polars_filter
```

A single SQL `WHERE` string is converted to a PyArrow `Expression` or a Polars
`Expr`, enabling predicate pushdown without rewriting queries per framework.

## Security architecture

`fsspeckit.common.security` provides defense-in-depth utilities used throughout
the codebase:

- **Path validation** (`validate_path`): prevents path traversal and enforces
  base-directory confinement.
- **Credential scrubbing** (`scrub_credentials`, `scrub_exception`,
  `safe_format_error`): removes secret-like values from logs and error
  messages.
- **Codec validation** (`validate_compression_codec`): restricts compression
  codecs to a safe allowlist.
- **Column validation** (`validate_columns`): guards against column injection in
  SQL-like operations.

## Optional dependencies

fsspeckit uses lazy imports throughout. `pyarrow`, `numpy`, `pandas`, and
`polars` are core dependencies (always installed), while `duckdb`, `sqlglot`,
and cloud client libraries are opt-in extras. `fsspeckit.common` is
architecturally independent: it never imports the data-processing libraries at
module level and works without them loaded. See
[Installation and Optional Extras](../installation.md) for the full extras
matrix.
