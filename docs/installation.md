# Installation

`fsspeckit` installs with `pip` (or any PEP 517 installer). Core filesystem
operations work with no extra dependencies; cloud providers and data-processing
backends are opt-in extras.

## Prerequisites

- Python 3.11 or higher.

## Install

```bash
pip install fsspeckit
```

Upgrade an existing install:

```bash
pip install --upgrade fsspeckit
```

## Optional extras

fsspeckit ships optional dependency groups. Install only what you need; imports
always succeed and the relevant extra is required only when you actually call
the feature.

```bash
# One extra
pip install "fsspeckit[aws]"

# Several extras
pip install "fsspeckit[aws,gcp,datasets]"
```

### Integration and extras matrix

This is the single authoritative mapping of each extra to the workflow or
provider it enables. Every reference page that depends on an extra links back
here; none duplicates this table.

| Extra | Enables | Typical import | Related docs |
|-------|---------|----------------|--------------|
| `aws` | S3 filesystem via `s3fs`/`boto3`; `AwsStorageOptions` | `from fsspeckit import AwsStorageOptions` | [Storage Options](reference/storage-options.md), [Configure Cloud Storage](how-to/configure-cloud-storage.md) |
| `gcp` | Google Cloud Storage via `gcsfs`; `GcsStorageOptions` | `from fsspeckit import GcsStorageOptions` | [Storage Options](reference/storage-options.md), [Configure Cloud Storage](how-to/configure-cloud-storage.md) |
| `azure` | Azure Blob/Data Lake via `adlfs`; `AzureStorageOptions` | `from fsspeckit import AzureStorageOptions` | [Storage Options](reference/storage-options.md), [Configure Cloud Storage](how-to/configure-cloud-storage.md) |
| `polars` | Compatibility install group for Polars type optimization; Polars is already a core dependency | `from fsspeckit.datasets import opt_dtype_pl` | [API Guide](reference/api-guide.md) |
| `datasets` | DuckDB + PyArrow dataset handlers, schema utilities, full dataset lifecycle | `from fsspeckit.datasets.duckdb import DuckDBDatasetIO` | [Dataset Handlers](dataset-handlers.md), [API Guide](reference/api-guide.md) |
| `monitoring` | System memory monitoring (`psutil`) for memory-aware merge and compaction | `from fsspeckit.datasets.pyarrow import MemoryMonitor` | [Adaptive Key Tracking](reference/adaptive-key-tracking.md) |
| `sql` | SQL-to-filter translation (`sqlglot`, `duckdb`) | `from fsspeckit.sql import sql2pyarrow_filter` | [API Guide](reference/api-guide.md), [Use SQL Filters](how-to/use-sql-filters.md) |

Notes on the matrix:

- `datasets` is the broadest data-processing extra. It adds `duckdb`,
  `sqlglot`, and `psutil`; `pyarrow`, `numpy`, `pandas`, `polars`, and `orjson`
  are core dependencies already installed with fsspeckit. Install `[datasets]`
  when you want the complete dataset read/write/merge stack, including the
  DuckDB handler.
- `polars` is retained as a compatibility install group for Polars type
  optimization. It adds no packages because Polars is already a core dependency.
- `monitoring` adds `psutil` for system-memory-aware streaming merge and
  compaction. It is separate from `datasets` so memory-constrained workflows can
  opt in without the full dataset stack, although `datasets` already includes it.
- `sql` is the minimal extra for SQL filter translation. It overlaps with
  `datasets` (both bring `duckdb` and `sqlglot`); install `sql` alone when you
  only need filter translation against your own PyArrow/Polars tables.
- Core (`filesystem`, `get_filesystem`, storage-options classes, `common`
  utilities) has no required extra. Cloud provider extras are only needed when
  you connect to that provider.

### Dependency management tools

`uv` is recommended for fast installs:

```bash
uv pip install fsspeckit
uv pip install "fsspeckit[aws,datasets]"
```

`pixi`:

```bash
pixi add fsspeckit
```

## Troubleshooting

- **Python version**: require 3.11+. Check with `python --version`.
- **Virtual environments**: use `venv`, `conda`, `uv`, or `pixi` to avoid
  system-package conflicts.
- **Missing optional dependency**: if you call a feature whose extra is not
  installed, fsspeckit raises an `ImportError` naming the required package. Match
  the error to a row in the matrix above.

For further help, see the [GitHub repository](https://github.com/legout/fsspeckit)
or open an issue.
