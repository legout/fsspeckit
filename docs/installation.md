# Installation

`fsspeckit` can be installed using `pip`, the Python package installer.

## Prerequisites

- Python 3.11 or higher is required.

## Install with pip

To install `fsspeckit` using `pip`, run the following command:

```bash
pip install fsspeckit
```

### Optional Dependencies

`fsspeckit` uses lazy imports for optional dependencies, allowing you to install only what you need:

#### Cloud Provider Support

```bash
# AWS S3 support
pip install "fsspeckit[aws]"

# Google Cloud Storage support
pip install "fsspeckit[gcp]"

# Azure Storage support
pip install "fsspeckit[azure]"

# All cloud providers
pip install "fsspeckit[aws,gcp,azure]"
```

#### Data Processing Libraries

The following libraries are optional and loaded only when needed:

```bash
# PyArrow for dataset operations
pip install pyarrow

# Polars for high-performance DataFrames
pip install polars

# DuckDB for SQL analytics
pip install duckdb

# SQLglot for SQL parsing
pip install sqlglot

# Install all data processing libraries
pip install pyarrow polars duckdb sqlglot
```

**Note:** You can use `fsspeckit` core functionality without any of these dependencies. They are only required when you use specific features:

- **PyArrow**: Required for `fsspeckit.datasets.pyarrow` operations
- **Polars**: Required for Polars-specific utilities in `fsspeckit.common.polars`
- **DuckDB**: Required for `fsspeckit.datasets.DuckDBParquetHandler`
- **SQLglot**: Required for SQL filter translation in `fsspeckit.sql.filters`

If you attempt to use a feature requiring an optional dependency that isn't installed, you'll receive a clear error message indicating which package to install.

### Upgrading

To upgrade `fsspeckit` to the latest version, use:

```bash
pip install --upgrade fsspeckit
```

## Environment Management with `uv` and `pixi`

For robust dependency management and faster installations, we recommend using `uv` or `pixi`.

### Using `uv`

`uv` is a fast Python package installer and resolver. To install `fsspeckit` with `uv`:

```bash
uv pip install fsspeckit
```

### Using `pixi`

`pixi` is a modern package manager for Python and other languages. To add `fsspeckit` to your `pixi` project:

```bash
pixi add fsspeckit
```

## Troubleshooting

If you encounter any issues during installation, consider the following:

- **Python Version**: Ensure you are using Python 3.11 or higher. You can check your Python version with `python --version`.
- **Virtual Environments**: It is highly recommended to use a virtual environment (e.g., `venv`, `conda`, `uv`, `pixi`) to avoid conflicts with system-wide packages.
- **Permissions**: If you encounter permission errors, you might need to run the installation command with `sudo` (e.g., `sudo pip install fsspeckit`), but this is generally not recommended in a virtual environment.
- **Network Issues**: Check your internet connection if the installation fails to download packages.

For further assistance, please refer to the [official fsspeckit GitHub repository](https://github.com/legout/fsspeckit) or open an issue.