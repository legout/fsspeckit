# Project Context

## Purpose
fsspeckit is a comprehensive Python toolkit that extends fsspec (filesystem specification) with enhanced utilities for multi-cloud storage, advanced caching, and multi-format data I/O operations. The project aims to simplify working with various filesystems (local, S3, GCS, Azure, GitHub, GitLab) by providing unified configuration, improved caching with monitoring, and streamlined data operations for JSON, CSV, Parquet, and other formats.

## Tech Stack
- **Python 3.11+** - Core language with modern type hints
- **fsspec** - Filesystem interface and protocol implementations
- **msgspec** - Fast serialization for configuration objects
- **PyArrow** - Columnar data format support
- **Polars** - High-performance DataFrame operations
- **pandas** - Data analysis compatibility layer
- **obstore** - Object storage integration
- **loguru** - Structured logging
- **joblib** - Parallel processing utilities
- **rich** - Progress bars and terminal formatting
- **Cloud SDKs**: boto3 (AWS), gcsfs (GCP), adlfs (Azure)

## Project Conventions

### Code Style
- **Type hints**: All public APIs must have complete type annotations
- **Docstrings**: Google-style docstrings with examples
- **Naming**: snake_case for functions/variables, PascalCase for classes
- **Imports**: Grouped imports (stdlib, third-party, local) with isort formatting
- **Line length**: 88 characters (Black standard)
- **Linting**: ruff for linting and formatting, mypy for type checking

### Typing Conventions
fsspeckit uses modern Python typing conventions (PEP 604, PEP 585):

- **PEP 604 unions**: Use `X | Y` instead of `Union[X, Y]`
  - Optional types: Write `str | None` instead of `Optional[str]` or `Union[str, None]`
  - Example: `def func(x: int | str) -> bool | None: ...`

- **Built-in generics**: Use built-in collection types instead of typing classes
  - `list[int]` instead of `List[int]`
  - `dict[str, Any]` instead of `Dict[str, Any]`
  - `tuple[int, ...]` instead of `Tuple[int, ...]`
  - `set[str]` instead of `Set[str]`

- **collections.abc**: Prefer `collections.abc` for callables and iterables
  - `from collections.abc import Callable, Iterable`
  - Write `Callable[[int], str]` instead of `typing.Callable[[int], str]`

- **from __future__ import annotations**: All modules should include this to enable forward references and avoid runtime evaluation issues

- **Optional dependencies**: Types from optional dependencies (polars, pandas, pyarrow, duckdb, sqlglot, orjson) must:
  - Be imported only under `if TYPE_CHECKING:` for type checkers, OR
  - Be obtained via helpers in `fsspeckit.common.optional` for runtime access
  - Never be imported at module level outside TYPE_CHECKING blocks
  - Example:
    ```python
    if TYPE_CHECKING:
        import polars as pl

    def func(df: pl.DataFrame) -> str:  # OK: type checker sees this
        from fsspeckit.common.optional import _import_polars
        pl = _import_polars()  # OK: runtime import
        # ... use pl ...
    ```
  
### Architecture Patterns
- **Modular design**: Core functionality separated into `core/`, `storage_options/`, and `utils/`
- **Abstract base classes**: `BaseStorageOptions` and `AbstractFileSystem` for extensibility
- **Factory pattern**: `filesystem()` function for unified filesystem creation
- **Configuration objects**: msgspec Structs for type-safe storage configurations
- **Caching layer**: Enhanced SimpleCacheFileSystem with custom path mapping

### Testing Strategy
- **pytest** with coverage reporting (minimum 80% coverage)
- **Test structure**: Unit tests in `tests/`, integration tests for cloud providers
- **Fixtures**: Shared test data and mock filesystems in `conftest.py`
- **Coverage**: HTML and XML reports generated on CI
- **Mocking**: pytest-mock for external service dependencies

### Git Workflow
- **Main branch**: `main` is the stable release branch
- **Versioning**: Semantic versioning in `pyproject.toml`
- **CI/CD**: Automatic PyPI publishing on version bump via GitHub Actions
- **Commits**: Conventional commits encouraged for changelog generation

## Domain Context
fsspeckit operates in the data engineering and cloud storage domain, providing abstractions for:
- **Object storage protocols**: S3, GCS, Azure Blob Storage
- **Version control filesystems**: GitHub, GitLab API access
- **Local filesystem operations**: Enhanced caching and monitoring
- **Data format handling**: Parquet, CSV, JSON with automatic format detection
- **Parallel processing**: Multi-threaded file operations and data transformations

## Important Constraints
- **Python 3.11+ minimum**: Uses modern language features
- **Cloud credentials**: Requires proper authentication for cloud providers
- **Memory usage**: Large datasets may require chunked processing
- **Network dependencies**: Cloud operations require internet connectivity
- **License compatibility**: MIT license to maximize compatibility

## External Dependencies
- **fsspec ecosystem**: Core filesystem implementations
- **Cloud provider APIs**: AWS S3, Google Cloud Storage, Azure Storage
- **Version control APIs**: GitHub REST API, GitLab API
- **Data processing libraries**: PyArrow for columnar data, Polars for DataFrames
- **Package management**: uv for fast dependency resolution
- **Documentation**: MkDocs with Material theme for API docs
