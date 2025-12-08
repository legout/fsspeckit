# Contributing to fsspeckit

Thank you for your interest in contributing to fsspeckit! This document provides guidelines and information for contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Architectural Guidelines](#architectural-guidelines)
- [Import Layering Rules](#import-layering-rules)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)

## Code of Conduct

Please note that this project adheres to a Code of Conduct. By participating, you are expected to uphold this code.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/fsspeckit.git`
3. Create a virtual environment: `python -m venv .venv`
4. Activate it: `source .venv/bin/activate` (Linux/Mac) or `.venv\Scripts\activate` (Windows)
5. Install dependencies: `uv sync --dev`
6. Run tests: `uv run pytest`

## Development Environment

We use [uv](https://docs.astral.sh/uv/) for dependency management and packaging.

### Setup

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync dependencies
uv sync --dev
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=fsspeckit --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_basic.py
```

## Architectural Guidelines

fsspeckit follows a domain-driven architecture with clear package boundaries and layering rules.

### Package Structure

```
fsspeckit/
├── common/          # Shared utilities (lowest level)
├── core/            # Core filesystem and I/O operations
├── datasets/        # Backend-specific dataset operations
├── sql/             # SQL-related functionality
└── utils/           # Backwards compatibility façade
```

### Dependency Flow

```
common → core → datasets → sql
   ↑        ↑
   └────────┴── utils (re-exports for backwards compatibility)
```

**Important**: Dependencies should only flow in one direction (downwards in the diagram). No upward or cross-package dependencies except through the utils façade.

## Import Layering Rules

### Critical Rules

1. **`fsspeckit.common`** can be imported by anyone
   - Contains shared utilities and schemas
   - No dependencies on other fsspeckit packages

2. **`fsspeckit.core`** can import from:
   - `fsspeckit.common`
   - `fsspeckit.storage_options`
   - External packages (fsspec, pyarrow, etc.)

3. **`fsspeckit.core` MUST NOT import from:**
   - `fsspeckit.datasets`
   - `fsspeckit.sql`

4. **`fsspeckit.datasets`** can import from:
   - `fsspeckit.common`
   - `fsspeckit.core`
   - `fsspeckit.storage_options`
   - External packages

5. **`fsspeckit.datasets` MUST NOT import from:**
   - `fsspeckit.sql`

6. **`fsspeckit.sql`** can import from:
   - `fsspeckit.common`
   - `fsspeckit.core`
   - `fsspeckit.datasets`
   - External packages

### CI Enforcement

Import layering is enforced automatically in CI via `scripts/check_layering.py`. This check runs on every PR and will fail if violations are detected.

### Checking Layering Locally

Run the layering check manually:

```bash
uv run python scripts/check_layering.py
```

### Examples

**✅ Correct:**
```python
# core/ext/parquet.py
from fsspeckit.common.schema import cast_schema  # ✓ Common is allowed
from fsspeckit.storage_options import BaseStorageOptions  # ✓ Storage options is allowed
```

**❌ Incorrect:**
```python
# core/ext/parquet.py
from fsspeckit.datasets.pyarrow import merge_parquet_dataset  # ✗ Datasets not allowed!
```

**Correct Solution:**
```python
# Move the functionality to common or create a protocol
# that both core and datasets can use
from fsspeckit.common.schema import merge_schemas  # ✓ Use common utilities
```

## Coding Standards

### Code Style

- Follow [PEP 8](https://pep8.org/)
- Use type hints for all public APIs
- Maximum line length: 88 characters (Black default)
- Use meaningful variable and function names

### Type Hints

```python
from typing import Optional

def process_data(
    data: list[dict[str, Any]],
    options: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Process data with optional configuration."""
    pass
```

### Docstrings

Use Google-style docstrings:

```python
def unify_schemas(schemas: list[pa.Schema]) -> pa.Schema:
    """Unify multiple PyArrow schemas into a single schema.

    Args:
        schemas: List of PyArrow schemas to unify

    Returns:
        A unified PyArrow schema

    Raises:
        ValueError: If schemas cannot be unified
    """
    pass
```

## Testing

### Writing Tests

- Place tests in the `tests/` directory
- Name test files `test_<module>.py`
- Name test functions `test_<functionality>`
- Use descriptive test names that explain what is being tested

### Test Structure

```python
def test_something_specific():
    """Test a specific piece of functionality."""
    # Arrange
    input_data = ...

    # Act
    result = ...

    # Assert
    assert result == expected
```

### Backwards Compatibility Tests

If you change public APIs, update backwards compatibility tests in:
`tests/test_utils/test_utils_backwards_compat.py`

## Documentation

### Module Docstrings

All modules should have a docstring describing their purpose:

```python
"""Module description here.

This module contains...
"""
```

### README Updates

If your changes affect user-facing functionality, update the README.md.

### API Documentation

API documentation is auto-generated from docstrings. Ensure all public APIs are documented.

## Submitting Changes

### Before Submitting

1. Run the full test suite: `uv run pytest`
2. Check import layering: `uv run python scripts/check_layering.py`
3. Run type checking: `uv run mypy src/fsspeckit`
4. Ensure all tests pass
5. Update documentation as needed

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): description

[optional body]

[optional footer]
```

Examples:
- `fix(core): resolve circular import in filesystem module`
- `feat(datasets): add merge-aware write for parquet`
- `docs: update API reference for schema utilities`

### Pull Requests

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make your changes
3. Write tests for new functionality
4. Update documentation
5. Commit with a conventional commit message
6. Push to your fork
7. Create a pull request

### Pull Request Checklist

- [ ] All tests pass
- [ ] Import layering check passes
- [ ] Type checking passes (no new mypy errors)
- [ ] Code follows style guidelines
- [ ] Tests added/updated for new functionality
- [ ] Documentation updated
- [ ] Commit message follows conventional format

## Architecture Decisions

For significant architectural changes, an Architectural Decision Record (ADR) should be created in `docs/architecture/`. See `docs/architecture/0001-layering-rules.md` for an example.

## Questions?

If you have questions about contributing, please:

1. Check existing issues and PRs
2. Create a new issue with the `question` label
3. Join the community discussion

## References

- [Project README](README.md)
- [Migration Guide](docs/how-to/migrate-package-layout.md)
- [API Documentation](docs/api/)
- [OpenSpec Project](openspec/)
