---
brief: Fix Storage Options Error Handling
date: 2025-12-03
# End date (estimated): 2025-12-05
status: draft
---

# Fix Storage Options Error Handling

## Why

Storage option classes under `fsspeckit.storage_options` form core entry points
for configuring filesystems and object stores. While most logic delegates to
fsspec and cloud SDKs, errors around configuration, environment variables, and
credentials need to be reported with clear, specific exceptions and without
generic `except Exception:` patterns or silent failures.

Aligning these modules with the global error-handling and logging requirements
from `standardize-error-handling-logging` ensures configuration problems are
diagnosable and do not fail silently.

## What Changes

**Scope**: `src/fsspeckit/storage_options/base.py`, `cloud.py`, `core.py`,
and `git.py`.

- Ensure configuration and validation failures use specific, well-documented
  exception types (e.g. `ValueError`, `FileNotFoundError`, `PermissionError`).
- Avoid introducing broad `except Exception:` blocks; when a catch-all is
  required, log and re-raise instead of swallowing errors.
- Provide clearer error messages when environment variable or profile-based
  configuration is incomplete or inconsistent.
- Route any error reporting through the centralized logging utilities rather
  than `print()`.

## Impact

- **Behaviour**: Storage options will fail fast with clear, specific exceptions
  when configuration is invalid.
- **Debuggability**: Misconfigurations will be easier to diagnose using log
  messages and exception types.
- **Maintainability**: Error-handling patterns in storage options will be
  consistent with the rest of the codebase.

## Code affected

- `src/fsspeckit/storage_options/base.py`
- `src/fsspeckit/storage_options/cloud.py`
- `src/fsspeckit/storage_options/core.py`
- `src/fsspeckit/storage_options/git.py`

