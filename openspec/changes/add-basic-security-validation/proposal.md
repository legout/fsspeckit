## Why

While the codebase does not directly expose raw SQL to untrusted users, there are several places where:

- Paths and compression codecs are interpolated into SQL strings or used to configure dataset operations without validation.
- Storage options may log exceptions that include sensitive credentials (access keys, tokens).
- Some helper functions accept user-provided paths without checking for path traversal or obviously malformed values.

These patterns are not immediate vulnerabilities in most deployments, but they are fragile, make it easy to misuse the APIs, and may leak sensitive information into logs when errors occur.

## What Changes

- Introduce basic validation for:
  - Paths used in dataset read/write operations (e.g. reject control characters, suspicious patterns, or paths that escape an expected base).
  - Compression codecs or similar configuration parameters used in SQL or filesystem calls (e.g. enforce membership in a known set).
- Add a small, reusable helper in storage options to scrub sensitive credential-like values from error messages before logging them.
- Apply these validations and scrubbing in DuckDB/PyArrow dataset helpers and in AWS (and potentially other cloud) storage options where exceptions are logged.

## Impact

- **Behaviour:**
  - Callers providing obviously invalid paths or codecs will receive early `ValueError`s with clear messages, rather than triggering obscure errors inside DuckDB/PyArrow or the filesystem.
  - Logs produced by storage option helpers will avoid containing raw access keys or tokens, reducing the risk of credential leakage.

- **Specs affected:**
  - `utils-duckdb` (DuckDB-based dataset helpers and options).
  - `utils-pyarrow` (PyArrow-based dataset helpers).
  - `storage-aws` (and potentially other cloud storage options) for credential scrubbing.

- **Code affected (non-exhaustive):**
  - `src/fsspeckit/datasets/duckdb.py`
  - `src/fsspeckit/datasets/pyarrow.py`
  - `src/fsspeckit/storage_options/cloud.py`

