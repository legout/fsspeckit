## Why

Error handling patterns across `fsspeckit` are inconsistent and sometimes too broad:

- Multiple places use `except Exception:` (or equivalent) to catch every error, sometimes followed by an empty `pass`, hiding real problems.
- Resource cleanup (e.g. unregistering DuckDB tables, closing or cleaning up temporary state) is often done inside a single generic `try/except`, so partial failures are invisible.
- Logging behaviour is not standardised; some errors are re-raised without logging, others are printed with `print()`, and some are silently ignored.

These patterns make debugging difficult, risk masking real bugs, and undermine debugging and observability in production.

## What Changes

- Define cross-cutting error-handling and logging requirements at the architecture level:
  - Add requirements to the `project-architecture` spec for non-silencing error handling in core code paths.
  - Add requirements that route error reporting through the established logging infrastructure (e.g. via `fsspeckit.common.logging.get_logger`) rather than using bare `print()`.
- Document preferred patterns so future code follows the same conventions:
  - Add a brief error-handling and logging section to `openspec/project.md` (or contributor docs).
  - Reference these patterns in implementation notes so they are discoverable.
- Delegate concrete module-level implementation work to narrower sub-proposals:
  - `fix-duckdb-error-handling` for DuckDB datasets and cleanup helpers.
  - `fix-pyarrow-error-handling` for PyArrow datasets and utilities.
  - `fix-core-io-error-handling` for core filesystem and I/O helpers.
  - `fix-common-modules-error-handling` for shared common utilities.
  - `fix-maintenance-error-handling` for backend-neutral maintenance flows.
  - `fix-storage-options-error-handling` for storage option classes where errors are logged.

## Impact

- **Behaviour:**
  - Unexpected errors will be logged with context instead of being silently swallowed.
  - Resource cleanup will be more robust and easier to reason about, with partial failures visible in logs.
  - Error handling patterns will be more uniform, simplifying future maintenance and reviews.

- **Specs affected:**
  - `project-architecture` (coding and error-handling standards).

- **Code affected (indirectly via sub-proposals):**
  - `src/fsspeckit/datasets/duckdb.py` and related DuckDB helpers.
  - `src/fsspeckit/datasets/pyarrow.py` and PyArrow utilities.
  - `src/fsspeckit/core/ext.py` and core I/O helpers.
  - `src/fsspeckit/core/maintenance.py` and maintenance flows.
  - `src/fsspeckit/common/*.py` where errors are handled or logged.
  - `src/fsspeckit/storage_options/*.py` where errors are logged.
