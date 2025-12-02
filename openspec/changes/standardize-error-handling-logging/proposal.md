## Why

Error handling patterns across `fsspeckit` are inconsistent and sometimes too broad:

- Multiple places use `except Exception:` (or equivalent) to catch every error, sometimes followed by an empty `pass`, hiding real problems.
- Resource cleanup (e.g. unregistering DuckDB tables, closing or cleaning up temporary state) is often done inside a single generic `try/except`, so partial failures are invisible.
- Logging behaviour is not standardised; some errors are re-raised without logging, others are printed with `print()`, and some are silently ignored.

These patterns make debugging difficult, risk masking real bugs, and undermine debugging and observability in production.

## What Changes

- Establish and apply consistent error-handling practices in core paths:
  - Replace generic `except Exception:` blocks with:
    - Narrower exception types where they are expected.
    - A catch-all that logs and re-raises unexpected exceptions where truly necessary, rather than swallowing them.
  - Standardise resource cleanup by:
    - Splitting cleanup work (e.g. unregistering DuckDB tables) into small helper functions that handle each resource individually.
    - Ensuring cleanup helpers log failures with enough context while keeping normal operation clean.
- Route error logging through the established logging infrastructure (e.g. via `fsspeckit.common.logging.get_logger`) rather than using bare `print()`.

## Impact

- **Behaviour:**
  - Unexpected errors will be logged with context instead of being silently swallowed.
  - Resource cleanup will be more robust and easier to reason about, with partial failures visible in logs.
  - Error handling patterns will be more uniform, simplifying future maintenance and reviews.

- **Specs affected:**
  - `project-architecture` (coding and error-handling standards).

- **Code affected (non-exhaustive):**
  - `src/fsspeckit/datasets/duckdb.py`
  - `src/fsspeckit/datasets/pyarrow.py`
  - `src/fsspeckit/core/ext.py`
  - `src/fsspeckit/storage_options/*.py` where errors are logged.

