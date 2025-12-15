## 1. Implementation

- [x] 1.1 Create `PyarrowDatasetIO` class under the `fsspeckit.datasets.pyarrow` package, encapsulating existing PyArrow
      dataset functions (read/write/merge/compact/optimize where supported) defined in the package-based layout.
- [x] 1.2 Add `PyarrowDatasetHandler` thin wrapper for convenience/backward compatibility; re-export from
      `fsspeckit.datasets`.
- [x] 1.3 Align method names and signatures with `DuckDBDatasetIO`/`DuckDBParquetHandler` where feasible.
- [x] 1.4 Ensure optional dependencies are handled lazily (do not import PyArrow at module import time).

## 2. Testing

- [x] 2.1 Add unit tests for the new handler covering key methods and ensuring delegation to underlying functions.
- [x] 2.2 Add optional-dependency tests to confirm import succeeds without PyArrow installed and raises clear ImportError when methods are invoked.

## 3. Documentation

- [x] 3.1 Update docs to present `PyarrowDatasetHandler` alongside `DuckDBDatasetHandler`, with guidance on when to choose each.
  - Completed: `docs/how-to/read-and-write-datasets.md` includes PyArrow and DuckDB sections with Backend Selection Guidance
  - Completed: `docs/how-to/merge-datasets.md` includes comprehensive Backend Selection Guidance section with detailed comparison
- [x] 3.2 Add examples demonstrating equivalent workflows in both backends.
  - Completed: Both docs show side-by-side examples for all merge strategies (INSERT, UPSERT, UPDATE, FULL_MERGE, DEDUPLICATE)
  - Completed: Real-world examples demonstrating equivalent workflows in both PyArrow and DuckDB backends
