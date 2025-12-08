## 1. Implementation

- [ ] 1.1 Create `PyarrowDatasetIO` class under the `fsspeckit.datasets.pyarrow` package, encapsulating existing PyArrow
      dataset functions (read/write/merge/compact/optimize where supported) defined in the package-based layout.
- [ ] 1.2 Add `PyarrowDatasetHandler` thin wrapper for convenience/backward compatibility; re-export from
      `fsspeckit.datasets`.
- [ ] 1.3 Align method names and signatures with `DuckDBDatasetIO`/`DuckDBParquetHandler` where feasible.
- [ ] 1.4 Ensure optional dependencies are handled lazily (do not import PyArrow at module import time).

## 2. Testing

- [ ] 2.1 Add unit tests for the new handler covering key methods and ensuring delegation to underlying functions.
- [ ] 2.2 Add optional-dependency tests to confirm import succeeds without PyArrow installed and raises clear ImportError when methods are invoked.

## 3. Documentation

- [ ] 3.1 Update docs to present `PyarrowDatasetHandler` alongside `DuckDBDatasetHandler`, with guidance on when to choose each.
- [ ] 3.2 Add examples demonstrating equivalent workflows in both backends.
