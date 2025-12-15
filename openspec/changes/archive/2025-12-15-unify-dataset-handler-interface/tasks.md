## 1. Implementation

- [x] 1.1 Define a minimal protocol / interface description for dataset handlers (DuckDB, PyArrow) covering shared methods.
- [x] 1.2 Align method names and signatures where feasible; add thin aliases if needed to maintain backwards compatibility.
- [x] 1.3 Add type annotations or protocol class to aid static analysis (optional if sticking to docs-only).

## 2. Testing

- [x] 2.1 Verify that both handlers satisfy the documented surface (via unit tests or static checks if a protocol is added).

## 3. Documentation

- [x] 3.1 Update docs to show the shared handler surface and note backend-specific differences.
- [x] 3.2 Add a comparison table for DuckDB vs PyArrow handler capabilities.

