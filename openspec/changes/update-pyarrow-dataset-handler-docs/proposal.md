# Change: Update PyArrow Dataset Handler Documentation

## Why
The `add-pyarrow-dataset-handler` implementation is complete and provides class-based PyArrow dataset operations with full API parity to DuckDB. However, the documentation only covers DuckDB classes and function-based PyArrow approaches, leaving users unaware of the new class-based PyArrow interface that provides the same ergonomic benefits as DuckDB.

## What Changes
- Update API reference documentation to include complete `PyarrowDatasetIO` and `PyarrowDatasetHandler` class documentation
- Update dataset handlers documentation to include class-based PyArrow approach alongside function-based approach
- Update getting started tutorial to show PyArrow examples alongside DuckDB examples
- Update API guide to include PyArrow handler classes in capability overview
- Update how-to guides to demonstrate class-based PyArrow usage patterns
- Ensure all documentation presents both function-based and class-based PyArrow approaches as valid options with clear guidance on when to use each

## Impact
- Improves discoverability of new PyArrow class-based interface
- Provides complete documentation parity between DuckDB and PyArrow handlers
- Enables users to make informed decisions between function-based vs class-based PyArrow approaches
- Reduces cognitive overhead when switching between backends
- Enhances developer experience with comprehensive examples and migration guidance