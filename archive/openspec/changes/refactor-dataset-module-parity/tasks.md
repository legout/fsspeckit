## 1. Baseline Inventory and Parity Matrix
- [x] 1.1 Enumerate public dataset APIs and signatures for PyArrow and DuckDB
- [x] 1.2 Document behavioral differences and defaults (parity matrix)
- [x] 1.3 Identify unused dataset code with evidence (rg/tests/docs)

## 2. API Surface Unification
- [x] 2.1 Align PyarrowDatasetIO and DuckDBDatasetIO method signatures + defaults
- [x] 2.2 Ensure function wrappers are thin delegates to class-based API
- [x] 2.3 Normalize result types (WriteDatasetResult, MergeResult) and error messages

## 3. Shared Core Logic Extraction
- [x] 3.1 Centralize shared validation/normalization in datasets/base.py or core helpers
- [x] 3.2 Remove duplicated backend logic by delegating to shared helpers

## 4. Remove Unused Code
- [x] 4.1 Delete confirmed dead modules/functions
- [x] 4.2 Update exports and documentation to reflect removals

## 5. Parity + Dependency Loading Tests
- [x] 5.1 Add contract tests for write/merge/compact/optimize parity
- [x] 5.2 Add tests for conditional dependency loading and error messaging

## 6. Documentation and Migration Notes
- [x] 6.1 Update dataset-handlers docs and API references
- [x] 6.2 Add migration notes for removed/renamed APIs
