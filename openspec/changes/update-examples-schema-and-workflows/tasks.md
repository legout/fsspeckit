## 1. Schema Examples Safety + Compatibility

- [x] 1.1 Fix `examples/datasets/schema/schema_basics.py`:
  - Replace removed PyArrow type helpers
  - Ensure cleanup deletes only the created temp directory
- [x] 1.2 Fix `examples/datasets/schema/schema_unification.py`:
  - Replace invalid `parents_only` usage
  - Fix dataset creation using supported PyArrow APIs
- [x] 1.3 Fix `examples/datasets/schema/type_optimization.py`:
  - Replace broken PyArrow table creation with supported APIs

## 2. Workflow Examples (Local-First)

- [x] 2.1 Fix `examples/datasets/workflows/performance_optimization.py` PyArrow table creation and reduce default dataset size for smoke runs
- [x] 2.2 Decide the fate of `examples/datasets/workflows/cloud_datasets.py`:
  - Either rewrite as local-first with optional real-cloud configuration, or remove/migrate to docs-only narrative

## 3. Validation

- [x] 3.1 Run all updated scripts with `.venv/bin/python` and confirm successful completion offline
- [x] 3.2 Include in updated runner categories (or remove if deprecated)

