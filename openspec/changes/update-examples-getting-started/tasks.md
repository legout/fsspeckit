## 1. Make Scripts Runnable (Offline)

- [x] 1.1 Update `examples/datasets/getting_started/01_duckdb_basics.py` to use supported PyArrow table construction
- [x] 1.2 Update `examples/datasets/getting_started/02_pyarrow_basics.py` similarly
- [x] 1.3 Update `examples/datasets/getting_started/03_simple_merges.py` similarly
- [x] 1.4 Update `examples/datasets/getting_started/04_pyarrow_merges.py` to be non-interactive by default (no `input()` in standard mode)

## 2. Consistency and UX

- [x] 2.1 Ensure all scripts follow the same conventions for temp directories, paths, and cleanup
- [x] 2.2 Reduce runtime and dataset sizes if needed for “smoke runnable” validation

## 3. Validation

- [x] 3.1 Run each script with `.venv/bin/python` and confirm it completes successfully
- [x] 3.2 Add these scripts to the runner’s category list (via the runner change)

