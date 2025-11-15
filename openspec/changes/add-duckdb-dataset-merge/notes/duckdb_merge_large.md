# DuckDB Merge Large Dataset Benchmark

- Date: 2025-11-15
- Environment: macOS (Python 3.13.7, DuckDB 1.4.2)
- Command: `.venv/bin/python examples/duckdb/duckdb_merge_benchmark.py`
- Dataset: 1,000,000 target rows + 300,000 source rows (70% updates, 30% inserts)

| Strategy  | Duration (s) | Inserted | Updated | Deleted |
|-----------|--------------|----------|---------|---------|
| UPSERT    | 0.09         | 90,000   | 210,000 | 0       |
| INSERT    | 0.10         | 90,000   | 0       | 0       |
| FULL_MERGE| 0.05         | 300,000  | 0       | 1,000,000 |

Notes:
- Each iteration overwrote the dataset to ensure comparable baselines.
- Timings are wall-clock measurements on the shared development laptop; rerun the script for updated hardware metrics.
