# DuckDB Merge Large Dataset Benchmark

- Date: 2025-11-15
- Environment: macOS (Python 3.13.7, DuckDB 1.4.2)
- Command:     UPSERT: 0.07s | stats={'total': 1090000, 'inserted': 90000, 'updated': 210000, 'deleted': 0}
    INSERT: 0.10s | stats={'total': 1090000, 'inserted': 90000, 'updated': 0, 'deleted': 0}
FULL_MERGE: 0.05s | stats={'total': 300000, 'inserted': 300000, 'updated': 0, 'deleted': 1000000}
- Dataset: 1,000,000 target rows + 300,000 source rows (70% updates, 30% inserts)

| Strategy | Duration (s) | Inserted | Updated | Deleted |
|----------|--------------|----------|---------|---------|
| UPSERT   | 0.08         | 90,000   | 210,000 | 0       |
| INSERT   | 0.11         | 90,000   | 0       | 0       |
| FULL_MERGE | 0.04       | 300,000  | 0       | 1,000,000 |

Notes:
- Each iteration overwrote the dataset to ensure comparable baselines.
- Timings are wall-clock measurements on the shared development laptop; rerun the script for updated hardware metrics.
