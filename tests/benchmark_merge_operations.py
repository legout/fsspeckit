"""Performance benchmarks for merge operations in DuckDB dataset.

Compares UNION ALL vs MERGE SQL performance for different dataset sizes
and merge scenarios.

Usage:
    python tests/benchmark_merge_operations.py

Requirements:
    - pytest
    - duckdb
    - pyarrow
    - pandas (for timing)
"""

import time
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple
import tempfile

import pandas as pd


class MergePerformanceBenchmark:
    """Benchmark framework for merge operations."""

    def __init__(self):
        self.results: List[Dict] = []

    def run(self):
        print("=" * 80)
        print("DuckDB Merge Operations Performance Benchmark")
        print("=" * 80)
        print()

        print("Testing small datasets (n=1000)...")
        self.benchmark_small_datasets()

        print("\nTesting medium datasets (n=10000)...")
        self.benchmark_medium_datasets()

        print("\nTesting large datasets (n=100000)...")
        self.benchmark_large_datasets()

        self.print_summary()

    def benchmark_small_datasets(self):
        sizes = [1000]
        operations = ["new_rows", "updates", "mixed"]

        for size in sizes:
            for operation in operations:
                result = self._run_benchmark(
                    dataset_size=size, operation=operation, expected_faster="union_all"
                )
                self.results.append(result)

    def benchmark_medium_datasets(self):
        sizes = [10000]
        operations = ["new_rows", "updates", "mixed"]

        for size in sizes:
            for operation in operations:
                result = self._run_benchmark(
                    dataset_size=size, operation=operation, expected_faster="union_all"
                )
                self.results.append(result)

    def benchmark_large_datasets(self):
        sizes = [100000]
        operations = ["new_rows", "updates", "mixed"]

        for size in sizes:
            for operation in operations:
                result = self._run_benchmark(
                    dataset_size=size, operation=operation, expected_faster="merge_sql"
                )
                self.results.append(result)

    def _run_benchmark(
        self, dataset_size: int, operation: str, expected_faster: str
    ) -> Dict:
        import pyarrow as pa
        from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection
        import tempfile
        import shutil
        import time
        import sys
        import importlib

        # Check if required modules are available
        try:
            import duckdb
        except ImportError:
            result = {
                "dataset_size": dataset_size,
                "operation": operation,
                "expected_faster": expected_faster,
                "union_all_time": None,
                "merge_sql_time": None,
                "faster": None,
                "speedup": None,
                "status": "skip_no_duckdb",
            }
            print(f"  - {operation} (n={dataset_size}): Skipped (DuckDB not available)")
            return result

        result = {
            "dataset_size": dataset_size,
            "operation": operation,
            "expected_faster": expected_faster,
            "union_all_time": None,
            "merge_sql_time": None,
            "faster": None,
            "speedup": None,
            "status": "success",
        }

        # Create test data based on operation
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Generate initial dataset
            if operation == "new_rows":
                # All new rows in merge
                existing = pa.table(
                    {
                        "id": list(range(1, dataset_size + 1)),
                        "value": [f"orig_{i}" for i in range(1, dataset_size + 1)],
                        "category": [
                            "cat_" + str(i % 10) for i in range(1, dataset_size + 1)
                        ],
                    }
                )
                source = pa.table(
                    {
                        "id": list(range(dataset_size + 1, dataset_size * 2 + 1)),
                        "value": [
                            f"new_{i}"
                            for i in range(dataset_size + 1, dataset_size * 2 + 1)
                        ],
                        "category": [
                            "cat_" + str(i % 10)
                            for i in range(dataset_size + 1, dataset_size * 2 + 1)
                        ],
                    }
                )
                strategy = "insert"
            elif operation == "updates":
                # All existing rows updated
                existing = pa.table(
                    {
                        "id": list(range(1, dataset_size + 1)),
                        "value": [f"orig_{i}" for i in range(1, dataset_size + 1)],
                        "category": [
                            "cat_" + str(i % 10) for i in range(1, dataset_size + 1)
                        ],
                    }
                )
                source = pa.table(
                    {
                        "id": list(range(1, dataset_size + 1)),
                        "value": [f"updated_{i}" for i in range(1, dataset_size + 1)],
                        "category": [
                            "cat_" + str(i % 10) for i in range(1, dataset_size + 1)
                        ],
                    }
                )
                strategy = "update"
            else:  # mixed
                # Half updates, half new rows
                existing = pa.table(
                    {
                        "id": list(range(1, dataset_size + 1)),
                        "value": [f"orig_{i}" for i in range(1, dataset_size + 1)],
                        "category": [
                            "cat_" + str(i % 10) for i in range(1, dataset_size + 1)
                        ],
                    }
                )
                source = pa.table(
                    {
                        "id": list(
                            range(
                                dataset_size // 2, dataset_size + dataset_size // 2 + 1
                            )
                        ),
                        "value": [
                            f"mixed_{i}"
                            for i in range(
                                dataset_size // 2, dataset_size + dataset_size // 2 + 1
                            )
                        ],
                        "category": [
                            "cat_" + str(i % 10)
                            for i in range(
                                dataset_size // 2, dataset_size + dataset_size // 2 + 1
                            )
                        ],
                    }
                )
                strategy = "upsert"

            # Write initial dataset
            conn = create_duckdb_connection()
            io = DuckDBDatasetIO(conn)
            dataset_path = str(tmppath / "dataset")
            io.write_dataset(existing, dataset_path, mode="overwrite")

            # Benchmark UNION ALL
            with tempfile.TemporaryDirectory() as tmpdir2:
                tmppath2 = Path(tmpdir2)
                dataset_path2 = str(tmppath2 / "dataset")
                io.write_dataset(existing, dataset_path2, mode="overwrite")

                # Clear connection cache to ensure fair comparison
                import gc

                gc.collect()
                time.sleep(0.1)

                # Measure UNION ALL time
                union_all_start = time.perf_counter()
                result_union_all = io.merge(
                    source,
                    dataset_path2,
                    strategy=strategy,
                    key_columns=["id"],
                    use_merge=False,
                )
                union_all_time = time.perf_counter() - union_all_start

            # Benchmark MERGE
            with tempfile.TemporaryDirectory() as tmpdir3:
                tmppath3 = Path(tmpdir3)
                dataset_path3 = str(tmppath3 / "dataset")
                io.write_dataset(existing, dataset_path3, mode="overwrite")

                # Clear connection cache
                gc.collect()
                time.sleep(0.1)

                # Measure MERGE time
                merge_sql_start = time.perf_counter()
                result_merge = io.merge(
                    source,
                    dataset_path3,
                    strategy=strategy,
                    key_columns=["id"],
                    use_merge=True,
                )
                merge_sql_time = time.perf_counter() - merge_sql_start

            # Verify results match
            assert result_union_all.inserted == result_merge.inserted
            assert result_union_all.updated == result_merge.updated

            result["union_all_time"] = union_all_time
            result["merge_sql_time"] = merge_sql_time

            if union_all_time > 0:
                if merge_sql_time < union_all_time:
                    result["faster"] = "merge_sql"
                    result["speedup"] = (
                        (union_all_time - merge_sql_time) / union_all_time
                    ) * 100
                else:
                    result["faster"] = "union_all"
                    result["speedup"] = (
                        (merge_sql_time - union_all_time) / merge_sql_time
                    ) * 100
            else:
                result["faster"] = "unknown"
                result["speedup"] = 0

            print(
                f"  - {operation} (n={dataset_size}): MERGE={merge_sql_time:.4f}s, UNION ALL={union_all_time:.4f}s, faster={result['faster']}, speedup={result['speedup']:.1f}%"
            )

        return result

    def print_summary(self):
        print("\n" + "=" * 80)
        print("Benchmark Summary")
        print("=" * 80)
        print()

        df = pd.DataFrame(self.results)

        # Group by dataset size
        for size in df["dataset_size"].unique():
            print(f"\nDataset Size: {size:,} rows")
            print("-" * 80)
            size_df = df[df["dataset_size"] == size]
            for _, row in size_df.iterrows():
                status = row["status"]
                if status == "skip_no_duckdb":
                    print(f"  {row['operation']:12s}: Skipped (DuckDB not available)")
                elif status == "success":
                    print(
                        f"  {row['operation']:12s}: MERGE={row['merge_sql_time']:7.4f}s, UNION ALL={row['union_all_time']:7.4f}s, "
                        f"faster={row['faster']:10s}, speedup={row['speedup']:6.1f}%"
                    )
                else:
                    print(f"  {row['operation']:12s}: Status={status}")

        # Overall summary
        successful_results = df[df["status"] == "success"]
        if len(successful_results) > 0:
            print("\n" + "=" * 80)
            print("Overall Performance Summary")
            print("=" * 80)

            merge_wins = len(
                successful_results[successful_results["faster"] == "merge_sql"]
            )
            union_wins = len(
                successful_results[successful_results["faster"] == "union_all"]
            )
            total = len(successful_results)

            print(
                f"\nMERGE wins: {merge_wins}/{total} ({merge_wins / total * 100:.1f}%)"
            )
            print(
                f"UNION ALL wins: {union_wins}/{total} ({union_wins / total * 100:.1f}%)"
            )

            if merge_wins > 0:
                avg_speedup = successful_results[
                    successful_results["faster"] == "merge_sql"
                ]["speedup"].mean()
                print(f"\nAverage speedup when MERGE is faster: {avg_speedup:.1f}%")

            if union_wins > 0:
                avg_slowdown = successful_results[
                    successful_results["faster"] == "union_all"
                ]["speedup"].mean()
                print(f"Average slowdown when UNION ALL is faster: {avg_slowdown:.1f}%")
        else:
            print("\nNo successful benchmarks run. Check if DuckDB is installed.")


def main():
    benchmark = MergePerformanceBenchmark()
    benchmark.run()


if __name__ == "__main__":
    main()
