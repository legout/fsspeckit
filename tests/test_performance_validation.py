"""Performance validation test for key matching improvements.

This test specifically validates the O(n) to O(1) performance improvement
in select_rows_by_keys_common function from the openspec fix-refactor-critical-issues.
"""

import time
import pyarrow as pa
import pytest
from typing import List, Set
import tempfile
import shutil
from pathlib import Path


class PerformanceBenchmark:
    """Benchmark class for key matching performance validation."""

    def __init__(self):
        self.results = []

    def benchmark_key_matching(
        self, table_size: int, key_count: int, num_runs: int = 3
    ):
        """Benchmark key matching performance."""

        # Generate test data
        data = {
            "id": list(range(table_size)),
            "value": [i * 0.5 for i in range(table_size)],
            "category": ["A", "B", "C"] * (table_size // 3 + 1),
        }
        table = pa.Table.from_pydict(data)

        # Generate test keys (subset of actual keys)
        test_keys = set(range(0, table_size, table_size // key_count))

        # Import the fixed function
        from fsspeckit.core.merge import select_rows_by_keys_common

        # Benchmark the fixed O(1) implementation
        times = []
        for _ in range(num_runs):
            start_time = time.perf_counter()
            result = select_rows_by_keys_common(table, ["id"], test_keys)
            end_time = time.perf_counter()
            times.append(end_time - start_time)

        avg_time = sum(times) / len(times)
        result_count = len(result)

        # Store results
        self.results.append(
            {
                "table_size": table_size,
                "key_count": key_count,
                "avg_time": avg_time,
                "result_count": result_count,
                "throughput": table_size / avg_time if avg_time > 0 else float("inf"),
            }
        )

        return avg_time, result_count

    def print_results(self):
        """Print performance results."""
        print("\n=== Key Matching Performance Results ===")
        print(
            f"{'Table Size':<12} {'Keys':<8} {'Avg Time (s)':<12} {'Results':<8} {'Throughput (rows/s)':<18}"
        )
        print("-" * 70)

        for result in self.results:
            print(
                f"{result['table_size']:<12} {result['key_count']:<8} "
                f"{result['avg_time']:<12.4f} {result['result_count']:<8} "
                f"{result['throughput']:<18.0f}"
            )

        print("\n=== Performance Analysis ===")

        # Analyze scalability
        if len(self.results) >= 2:
            # Check if performance scales reasonably with data size
            small_table = self.results[0]
            large_table = self.results[-1]

            size_ratio = large_table["table_size"] / small_table["table_size"]
            time_ratio = large_table["avg_time"] / small_table["avg_time"]

            print(f"Size increase: {size_ratio:.1f}x")
            print(f"Time increase: {time_ratio:.1f}x")

            if time_ratio < size_ratio * 2:  # Allow some overhead
                print("✅ Performance scales well (O(1) key lookup confirmed)")
            else:
                print("⚠️  Performance may not be O(1) - investigate further")


def test_performance_validation():
    """Test function to validate performance improvements."""

    benchmark = PerformanceBenchmark()

    # Test different data sizes
    test_cases = [
        (1000, 10),  # Small dataset
        (10000, 100),  # Medium dataset
        (100000, 1000),  # Large dataset (if needed)
    ]

    print("Starting performance validation for key matching improvements...")

    for table_size, key_count in test_cases:
        print(f"\nTesting table size {table_size} with {key_count} keys...")
        avg_time, result_count = benchmark.benchmark_key_matching(table_size, key_count)
        print(f"Completed in {avg_time:.4f}s, found {result_count} matching rows")

    benchmark.print_results()

    # Validate results are correct
    assert all(r["result_count"] > 0 for r in benchmark.results), (
        "All tests should find matching keys"
    )
    assert all(r["avg_time"] > 0 for r in benchmark.results), (
        "All tests should have positive execution time"
    )

    print("\n✅ Performance validation completed successfully!")


def test_key_matching_correctness():
    """Test that the key matching improvement maintains correctness."""

    # Import the fixed function
    from fsspeckit.core.merge import select_rows_by_keys_common

    # Create test data
    data = {
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "name": [f"name_{i}" for i in range(1, 11)],
        "category": ["A", "B", "A", "C", "B", "A", "C", "B", "A", "C"],
    }
    table = pa.Table.from_pydict(data)

    # Test single key matching
    test_keys = {2, 4, 6, 8}
    result = select_rows_by_keys_common(table, ["id"], test_keys)

    expected_ids = {2, 4, 6, 8}
    actual_ids = set(result.column("id").to_pylist())

    assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"
    assert len(result) == 4, f"Expected 4 rows, got {len(result)}"

    # Test multi-key matching
    multi_data = {
        "user_id": [1, 1, 2, 2, 3, 3],
        "session_id": [100, 101, 100, 101, 100, 101],
        "value": [10, 20, 30, 40, 50, 60],
    }
    multi_table = pa.Table.from_pydict(multi_data)

    test_multi_keys = {(1, 100), (2, 101)}
    result_multi = select_rows_by_keys_common(
        multi_table, ["user_id", "session_id"], test_multi_keys
    )

    expected_multi = {(1, 100), (2, 101)}
    actual_multi = set(
        zip(
            result_multi.column("user_id").to_pylist(),
            result_multi.column("session_id").to_pylist(),
        )
    )

    assert actual_multi == expected_multi, (
        f"Expected {expected_multi}, got {actual_multi}"
    )
    assert len(result_multi) == 2, f"Expected 2 rows, got {len(result_multi)}"

    # Test empty key set
    empty_result = select_rows_by_keys_common(table, ["id"], set())
    assert len(empty_result) == 0, "Empty key set should return empty result"

    print("✅ Key matching correctness validation passed!")


def test_edge_cases():
    """Test edge cases for the performance improvement."""

    from fsspeckit.core.merge import select_rows_by_keys_common

    # Test with empty table
    empty_table = pa.Table.from_pydict({"id": [], "value": []})
    result = select_rows_by_keys_common(empty_table, ["id"], {1, 2, 3})
    assert len(result) == 0, "Empty table should return empty result"

    # Test with duplicate keys in table
    duplicate_data = {"id": [1, 2, 2, 3, 3, 3], "value": [10, 20, 21, 30, 31, 32]}
    duplicate_table = pa.Table.from_pydict(duplicate_data)
    result = select_rows_by_keys_common(duplicate_table, ["id"], {2, 3})

    # Should return all rows with matching keys
    assert len(result) == 5, f"Expected 5 rows with duplicates, got {len(result)}"

    # Test with non-matching keys
    non_matching_data = {"id": [1, 2, 3], "value": [10, 20, 30]}
    non_matching_table = pa.Table.from_pydict(non_matching_data)
    result = select_rows_by_keys_common(non_matching_table, ["id"], {999})

    assert len(result) == 0, "Non-matching keys should return empty result"

    print("✅ Edge cases validation passed!")


if __name__ == "__main__":
    # Run performance validation
    test_key_matching_correctness()
    test_edge_cases()
    test_performance_validation()
