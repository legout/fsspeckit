#!/usr/bin/env python
"""Performance benchmark for AdaptiveKeyTracker with large datasets.

This script evaluates the overhead of to_pylist() and tracker.insert()
for datasets with 10M+ rows.
"""

import time
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

try:
    import pyarrow as pa
except ImportError:
    print(
        "PyArrow is required for this benchmark. Install it with: pip install pyarrow"
    )
    exit(1)


def benchmark_key_tracker(row_counts: list[int] = [1_000_000, 5_000_000, 10_000_000]):
    """Benchmark AdaptiveKeyTracker performance with different dataset sizes.

    Args:
        row_counts: List of row counts to test
    """
    print("=" * 80)
    print("AdaptiveKeyTracker Performance Benchmark")
    print("=" * 80)

    for row_count in row_counts:
        print(f"\n--- Testing with {row_count:,} rows ---")

        # Generate test data with integer key column
        print(f"  Generating test data...")
        key_array = pa.array(range(row_count))
        data_array = pa.array([f"value_{i}" for i in range(row_count)])
        table = pa.Table.from_arrays([key_array, data_array], names=["id", "value"])

        # Extract key column for tracking
        key_column = table.column("id")

        # Test 1: to_pylist() conversion time
        print(f"  Testing to_pylist() conversion...")
        start_time = time.time()
        keys_as_list = key_column.to_pylist()
        to_pylist_time = time.time() - start_time
        print(f"    to_pylist() time: {to_pylist_time:.4f}s")

        # Test 2: Tracker insertion time
        print(f"  Testing AdaptiveKeyTracker insertion...")
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker()
        start_time = time.time()

        # Insert keys in batches of 100K to simulate realistic usage
        batch_size = 100_000
        for i in range(0, len(keys_as_list), batch_size):
            batch = keys_as_list[i : i + batch_size]
            tracker.insert_batch(batch)

        tracker_time = time.time() - start_time
        print(f"    Tracker insertion time: {tracker_time:.4f}s")

        # Test 3: Membership check time
        print(f"  Testing membership checks...")
        start_time = time.time()

        # Check membership for a subset of keys
        check_keys = keys_as_list[:100_000]
        check_count = 0
        for key in check_keys:
            if tracker.contains(key):
                check_count += 1

        check_time = time.time() - start_time
        print(f"    Membership check time: {check_time:.4f}s")
        print(f"    Checked {check_count:,} keys (expected 100,000)")

        # Summary
        print(f"\n  Summary for {row_count:,} rows:")
        print(f"    Total time: {to_pylist_time + tracker_time:.4f}s")
        print(
            f"    - to_pylist(): {to_pylist_time:.4f}s ({to_pylist_time / (to_pylist_time + tracker_time) * 100:.1f}%)"
        )
        print(
            f"    - Tracker insertion: {tracker_time:.4f}s ({tracker_time / (to_pylist_time + tracker_time) * 100:.1f}%)"
        )
        print(f"    Tracker size: {len(tracker):,} keys")
        print(
            f"    Bytes per key: {(tracker._total_bytes / len(tracker)) if len(tracker) > 0 else 0:.2f}"
        )


def benchmark_vectorized_operations(row_count: int = 10_000_000):
    """Benchmark vectorized key operations using PyArrow compute.

    Args:
        row_count: Number of rows to test
    """
    print("\n" + "=" * 80)
    print("Vectorized Operations Benchmark")
    print("=" * 80)
    print(f"\n--- Testing vectorized operations with {row_count:,} rows ---")

    # Generate test data
    print(f"  Generating test data...")
    key_array = pa.array(range(row_count))
    table = pa.Table.from_arrays([key_array], names=["id"])

    # Test vectorized filtering
    print(f"  Testing vectorized filtering with is_in()...")
    import pyarrow.compute as pc

    # Create a value set to filter against
    value_keys = pa.array(range(0, row_count, 100))  # Every 100th key

    start_time = time.time()
    mask = pc.is_in(table.column("id"), value_set=pa.chunked_array([value_keys]))
    filtered = table.filter(mask)
    filter_time = time.time() - start_time

    print(f"    Filter time: {filter_time:.4f}s")
    print(f"    Filtered to {filtered.num_rows:,} rows (expected {len(value_keys):,})")

    # Test vectorized duplicate detection
    print(f"\n  Testing vectorized duplicate detection...")
    dupe_table = pa.concat_tables([table, table])

    start_time = time.time()
    # Use count() with groups to find duplicates
    grouped = pc.count(dupe_table, "id")
    dedup_time = time.time() - start_time

    print(f"    Deduplication time: {dedup_time:.4f}s")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Benchmark AdaptiveKeyTracker performance"
    )
    parser.add_argument(
        "--rows",
        type=int,
        nargs="+",
        default=[1_000_000, 5_000_000, 10_000_000],
        help="Row counts to test (default: 1M, 5M, 10M)",
    )
    parser.add_argument(
        "--vectorized",
        action="store_true",
        help="Also benchmark vectorized PyArrow operations for comparison",
    )

    args = parser.parse_args()

    try:
        benchmark_key_tracker(args.rows)

        if args.vectorized:
            benchmark_vectorized_operations(max(args.rows))

        print("\n" + "=" * 80)
        print("Benchmark complete!")
        print("=" * 80)

    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user.")
        exit(0)
    except Exception as e:
        print(f"\n\nError during benchmark: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
