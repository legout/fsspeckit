"""
PyArrow Multi-Key Vectorization Examples

This script demonstrates the new multi-column key vectorization capabilities
in fsspeckit, providing examples of deduplication and merge operations
with composite keys.

Run with: python examples/multi_key_vectorization_demo.py
"""

import pyarrow as pa
import tempfile
import os
from pathlib import Path

# Import multi-key functions
from fsspeckit.datasets.pyarrow import (
    deduplicate_pyarrow,
    merge_parquet_dataset_pyarrow,
)


def demo_basic_composite_key_deduplication():
    """Demonstrate basic composite key deduplication."""
    print("=== Basic Composite Key Deduplication ===")

    # Create sample multi-tenant data with duplicates
    data = {
        "tenant_id": [1, 1, 1, 2, 2, 2, 1],
        "user_id": [100, 100, 101, 200, 201, 200, 102],
        "record_id": [1, 1, 2, 1, 1, 1, 3],  # Duplicates exist
        "value": [10, 20, 30, 40, 50, 60, 70],  # Conflicting values
        "timestamp": [
            "2024-01-01",
            "2024-01-02",
            "2024-01-01",
            "2024-01-01",
            "2024-01-01",
            "2024-01-03",
            "2024-01-01",
        ],
    }

    table = pa.Table.from_pydict(data)
    print(f"Original data: {table.num_rows} rows")
    print("Sample rows:")
    print(table.slice(0, 5).to_pandas())

    # Deduplicate using composite key [tenant_id, user_id, record_id]
    unique_table = deduplicate_pyarrow(
        table=table,
        key_columns=["tenant_id", "user_id", "record_id"],
        dedup_order_by=["timestamp"],  # Keep first occurrence by timestamp
        keep="first",
    )

    print(f"\nAfter deduplication: {unique_table.num_rows} rows")
    print(f"Removed {table.num_rows - unique_table.num_rows} duplicate rows")
    print("Unique rows:")
    print(unique_table.to_pandas().sort_values(["tenant_id", "user_id", "record_id"]))
    print()


def demo_composite_key_merge():
    """Demonstrate merge operations with composite keys."""
    print("=== Composite Key Merge Operations ===")

    # Existing dataset
    existing_data = {
        "tenant_id": [1, 1, 2, 2],
        "customer_id": [100, 101, 200, 201],
        "order_id": [1001, 1002, 2001, 2002],
        "status": ["confirmed", "confirmed", "pending", "confirmed"],
        "amount": [150.0, 200.0, 100.0, 250.0],
    }

    # New incoming data (updates + new records)
    new_data = {
        "tenant_id": [1, 1, 2, 2, 3],
        "customer_id": [100, 103, 200, 203, 300],
        "order_id": [1001, 1003, 2001, 2004, 3001],
        "status": ["shipped", "confirmed", "confirmed", "pending", "confirmed"],
        "amount": [150.0, 175.0, 100.0, 300.0, 400.0],
    }

    existing_table = pa.Table.from_pydict(existing_data)
    new_table = pa.Table.from_pydict(new_data)

    print(f"Existing dataset: {existing_table.num_rows} rows")
    print(f"New data: {new_table.num_rows} rows")

    # Use temporary directory for demo
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = os.path.join(temp_dir, "orders")

        # Write initial dataset
        merge_parquet_dataset_pyarrow(
            data=existing_table,
            path=dataset_path,
            strategy="append",  # Initial load
        )

        # Upsert new data using composite key
        stats = merge_parquet_dataset_pyarrow(
            data=new_table,
            path=dataset_path,
            strategy="upsert",
            key_columns=["tenant_id", "customer_id", "order_id"],
        )

        print(f"\nMerge Statistics:")
        print(f"  Source rows: {stats.source_rows}")
        print(f"  Existing rows: {stats.existing_rows}")
        print(f"  Upserted rows: {stats.upserted_rows}")
        print(f"  Inserted rows: {stats.inserted_rows}")
        print(f"  Updated rows: {stats.updated_rows}")
    print()


def demo_performance_comparison():
    """Demonstrate performance comparison between single and multi-column keys."""
    print("=== Performance Comparison ===")

    import time
    from fsspeckit.datasets.pyarrow.dataset import PerformanceMonitor

    # Create larger test dataset
    n_rows = 10000
    data = {
        "id1": [i % 100 for i in range(n_rows)],
        "id2": [i % 50 for i in range(n_rows)],
        "id3": [i % 25 for i in range(n_rows)],
        "value": list(range(n_rows)),
        "timestamp": [1704067200 + i for i in range(n_rows)],
    }

    test_table = pa.Table.from_pydict(data)

    scenarios = [
        ("single_key", ["id1"]),
        ("dual_key", ["id1", "id2"]),
        ("triple_key", ["id1", "id2", "id3"]),
    ]

    results = {}

    for scenario_name, key_columns in scenarios:
        print(f"\nTesting {scenario_name}: {key_columns}")

        monitor = PerformanceMonitor(max_pyarrow_mb=512)
        monitor.start_op("deduplication")

        start_time = time.time()

        try:
            result = deduplicate_pyarrow(
                table=test_table,
                key_columns=key_columns,
                dedup_order_by=["timestamp"],
                keep="first",
            )

            end_time = time.time()
            monitor.end_op()

            metrics = monitor.get_metrics(
                total_rows_before=test_table.num_rows,
                total_rows_after=result.num_rows,
                total_bytes=test_table.nbytes,
            )

            results[scenario_name] = {
                "duration": end_time - start_time,
                "rows_before": test_table.num_rows,
                "rows_after": result.num_rows,
                "memory_mb": metrics["memory_peak_mb"],
                "throughput": metrics["rows_per_sec"],
            }

            print(f"  Duration: {end_time - start_time:.2f}s")
            print(f"  Rows removed: {test_table.num_rows - result.num_rows}")
            print(f"  Memory: {metrics['memory_peak_mb']:.1f} MB")
            print(f"  Throughput: {metrics['rows_per_sec']:.0f} rows/sec")

        except Exception as e:
            print(f"  Failed: {e}")
            results[scenario_name] = {"error": str(e)}

    # Summary
    print(f"\n=== Performance Summary ===")
    baseline = results.get("single_key", {}).get("duration", 1.0)
    for scenario, metrics in results.items():
        if "error" not in metrics:
            speedup = baseline / metrics["duration"] if baseline > 0 else 1.0
            print(
                f"{scenario:12}: {metrics['duration']:6.2f}s, "
                f"{metrics['throughput']:8.0f} rows/sec, "
                f"{speedup:5.1f}x vs single-key"
            )
    print()


def demo_mixed_type_keys():
    """Demonstrate handling of mixed data types in composite keys."""
    print("=== Mixed Type Key Handling ===")

    # Table with mixed types
    mixed_data = {
        "tenant_id": [1, 1, 2, 2, 3],  # int64
        "record_id": ["A001", "A002", "B001", "B002", "C001"],  # string
        "event_timestamp": [
            1704067200,
            1704067260,
            1704067320,
            1704067380,
            1704067440,
        ],  # timestamp (int64)
        "status_code": [200, 404, 200, 200, 500],  # int32
        "data": ["value1", "value2", "value3", "value4", "value5"],
    }

    table = pa.Table.from_pydict(mixed_data)
    print(f"Mixed type data: {table.num_rows} rows")
    print("Schema:")
    print(table.schema)

    try:
        # This will use vectorized approach for compatible types,
        # fallback for complex combos
        unique_records = deduplicate_pyarrow(
            table=table,
            key_columns=["tenant_id", "record_id", "event_timestamp"],
            dedup_order_by=["event_timestamp"],
            keep="first",
        )

        print(
            f"\nSuccessfully processed mixed-type keys: {unique_records.num_rows} unique records"
        )
        print("Results:")
        print(unique_records.to_pandas())

    except Exception as e:
        print(f"Error with mixed types: {e}")

    print()


def main():
    """Run all multi-key vectorization demonstrations."""
    print("PyArrow Multi-Key Vectorization Demo")
    print("====================================\n")

    try:
        demo_basic_composite_key_deduplication()
        demo_composite_key_merge()
        demo_performance_comparison()
        demo_mixed_type_keys()

        print("✅ All demos completed successfully!")
        print("\nFor more examples and documentation:")
        print("- Multi-Key Usage Examples: docs/how-to/multi-key-examples.md")
        print("- Performance Guide: docs/how-to/multi-key-performance.md")
        print("- API Reference: docs/reference/multi-key-api.md")

    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure fsspeckit with PyArrow support is installed:")
        print("pip install 'fsspeckit[datasets]'")
    except Exception as e:
        print(f"❌ Error running demos: {e}")
        raise


if __name__ == "__main__":
    main()
