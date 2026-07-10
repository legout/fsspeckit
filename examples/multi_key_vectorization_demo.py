"""
PyArrow Multi-Key Vectorization Examples

This script demonstrates the persisted-dataset composite-key workflow
in fsspeckit, providing examples of deduplication and merge operations
with composite keys.

Run with: python examples/multi_key_vectorization_demo.py
"""

import os
import tempfile
import time

import pyarrow as pa

from fsspeckit.datasets.pyarrow import (
    PyarrowDatasetIO,
    deduplicate_parquet_dataset_pyarrow,
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

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = os.path.join(temp_dir, "dedup_demo")

        io = PyarrowDatasetIO()
        io.write_dataset(table, dataset_path, mode="overwrite")

        stats = deduplicate_parquet_dataset_pyarrow(
            path=dataset_path,
            key_columns=["tenant_id", "user_id", "record_id"],
            dedup_order_by=["timestamp"],
        )

        unique_table = io.read_parquet(dataset_path)

    print(f"\nAfter deduplication: {unique_table.num_rows} rows")
    print(f"Removed {stats['deduplicated_rows']} duplicate rows")
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

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = os.path.join(temp_dir, "orders")

        io = PyarrowDatasetIO()
        io.write_dataset(existing_table, dataset_path, mode="overwrite")

        result = io.merge(
            data=new_table,
            path=dataset_path,
            strategy="upsert",
            key_columns=["tenant_id", "customer_id", "order_id"],
        )

        print(f"\nMerge Statistics:")
        print(f"  Source rows: {result.source_count}")
        print(f"  Target rows before: {result.target_count_before}")
        print(f"  Target rows after: {result.target_count_after}")
        print(f"  Inserted rows: {result.inserted}")
        print(f"  Updated rows: {result.updated}")
    print()


def demo_performance_comparison():
    """Demonstrate performance comparison between single and multi-column keys."""
    print("=== Performance Comparison ===")

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

    with tempfile.TemporaryDirectory() as temp_dir:
        for scenario_name, key_columns in scenarios:
            print(f"\nTesting {scenario_name}: {key_columns}")

            dataset_path = os.path.join(temp_dir, scenario_name)
            io = PyarrowDatasetIO()
            io.write_dataset(test_table, dataset_path, mode="overwrite")

            start_time = time.time()

            stats = deduplicate_parquet_dataset_pyarrow(
                path=dataset_path,
                key_columns=key_columns,
                dedup_order_by=["timestamp"],
            )

            end_time = time.time()
            duration = end_time - start_time

            result_table = io.read_parquet(dataset_path)
            rows_removed = stats["deduplicated_rows"]

            results[scenario_name] = {
                "duration": duration,
                "rows_before": test_table.num_rows,
                "rows_after": result_table.num_rows,
                "rows_removed": rows_removed,
                "throughput": test_table.num_rows / duration if duration > 0 else 0.0,
            }

            print(f"  Duration: {duration:.2f}s")
            print(f"  Rows removed: {rows_removed}")
            print(f"  Throughput: {results[scenario_name]['throughput']:.0f} rows/sec")

    # Summary
    print("\n=== Performance Summary ===")
    baseline = results.get("single_key", {}).get("duration", 1.0)
    for scenario, metrics in results.items():
        speedup = baseline / metrics["duration"] if metrics["duration"] > 0 else 1.0
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

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = os.path.join(temp_dir, "mixed_keys")
        io = PyarrowDatasetIO()
        io.write_dataset(table, dataset_path, mode="overwrite")

        try:
            stats = deduplicate_parquet_dataset_pyarrow(
                path=dataset_path,
                key_columns=["tenant_id", "record_id", "event_timestamp"],
                dedup_order_by=["event_timestamp"],
            )

            unique_records = io.read_parquet(dataset_path)

            print(
                f"\nSuccessfully processed mixed-type keys: "
                f"{unique_records.num_rows} unique records "
                f"({stats['deduplicated_rows']} removed)"
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

        print("All demos completed successfully!")
        print("\nFor more examples and documentation:")
        print("- Multi-Key Usage Examples: docs/how-to/multi-key-examples.md")
        print("- Performance Guide: docs/how-to/multi-key-performance.md")
        print("- API Reference: docs/reference/multi-key-api.md")

    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure fsspeckit with PyArrow support is installed:")
        print("pip install 'fsspeckit[datasets]'")
    except Exception as e:
        print(f"Error running demos: {e}")
        raise


if __name__ == "__main__":
    main()
