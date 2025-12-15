#!/usr/bin/env python3
"""
Dataset Deduplication Maintenance Example

This example demonstrates the new dataset deduplication maintenance API,
showing how to deduplicate existing parquet datasets both independently
and as part of optimization workflows.

Key features demonstrated:
1. Key-based deduplication with custom ordering
2. Exact duplicate removal
3. Integration with dataset optimization
4. Dry-run mode for planning
"""

import tempfile
from pathlib import Path

# Example data with duplicates for demonstration
SAMPLE_DATA_WITH_DUPLICATES = [
    # Some unique records
    {"id": 1, "name": "Alice", "timestamp": "2024-01-01", "value": 100},
    {"id": 2, "name": "Bob", "timestamp": "2024-01-02", "value": 200},
    # Duplicate records with different timestamps (keep most recent)
    {
        "id": 1,
        "name": "Alice",
        "timestamp": "2024-01-03",
        "value": 150,
    },  # Should keep this
    {
        "id": 1,
        "name": "Alice",
        "timestamp": "2024-01-02",
        "value": 120,
    },  # Should remove this
    # More unique records
    {"id": 3, "name": "Charlie", "timestamp": "2024-01-01", "value": 300},
    # Exact duplicates (same in all columns)
    {"id": 4, "name": "David", "timestamp": "2024-01-01", "value": 400},
    {
        "id": 4,
        "name": "David",
        "timestamp": "2024-01-01",
        "value": 400,
    },  # Exact duplicate
]


def create_sample_dataset(dataset_path: str):
    """Create a sample parquet dataset with duplicates for testing."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Convert sample data to PyArrow table
    table = pa.Table.from_pylist(SAMPLE_DATA_WITH_DUPLICATES)

    # Write to dataset directory
    pq.write_table(table, dataset_path)

    print(f"✓ Created sample dataset at: {dataset_path}")
    print(f"  Original records: {table.num_rows}")


def demonstrate_key_based_deduplication(dataset_path: str):
    """Demonstrate key-based deduplication with custom ordering."""
    print("\n🗝️  Key-Based Deduplication Example")
    print("=" * 50)

    try:
        # Import the deduplication function
        from fsspeckit.datasets.pyarrow.dataset import (
            deduplicate_parquet_dataset_pyarrow,
        )

        print("\n1. First, let's see the plan with dry_run=True:")
        plan = deduplicate_parquet_dataset_pyarrow(
            path=dataset_path,
            key_columns=["id"],  # Deduplicate based on 'id' column
            dedup_order_by=["-timestamp"],  # Keep most recent record
            dry_run=True,
            verbose=True,
        )

        print(
            f"   Plan: {plan['before_file_count']} files -> {plan['after_file_count']} files"
        )
        print(f"   Key columns: {plan.get('key_columns', 'None')}")
        print(f"   Order by: {plan.get('dedup_order_by', 'None')}")

        print("\n2. Now let's execute the deduplication:")
        result = deduplicate_parquet_dataset_pyarrow(
            path=dataset_path,
            key_columns=["id"],
            dedup_order_by=["-timestamp"],
            verbose=True,
        )

        print(f"   ✓ Deduplication complete!")
        print(
            f"   Files: {result['before_file_count']} -> {result['after_file_count']}"
        )
        print(f"   Rows deduplicated: {result.get('deduplicated_rows', 0)}")

    except ImportError as e:
        print(f"   ⚠️  PyArrow backend not available: {e}")


def demonstrate_exact_duplicate_removal(dataset_path: str):
    """Demonstrate exact duplicate removal (no key columns)."""
    print("\n🔍 Exact Duplicate Removal Example")
    print("=" * 50)

    try:
        from fsspeckit.datasets.pyarrow.dataset import (
            deduplicate_parquet_dataset_pyarrow,
        )

        print("\nRemoving exact duplicates across all columns...")
        result = deduplicate_parquet_dataset_pyarrow(
            path=dataset_path,
            # No key_columns = exact duplicate removal
            verbose=True,
        )

        print(f"   ✓ Exact deduplication complete!")
        print(
            f"   Files: {result['before_file_count']} -> {result['after_file_count']}"
        )
        print(f"   Rows deduplicated: {result.get('deduplicated_rows', 0)}")

    except ImportError as e:
        print(f"   ⚠️  PyArrow backend not available: {e}")


def demonstrate_optimization_with_deduplication(dataset_path: str):
    """Demonstrate optimization that includes deduplication."""
    print("\n🚀 Optimization with Deduplication Example")
    print("=" * 50)

    try:
        from fsspeckit.datasets.pyarrow.dataset import optimize_parquet_dataset_pyarrow

        print("\nOptimizing dataset with deduplication...")
        result = optimize_parquet_dataset_pyarrow(
            path=dataset_path,
            target_mb_per_file=1,  # Target file size
            deduplicate_key_columns=["id"],  # Deduplicate before optimization
            dedup_order_by=["-timestamp"],
            verbose=True,
        )

        print(f"   ✓ Optimization with deduplication complete!")
        print(
            f"   Files: {result['before_file_count']} -> {result['after_file_count']}"
        )
        print(
            f"   Size: {result['before_total_bytes']} -> {result['after_total_bytes']} bytes"
        )

    except ImportError as e:
        print(f"   ⚠️  PyArrow backend not available: {e}")


def demonstrate_filesystem_level_api(dataset_path: str):
    """Demonstrate the filesystem-level deduplication API."""
    print("\n💾 Filesystem-Level API Example")
    print("=" * 50)

    try:
        from fsspec import LocalFileSystem

        fs = LocalFileSystem()

        print("\nUsing filesystem-level deduplication API...")
        result = fs.deduplicate_parquet_dataset(
            path=dataset_path,
            key_columns=["id", "name"],  # Multi-column deduplication
            verbose=True,
        )

        print(f"   ✓ Filesystem-level deduplication complete!")
        print(
            f"   Files: {result['before_file_count']} -> {result['after_file_count']}"
        )
        print(f"   Rows deduplicated: {result.get('deduplicated_rows', 0)}")

    except ImportError as e:
        print(f"   ⚠️  Filesystem API not available: {e}")


def main():
    """Main demonstration function."""
    print("📚 Dataset Deduplication Maintenance API Examples")
    print("=" * 60)

    # Create a temporary dataset for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "sample_dataset.parquet"

        try:
            # Create sample dataset
            create_sample_dataset(str(dataset_path))

            # Demonstrate different deduplication approaches
            demonstrate_key_based_deduplication(str(dataset_path))

            # Reset dataset for next example
            create_sample_dataset(str(dataset_path))
            demonstrate_exact_duplicate_removal(str(dataset_path))

            # Reset dataset for optimization example
            create_sample_dataset(str(dataset_path))
            demonstrate_optimization_with_deduplication(str(dataset_path))

            # Reset dataset for filesystem API example
            create_sample_dataset(str(dataset_path))
            demonstrate_filesystem_level_api(str(dataset_path))

            print("\n✅ All examples completed successfully!")
            print("\n📖 Key Takeaways:")
            print("   • Use key_columns for targeted deduplication")
            print("   • Use dedup_order_by to control which records are kept")
            print("   • Omit key_columns for exact duplicate removal")
            print("   • Use dry_run=True to plan before executing")
            print(
                "   • Integration with optimize_parquet_dataset for comprehensive maintenance"
            )

        except Exception as e:
            print(f"❌ Error during demonstration: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    main()
