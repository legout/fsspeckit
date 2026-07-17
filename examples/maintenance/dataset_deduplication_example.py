#!/usr/bin/env python3
"""
Dataset Deduplication Maintenance Example

This example demonstrates the coordinator-backed dataset deduplication API
(introduced in 0.24.0 / 0.25.0), showing how to deduplicate existing parquet
datasets both independently and as part of optimization workflows.

Key features demonstrated:
1. Key-based deduplication with ordering (plan-then-execute)
2. Exact duplicate removal (no key columns)
3. One-call optimization with integrated deduplication
4. Multi-column keys

The maintenance facades are registered on every fsspec ``AbstractFileSystem``
when ``fsspeckit`` is imported. All operations return a typed
``MaintenanceResult`` instead of a dictionary, and the removed ``dry_run``
mode is replaced by creating a plan and inspecting it before executing.

Ordering semantics: ``dedup_order_by`` sorts ascending and keeps the FIRST
row per key (ties are broken by original row order). The legacy ``-column``
descending prefix from the pre-0.25 helpers is not supported - see
``docs/migration/maintenance-api.md``.
"""

import tempfile
from pathlib import Path

import fsspec

import fsspeckit  # noqa: F401  (registers the filesystem maintenance facades)

# Example data with duplicates for demonstration
SAMPLE_DATA_WITH_DUPLICATES = [
    # Some unique records
    {"id": 1, "name": "Alice", "timestamp": "2024-01-01", "value": 100},
    {"id": 2, "name": "Bob", "timestamp": "2024-01-02", "value": 200},
    # Duplicate records with different timestamps
    {"id": 1, "name": "Alice", "timestamp": "2024-01-03", "value": 150},
    {"id": 1, "name": "Alice", "timestamp": "2024-01-02", "value": 120},
    # More unique records
    {"id": 3, "name": "Charlie", "timestamp": "2024-01-01", "value": 300},
    # Exact duplicates (same in all columns)
    {"id": 4, "name": "David", "timestamp": "2024-01-01", "value": 400},
    {"id": 4, "name": "David", "timestamp": "2024-01-01", "value": 400},
]


def create_sample_dataset(dataset_path: str):
    """Create a sample parquet dataset with duplicates for testing."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(SAMPLE_DATA_WITH_DUPLICATES)

    # Write two files so this is a real multi-file dataset
    root = Path(dataset_path)
    root.mkdir(parents=True, exist_ok=True)
    for stale in root.glob("*.parquet"):
        stale.unlink()
    half = table.num_rows // 2
    pq.write_table(table.slice(0, half), root / "batch_0.parquet")
    pq.write_table(table.slice(half), root / "batch_1.parquet")

    print(f"✓ Created sample dataset at: {dataset_path}")
    print(f"  Original records: {table.num_rows} (2 files)")


def read_dataset_rows(dataset_path: str) -> list[dict]:
    """Read the full dataset back as a list of row dicts."""
    import pyarrow.dataset as pds

    rows: list[dict] = pds.dataset(dataset_path).to_table().to_pylist()
    return rows


def demonstrate_key_based_deduplication(dataset_path: str):
    """Demonstrate key-based deduplication with plan-then-execute."""
    print("\n🗝️  Key-Based Deduplication Example")
    print("=" * 50)

    fs = fsspec.filesystem("file")

    print("\n1. Create a plan and inspect it (replaces dry_run=True):")
    plan = fs.plan_parquet_partition_local_deduplication(
        dataset_path,
        key_columns=["id"],  # Deduplicate based on 'id' column
        dedup_order_by=["timestamp"],  # Ascending: earliest row per id wins
    )
    print(f"   Plan type: {type(plan).__name__}")
    print(f"   Guarantee level: {plan.guarantee_level.value}")
    print(f"   Source files: {len(plan.source_snapshot.files)}")
    print(f"   Key columns: {plan.dedup_key_columns}")
    print(f"   Order by: {plan.dedup_order_by}")

    print("\n2. Execute the plan:")
    result = fs.execute_maintenance_plan(plan)
    print(f"   ✓ Succeeded: {result.succeeded}")
    if result.actual_metrics is not None:
        print(
            f"   Files: {len(plan.source_snapshot.files)} -> "
            f"{result.actual_metrics.file_count}"
        )
        print(
            f"   Rows: {plan.source_snapshot.total_rows} -> "
            f"{result.actual_metrics.row_count}"
        )

    rows = read_dataset_rows(dataset_path)
    kept = sorted((r["id"], r["timestamp"]) for r in rows)
    print(f"   Kept (id, timestamp): {kept}")


def demonstrate_exact_duplicate_removal(dataset_path: str):
    """Demonstrate exact duplicate removal (no key columns)."""
    print("\n🔍 Exact Duplicate Removal Example")
    print("=" * 50)

    fs = fsspec.filesystem("file")

    print("\nRemoving exact duplicates across all columns (one call)...")
    result = fs.deduplicate_parquet_dataset(dataset_path)

    print(f"   ✓ Succeeded: {result.succeeded}")
    if result.actual_metrics is not None:
        print(
            f"   Rows: {result.plan.source_snapshot.total_rows} -> "
            f"{result.actual_metrics.row_count}"
        )


def demonstrate_optimization_with_deduplication(dataset_path: str):
    """Demonstrate optimization that includes deduplication."""
    print("\n🚀 Optimization with Deduplication Example")
    print("=" * 50)

    fs = fsspec.filesystem("file")

    print("\nOptimizing dataset with deduplication (one call)...")
    result = fs.optimize_parquet_dataset(
        dataset_path,
        deduplicate_key_columns=["id"],  # Deduplicate before compaction
        target_mb_per_file=1,  # Target file size
    )

    print(f"   ✓ Succeeded: {result.succeeded}")
    if result.actual_metrics is not None:
        print(
            f"   Files: {result.plan.source_snapshot.total_rows} rows -> "
            f"{result.actual_metrics.row_count} rows in "
            f"{result.actual_metrics.file_count} file(s) "
            f"({result.actual_metrics.total_bytes} bytes)"
        )


def demonstrate_multi_column_key(dataset_path: str):
    """Demonstrate deduplication on a multi-column key."""
    print("\n💾 Multi-Column Key Example")
    print("=" * 50)

    fs = fsspec.filesystem("file")

    print("\nDeduplicating on key_columns=['id', 'name']...")
    result = fs.deduplicate_parquet_dataset(
        dataset_path,
        key_columns=["id", "name"],
    )

    print(f"   ✓ Succeeded: {result.succeeded}")
    if result.actual_metrics is not None:
        print(
            f"   Rows: {result.plan.source_snapshot.total_rows} -> "
            f"{result.actual_metrics.row_count}"
        )


def main():
    """Main demonstration function."""
    print("📚 Dataset Deduplication Maintenance API Examples")
    print("=" * 60)

    # Create a temporary dataset for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "sample_dataset"

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

            # Reset dataset for multi-column key example
            create_sample_dataset(str(dataset_path))
            demonstrate_multi_column_key(str(dataset_path))

            print("\n✅ All examples completed successfully!")
            print("\n📖 Key Takeaways:")
            print("   • Use key_columns for targeted deduplication")
            print("   • dedup_order_by sorts ascending; the first row per key wins")
            print("   • Omit key_columns for exact duplicate removal")
            print("   • Create a plan first to inspect what would happen")
            print("     (this replaces the removed dry_run=True mode)")
            print(
                "   • Use optimize_parquet_dataset for dedup + compaction in one call"
            )

        except Exception as e:
            print(f"❌ Error during demonstration: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    main()
