"""
PyArrow Basics - Dataset Optimization

This beginner-friendly example introduces PyArrow dataset optimization techniques
using fsspeckit's PyArrow utilities for high-performance data operations.

The example covers:
1. Basic PyArrow table creation and manipulation
2. Dataset optimization with PyArrow
3. Data compaction strategies
4. Performance comparison between optimized and unoptimized data
5. Memory-efficient data processing patterns

This example complements the DuckDB basics by showing an alternative
approach that doesn't require a database engine.
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency 'pyarrow'. Install with: pip install -e \".[datasets]\" "
        "(or run `uv sync` then `uv run python ...`)."
    ) from exc

try:
    from fsspeckit.datasets import (
        optimize_parquet_dataset_pyarrow,
        compact_parquet_dataset_pyarrow,
    )
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing fsspeckit dataset dependencies. Install with: pip install -e \".[datasets]\" "
        "(or run `uv sync` then `uv run python ...`)."
    ) from exc


def create_sample_inventory_data() -> pa.Table:
    """Create sample inventory data for optimization demonstration."""

    print("📦 Creating sample inventory data...")

    # Generate realistic inventory data with patterns that benefit from optimization
    import random
    from datetime import datetime, timedelta

    products = [
        "Laptop",
        "Mouse",
        "Keyboard",
        "Monitor",
        "Headphones",
        "Webcam",
        "USB Hub",
        "External SSD",
        "Docking Station",
        "Cable Kit",
        "Router",
        "Switch",
        "Access Point",
        "Network Cable",
        "Power Strip",
    ]

    categories = ["Electronics", "Accessories", "Peripherals", "Networking", "Power"]
    suppliers = ["TechCorp", "InnoTech", "DataSystems", "NetPro", "PowerHouse"]
    statuses = ["in_stock", "out_of_stock", "discontinued", "backorder"]

    records = []
    base_date = datetime(2024, 1, 1)

    for i in range(200):  # 200 records for meaningful optimization testing
        record = {
            "product_id": f"PROD-{i:04d}",
            "product_name": random.choice(products),
            "category": random.choice(categories),
            "supplier": random.choice(suppliers),
            "quantity": random.randint(0, 1000),
            "unit_price": round(random.uniform(10.0, 500.0), 2),
            "reorder_level": random.randint(10, 100),
            "last_stock_date": (
                base_date + timedelta(days=random.randint(0, 180))
            ).strftime("%Y-%m-%d"),
            "status": random.choice(statuses),
            "weight": round(random.uniform(0.1, 10.0), 2),
            "location": f"WH-{random.choice(['A', 'B', 'C'])}-{random.randint(1, 20):02d}",
            "created_timestamp": (
                base_date + timedelta(hours=random.randint(0, 4320))
            ).isoformat(),
        }
        records.append(record)

    table = pa.Table.from_pylist(records)
    print(f"Created inventory dataset with {len(table)} records")
    return table


def demonstrate_basic_pyarrow_operations():
    """Demonstrate fundamental PyArrow table operations."""

    print("\n🔹 Basic PyArrow Table Operations")

    # Create sample data
    inventory_data = create_sample_inventory_data()

    print(f"\n📊 Dataset Overview:")
    print(f"  Records: {len(inventory_data):,}")
    print(f"  Columns: {len(inventory_data.schema)}")
    print(f"  Memory: {inventory_data.nbytes / 1024 / 1024:.2f} MB")

    # Display schema
    print(f"\n📋 Table Schema:")
    for i, field in enumerate(inventory_data.schema, 1):
        print(f"  {i:2d}. {field.name:<20} {field.type}")

    # Basic filtering and selection
    print(f"\n🔍 Basic Filtering Examples:")

    # Example 1: Filter by quantity
    low_stock = inventory_data.filter(pc.less(inventory_data.column("quantity"), 50))
    print(f"  Low stock items (< 50): {len(low_stock)} records")

    # Example 2: Filter by category and status
    electronics_in_stock = inventory_data.filter(
        pc.and_(
            pc.equal(inventory_data.column("category"), "Electronics"),
            pc.equal(inventory_data.column("status"), "in_stock"),
        )
    )
    print(f"  Electronics in stock: {len(electronics_in_stock)} records")

    # Example 3: Select specific columns
    basic_info = inventory_data.select(
        ["product_id", "product_name", "quantity", "unit_price"]
    )
    print(
        f"  Basic info columns: {len(basic_info.schema)} fields, {basic_info.nbytes / 1024:.1f} KB"
    )

    # Basic aggregation
    print(f"\n📈 Basic Aggregations:")

    # Count by category
    categories = inventory_data.column("category")
    unique_categories = pc.unique(categories)
    print(f"  Unique categories: {len(unique_categories)}")

    # Calculate statistics
    quantity_col = inventory_data.column("quantity")
    stats = {
        "total_items": pc.sum(quantity_col).as_py(),
        "avg_quantity": pc.mean(quantity_col).as_py(),
        "max_quantity": pc.max(quantity_col).as_py(),
        "min_quantity": pc.min(quantity_col).as_py(),
    }

    print(f"  Quantity statistics:")
    for key, value in stats.items():
        print(
            f"    {key}: {value:.2f}"
            if isinstance(value, float)
            else f"    {key}: {value}"
        )

    return inventory_data


def create_unoptimized_dataset(data: pa.Table, output_path: Path) -> Path:
    """Create an unoptimized dataset for comparison."""

    print(f"\n📝 Creating unoptimized dataset...")

    # Simulate poor organization by creating many small files
    chunk_size = 50
    chunks = []

    for i in range(0, len(data), chunk_size):
        chunk = data.slice(i, min(chunk_size, len(data) - i))
        chunk_file = output_path / f"chunk_{i // chunk_size:03d}.parquet"
        pq.write_table(chunk, chunk_file)
        chunks.append(chunk_file)

    print(f"  Created {len(chunks)} small files")
    total_size = sum(f.stat().st_size for f in chunks)
    print(f"  Total size: {total_size / 1024 / 1024:.2f} MB")

    return output_path


def demonstrate_dataset_optimization():
    """Demonstrate PyArrow dataset optimization."""

    print("\n⚡ Dataset Optimization with PyArrow")

    temp_dir = Path(tempfile.mkdtemp())
    dataset_path = temp_dir / "inventory_dataset"
    dataset_path.mkdir(parents=True)

    try:
        # Create unoptimized dataset
        inventory_data = create_sample_inventory_data()
        unoptimized_path = dataset_path / "unoptimized"
        unoptimized_path.mkdir()
        create_unoptimized_dataset(inventory_data, unoptimized_path)

        # Create well-organized dataset for comparison
        optimized_path = dataset_path / "well_organized"
        optimized_path.mkdir()
        pq.write_table(inventory_data, optimized_path / "inventory.parquet")

        print(f"\n📊 Dataset Comparison:")
        print(f"  Unoptimized: {len(list(unoptimized_path.glob('*.parquet')))} files")
        print(f"  Organized:   {len(list(optimized_path.glob('*.parquet')))} files")

        # Measure read performance
        print(f"\n⏱️  Performance Comparison:")

        # Read unoptimized dataset
        start_time = time.time()
        unoptimized_files = list(unoptimized_path.glob("*.parquet"))
        unoptimized_tables = [pq.read_table(f) for f in unoptimized_files]
        unoptimized_combined = pa.concat_tables(unoptimized_tables)
        unoptimized_time = time.time() - start_time

        # Read organized dataset
        start_time = time.time()
        optimized_table = pq.read_table(optimized_path / "inventory.parquet")
        optimized_time = time.time() - start_time

        print(f"  Unoptimized read: {unoptimized_time:.4f} seconds")
        print(f"  Organized read:   {optimized_time:.4f} seconds")
        print(f"  Performance gain: {unoptimized_time / optimized_time:.2f}x")

        # Verify data integrity
        if len(unoptimized_combined) == len(optimized_table):
            print(f"  ✅ Data integrity verified: {len(unoptimized_combined)} records")
        else:
            print(
                f"  ❌ Data mismatch: {len(unoptimized_combined)} vs {len(optimized_table)}"
            )

        return dataset_path, inventory_data

    except Exception as e:
        print(f"❌ Optimization demo failed: {e}")
        raise


def demonstrate_pyarrow_compaction():
    """Demonstrate PyArrow dataset compaction."""

    print("\n🔧 Dataset Compaction with PyArrow")

    temp_dir = Path(tempfile.mkdtemp())
    dataset_path = temp_dir / "compact_test"
    dataset_path.mkdir()

    try:
        # Create a fragmented dataset (simulating real-world scenario)
        inventory_data = create_sample_inventory_data()

        # Write data in small, unorganized chunks
        print("Creating fragmented dataset...")
        for i in range(0, len(inventory_data), 25):  # Very small chunks
            chunk = inventory_data.slice(i, min(25, len(inventory_data) - i))
            chunk_file = dataset_path / f"data_part_{i // 25:03d}.parquet"
            pq.write_table(chunk, chunk_file)

        original_files = list(dataset_path.glob("*.parquet"))
        original_size = sum(f.stat().st_size for f in original_files)

        print(
            f"  Created fragmented dataset: {len(original_files)} files, {original_size / 1024:.1f} KB"
        )

        # Apply compaction
        print("\nApplying dataset compaction...")

        try:
            start_time = time.time()
            compact_parquet_dataset_pyarrow(str(dataset_path), target_mb_per_file=1)
            compaction_time = time.time() - start_time
        except Exception as e:
            print(f"  ⚠️  Compaction skipped due to: {e}")
            print("  ✅ Dataset is still functional for demonstration purposes")
            compaction_time = 0

        # Check results
        compacted_files = list(dataset_path.glob("*.parquet"))
        compacted_size = sum(f.stat().st_size for f in compacted_files)

        print(f"  Compaction time: {compaction_time:.3f} seconds")
        print(f"  Files after compaction: {len(compacted_files)}")
        print(f"  Size after compaction: {compacted_size / 1024:.1f} KB")

        if compacted_size < original_size:
            size_reduction = (1 - compacted_size / original_size) * 100
            print(f"  Size reduction: {size_reduction:.1f}%")
        else:
            print(
                f"  Size change: {((compacted_size / original_size) - 1) * 100:.1f}% (may be due to compression)"
            )

        # Verify data integrity
        print("\n🔍 Verifying data integrity after compaction...")

        # Read compacted data
        compacted_dataset = ds.dataset(dataset_path, format="parquet")
        compacted_table = compacted_dataset.to_table()

        if len(compacted_table) == len(inventory_data):
            print(f"  ✅ Data integrity verified: {len(compacted_table)} records")
        else:
            print(
                f"  ❌ Data mismatch: {len(compacted_table)} vs {len(inventory_data)}"
            )

    except Exception as e:
        print(f"❌ Compaction demo failed: {e}")
        raise

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_pyarrow_optimization():
    """Demonstrate PyArrow dataset optimization with Z-ordering."""

    print("\n🎯 Dataset Optimization")

    temp_dir = Path(tempfile.mkdtemp())
    dataset_path = temp_dir / "zorder_test"
    dataset_path.mkdir()

    try:
        # Create sample data with clear optimization opportunities
        inventory_data = create_sample_inventory_data()

        # Write as unoptimized dataset
        print("Creating unoptimized dataset...")
        unoptimized_path = dataset_path / "unoptimized"
        unoptimized_path.mkdir()

        # Write data in a way that doesn't benefit from query optimization
        for i in range(0, len(inventory_data), 100):
            chunk = inventory_data.slice(i, min(100, len(inventory_data) - i))
            chunk_file = unoptimized_path / f"chunk_{i // 100:03d}.parquet"
            pq.write_table(chunk, chunk_file)

        print(
            f"  Unoptimized dataset: {len(list(unoptimized_path.glob('*.parquet')))} files"
        )

        # Apply optimization through compaction
        print("\nApplying dataset optimization...")

        try:
            start_time = time.time()
            optimize_parquet_dataset_pyarrow(
                str(unoptimized_path), target_mb_per_file=2
            )
            optimization_time = time.time() - start_time
            print(f"  Optimization time: {optimization_time:.3f} seconds")
        except Exception as e:
            print(f"  ⚠️  Optimization skipped due to: {e}")
            print("  ✅ Dataset is still functional for demonstration purposes")
            optimization_time = 0

        # Test query performance improvement
        print("\n🏃 Testing query performance...")

        # Create dataset from optimized files
        optimized_dataset = ds.dataset(unoptimized_path, format="parquet")
        optimized_table = optimized_dataset.to_table()

        # Query 1: Filter by category
        start_time = time.time()
        electronics_result = optimized_table.filter(
            pc.equal(optimized_table.column("category"), "Electronics")
        )
        query1_time = time.time() - start_time

        print(
            f"  Query 1 (category filter): {query1_time:.4f}s, {len(electronics_result)} results"
        )

        # Query 2: Filter by status and quantity
        start_time = time.time()
        status_quantity_result = optimized_table.filter(
            pc.and_(
                pc.equal(optimized_table.column("status"), "in_stock"),
                pc.greater(optimized_table.column("quantity"), 100),
            )
        )
        query2_time = time.time() - start_time

        print(
            f"  Query 2 (status + quantity): {query2_time:.4f}s, {len(status_quantity_result)} results"
        )

        # Query 3: Complex query with multiple filters
        start_time = time.time()
        complex_result = optimized_table.filter(
            pc.and_(
                pc.equal(optimized_table.column("category"), "Electronics"),
                pc.and_(
                    pc.greater_equal(optimized_table.column("unit_price"), 100),
                    pc.less(optimized_table.column("quantity"), 500),
                ),
            )
        )
        query3_time = time.time() - start_time

        print(
            f"  Query 3 (complex filter): {query3_time:.4f}s, {len(complex_result)} results"
        )

        total_query_time = query1_time + query2_time + query3_time
        print(f"  Total query time: {total_query_time:.4f} seconds")

    except Exception as e:
        print(f"❌ Optimization demo failed: {e}")
        raise

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_memory_efficiency():
    """Demonstrate memory-efficient PyArrow operations."""

    print("\n💾 Memory-Efficient PyArrow Operations")

    # Create a larger dataset to show memory benefits
    print("Creating larger dataset for memory testing...")
    large_inventory = create_sample_inventory_data()

    # Replicate to make it larger
    large_inventory = pa.concat_tables([large_inventory] * 10)  # ~2,000 records

    print(f"Large dataset: {len(large_inventory):,} records")
    print(f"Memory usage: {large_inventory.nbytes / 1024 / 1024:.2f} MB")

    # Memory-efficient operation 1: Column projection
    print(f"\n🎯 Column Projection:")
    start_time = time.time()

    # Select only needed columns
    essential_columns = large_inventory.select(
        ["product_id", "product_name", "quantity", "status"]
    )

    projection_time = time.time() - start_time
    memory_savings = (large_inventory.nbytes - essential_columns.nbytes) / 1024 / 1024

    print(f"  Selected 4/12 columns")
    print(f"  Memory reduction: {memory_savings:.2f} MB")
    print(f"  Projection time: {projection_time:.4f} seconds")

    # Memory-efficient operation 2: Early filtering
    print(f"\n🔍 Early Filtering:")
    start_time = time.time()

    # Filter early to reduce data size
    in_stock_items = large_inventory.filter(
        pc.equal(large_inventory.column("status"), "in_stock")
    )

    filtering_time = time.time() - start_time
    reduction_percent = (1 - len(in_stock_items) / len(large_inventory)) * 100

    print(f"  Filtered to in-stock items")
    print(f"  Data reduction: {reduction_percent:.1f}%")
    print(f"  Filtering time: {filtering_time:.4f} seconds")

    # Memory-efficient operation 3: Chunked processing
    print(f"\n📦 Chunked Processing:")
    chunk_size = 1000
    processed_chunks = []

    start_time = time.time()
    for i in range(0, len(large_inventory), chunk_size):
        chunk = large_inventory.slice(i, min(chunk_size, len(large_inventory) - i))

        # Process chunk (e.g., filter and aggregate)
        chunk_filtered = chunk.filter(pc.greater(chunk.column("quantity"), 100))
        processed_chunks.append(chunk_filtered)

    chunked_time = time.time() - start_time
    total_processed = sum(len(chunk) for chunk in processed_chunks)

    print(f"  Processed {len(processed_chunks)} chunks of {chunk_size:,} records")
    print(f"  Total filtered items: {total_processed:,}")
    print(f"  Chunked processing time: {chunked_time:.4f} seconds")


def main():
    """Run all PyArrow basics examples."""

    print("➡️ PyArrow Basics - Dataset Optimization")
    print("=" * 60)
    print("This example introduces PyArrow dataset optimization techniques")
    print("for high-performance data processing without requiring a database.")

    try:
        # Run all demonstrations
        inventory_data = demonstrate_basic_pyarrow_operations()
        dataset_path, _ = demonstrate_dataset_optimization()
        demonstrate_pyarrow_compaction()
        demonstrate_pyarrow_optimization()
        demonstrate_memory_efficiency()

        print("\n" + "=" * 60)
        print("✅ PyArrow basics completed successfully!")

        print("\n🎯 Key Takeaways:")
        print(
            "• PyArrow provides powerful dataset operations without database overhead"
        )
        print("• Optimization can significantly improve query performance")
        print("• Compaction reduces file count and improves organization")
        print("• Dataset optimization helps with common query patterns")
        print("• Memory efficiency is crucial for large datasets")

        print("\n🔗 Related Examples:")
        print("• DuckDB basics: Alternative database-based approach")
        print("• Schema management: Data quality and type optimization")
        print("• Type conversion: Format interoperability")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()
