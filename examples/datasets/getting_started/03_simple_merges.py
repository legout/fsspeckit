"""
Simple Dataset Merges - Getting Started

This beginner-friendly example introduces fundamental dataset merging concepts
using fsspeckit's dataset utilities.

The example covers:
1. Basic dataset merging concepts
2. Simple append operations
3. Schema handling during merges
4. Duplicate detection and handling
5. Performance considerations for merging

This example helps you understand how to combine multiple datasets
efficiently and safely.
"""

from __future__ import annotations

import tempfile
import time
from datetime import datetime, timedelta
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
    from fsspeckit.datasets import DuckDBParquetHandler
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing fsspeckit dataset dependencies. Install with: pip install -e \".[datasets]\" "
        "(or run `uv sync` then `uv run python ...`)."
    ) from exc


def create_sample_sales_data(batch_id: str, num_records: int = 50) -> pa.Table:
    """Create sample sales data for a specific batch."""

    import random

    products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]
    customers = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    regions = ["North", "South", "East", "West", "Central"]

    records = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_records):
        record = {
            "order_id": f"{batch_id}-{i + 1:04d}",
            "batch_id": batch_id,
            "customer": random.choice(customers),
            "product": random.choice(products),
            "quantity": random.randint(1, 10),
            "unit_price": round(random.uniform(10.0, 500.0), 2),
            "region": random.choice(regions),
            "order_date": (base_date + timedelta(days=random.randint(0, 90))).strftime(
                "%Y-%m-%d"
            ),
        }
        record["total"] = record["quantity"] * record["unit_price"]
        records.append(record)

    return pa.Table.from_pylist(records)


def demonstrate_basic_append():
    """Demonstrate basic dataset append operations."""

    print("\n📋 Basic Dataset Append")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create initial dataset
        print("Creating initial dataset (Batch 1)...")
        batch1_data = create_sample_sales_data("B001", 30)

        # Save initial batch
        initial_file = temp_dir / "sales.parquet"
        pq.write_table(batch1_data, initial_file)

        print(f"✅ Created initial dataset: {len(batch1_data)} records")

        # Create additional batches
        print("\nAdding more batches...")

        batch2_data = create_sample_sales_data("B002", 25)
        batch3_data = create_sample_sales_data("B003", 35)

        # Simple append using PyArrow concat
        print("Performing simple append operation...")
        start_time = time.time()

        # Read existing data
        existing_data = pq.read_table(initial_file)

        # Append new data
        combined_data = pa.concat_tables([existing_data, batch2_data, batch3_data])

        append_time = time.time() - start_time

        # Save combined data
        combined_file = temp_dir / "sales_combined.parquet"
        pq.write_table(combined_data, combined_file)

        print(f"✅ Append completed in {append_time:.4f} seconds")
        print(f"   Combined dataset: {len(combined_data)} records")

        # Verify data integrity
        expected_total = len(batch1_data) + len(batch2_data) + len(batch3_data)
        if len(combined_data) == expected_total:
            print(f"   ✅ Data integrity verified: {expected_total} records")
        else:
            print(
                f"   ❌ Data mismatch: expected {expected_total}, got {len(combined_data)}"
            )

        # Show batch distribution
        print(f"\n📊 Batch Distribution:")
        for batch_id in ["B001", "B002", "B003"]:
            batch_filter = pc.equal(combined_data.column("batch_id"), batch_id)
            batch_count = pc.sum(batch_filter).as_py()
            print(f"   {batch_id}: {batch_count} records")

    except Exception as e:
        print(f"❌ Basic append failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_schema_consistency():
    """Demonstrate handling schema consistency during merges."""

    print("\n🔧 Schema Consistency in Merges")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create datasets with compatible schemas
        print("Creating datasets with consistent schemas...")

        # Dataset 1: Basic sales data
        sales1 = create_sample_sales_data("S001", 20)

        # Dataset 2: Sales data with same schema
        sales2 = create_sample_sales_data("S002", 20)

        # Dataset 3: Sales data with same schema (for demonstration)
        sales3 = create_sample_sales_data("S003", 20)

        print(f"✅ Created 3 datasets with consistent schemas")
        print(f"   Schema: {[field.name for field in sales1.schema]}")

        # Verify schemas are identical
        schemas_identical = sales1.schema.equals(
            sales2.schema
        ) and sales2.schema.equals(sales3.schema)

        if schemas_identical:
            print("✅ All schemas are identical - safe to merge")
        else:
            print("❌ Schema differences detected - need alignment")

        # Perform merge
        print("\nPerforming merge with consistent schemas...")
        start_time = time.time()

        merged_data = pa.concat_tables([sales1, sales2, sales3])

        merge_time = time.time() - start_time

        print(f"✅ Merge completed in {merge_time:.4f} seconds")
        print(f"   Merged dataset: {len(merged_data)} records")

        # Analyze merged data
        print(f"\n📈 Merged Data Analysis:")
        total_sales = pc.sum(merged_data.column("total")).as_py()
        avg_order_value = pc.mean(merged_data.column("total")).as_py()

        print(f"   Total sales value: ${total_sales:,.2f}")
        print(f"   Average order value: ${avg_order_value:,.2f}")

        # Show distribution by batch
        print(f"\n📋 Distribution by Batch:")
        for batch_id in ["S001", "S002", "S003"]:
            batch_count = pc.sum(
                pc.equal(merged_data.column("batch_id"), batch_id)
            ).as_py()
            print(f"   {batch_id}: {batch_count} records")

    except Exception as e:
        print(f"❌ Schema consistency demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_schema_alignment():
    """Demonstrate aligning different schemas for merging."""

    print("\n🔄 Schema Alignment for Merging")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create datasets with different schemas
        print("Creating datasets with different schemas...")

        # Dataset 1: Basic sales data
        sales_basic = create_sample_sales_data("X001", 20)
        # Remove some columns to create schema differences
        sales_basic = sales_basic.drop(["batch_id", "total"])

        # Dataset 2: Extended sales data with additional columns
        import random

        extended_records = []
        for i in range(20):
            record = {
                "order_id": f"X002-{i + 1:04d}",
                "customer": random.choice(["Alice", "Bob", "Charlie"]),
                "product": random.choice(["Laptop", "Mouse"]),
                "quantity": random.randint(1, 5),
                "unit_price": round(random.uniform(50.0, 200.0), 2),
                "region": random.choice(["North", "South"]),
                "order_date": "2024-02-01",
                "priority": random.choice(["high", "medium", "low"]),
                "sales_rep": random.choice(["John", "Jane", "Mike"]),
            }
            record["total"] = record["quantity"] * record["unit_price"]
            extended_records.append(record)

        sales_extended = pa.Table.from_pylist(extended_records)

        print(f"\n📋 Schema Comparison:")
        print(f"   Basic schema: {[field.name for field in sales_basic.schema]}")
        print(f"   Extended schema: {[field.name for field in sales_extended.schema]}")

        # Align schemas by adding missing columns with nulls
        print(f"\n🔧 Aligning schemas...")

        # Find all unique columns across both datasets
        all_columns = set()
        for schema in [sales_basic.schema, sales_extended.schema]:
            all_columns.update(field.name for field in schema)

        # Create aligned tables
        aligned_tables = []

        for table, name in [(sales_basic, "Basic"), (sales_extended, "Extended")]:
            aligned_data = {}

            for column_name in all_columns:
                if column_name in table.column_names:
                    aligned_data[column_name] = table.column(column_name)
                else:
                    # Add null array for missing column - use same type as in extended schema
                    if column_name in ["priority", "sales_rep"]:
                        aligned_data[column_name] = pa.array(
                            [None] * len(table), type=pa.string()
                        )
                    elif column_name in ["batch_id", "total"]:
                        aligned_data[column_name] = pa.array(
                            [None] * len(table),
                            type=pa.string()
                            if column_name == "batch_id"
                            else pa.float64(),
                        )
                    else:
                        aligned_data[column_name] = pa.array(
                            [None] * len(table), type=pa.string()
                        )

            aligned_table = pa.Table.from_pydict(aligned_data)
            aligned_tables.append(aligned_table)

            print(f"   {name} table aligned to {len(aligned_table.schema)} columns")

        # Merge aligned tables
        print(f"\n🔄 Merging aligned tables...")
        start_time = time.time()

        merged_aligned = pa.concat_tables(aligned_tables)

        merge_time = time.time() - start_time

        print(f"✅ Aligned merge completed in {merge_time:.4f} seconds")
        print(f"   Merged dataset: {len(merged_aligned)} records")
        print(f"   Final schema: {[field.name for field in merged_aligned.schema]}")

        # Show some sample data to verify alignment
        print(f"\n📄 Sample merged data (first 5 rows):")
        sample_data = merged_aligned.slice(0, min(5, len(merged_aligned)))
        for i in range(min(5, len(sample_data))):
            row_dict = {}
            for j, field in enumerate(sample_data.schema):
                value = sample_data.column(j)[i].as_py()
                row_dict[field.name] = value if value is not None else "NULL"
            print(f"   Row {i + 1}: {row_dict}")

    except Exception as e:
        print(f"❌ Schema alignment demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_duplicate_handling():
    """Demonstrate detecting and handling duplicates during merges."""

    print("\n🔍 Duplicate Detection and Handling")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create datasets with potential duplicates
        print("Creating datasets with duplicate records...")

        # Dataset 1: Initial sales data
        sales1 = create_sample_sales_data("DUP001", 30)

        # Dataset 2: Some duplicates and some new records
        import random

        duplicate_records = []
        new_records = []

        # Add some duplicates from first dataset
        for i in range(10):  # 10 duplicates
            original_row = i  # Reuse first 10 rows
            record = {
                "order_id": f"DUP002-{original_row + 1:04d}",  # Same order IDs
                "batch_id": "DUP002",
                "customer": sales1.column("customer")[original_row].as_py(),
                "product": sales1.column("product")[original_row].as_py(),
                "quantity": sales1.column("quantity")[original_row].as_py()
                + random.randint(-2, 2),  # Slight variation
                "unit_price": sales1.column("unit_price")[original_row].as_py(),
                "region": sales1.column("region")[original_row].as_py(),
                "order_date": sales1.column("order_date")[original_row].as_py(),
            }
            record["total"] = record["quantity"] * record["unit_price"]
            duplicate_records.append(record)

        # Add some new records
        for i in range(20):
            record = {
                "order_id": f"DUP002-{i + 50:04d}",  # Different IDs
                "batch_id": "DUP002",
                "customer": random.choice(["Alice", "Bob", "Charlie"]),
                "product": random.choice(["Laptop", "Mouse", "Keyboard"]),
                "quantity": random.randint(1, 10),
                "unit_price": round(random.uniform(50.0, 300.0), 2),
                "region": random.choice(["North", "South"]),
                "order_date": "2024-03-01",
            }
            record["total"] = record["quantity"] * record["unit_price"]
            new_records.append(record)

        sales2 = pa.Table.from_pylist(duplicate_records + new_records)

        print(f"   Dataset 1: {len(sales1)} records")
        print(
            f"   Dataset 2: {len(sales2)} records ({len(duplicate_records)} potential duplicates)"
        )

        # Simple merge first
        print(f"\n🔄 Simple merge (may contain duplicates)...")
        simple_merged = pa.concat_tables([sales1, sales2])
        print(f"   Simple merge result: {len(simple_merged)} records")

        # Use DuckDB for duplicate detection and removal
        print(f"\n🐥 Using DuckDB for duplicate handling...")

        # Save datasets temporarily
        file1 = temp_dir / "sales1.parquet"
        file2 = temp_dir / "sales2.parquet"
        pq.write_table(sales1, file1)
        pq.write_table(sales2, file2)

        with DuckDBParquetHandler() as handler:
            # Find duplicates based on order_id using direct file access
            print(f"\n🔍 Analyzing duplicates...")
            duplicate_analysis = handler.execute_sql(f"""
                SELECT
                    order_id,
                    COUNT(*) as duplicate_count
                FROM (
                    SELECT order_id FROM read_parquet('{file1}')
                    UNION ALL
                    SELECT order_id FROM read_parquet('{file2}')
                )
                GROUP BY order_id
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC
            """).fetchdf()

            if len(duplicate_analysis) > 0:
                print(f"   Found {len(duplicate_analysis)} duplicate order IDs:")
                print(duplicate_analysis.to_string())
            else:
                print("   No duplicates found")

            # Create deduplicated dataset using UNION
            print(f"\n🧹 Creating deduplicated dataset...")
            start_time = time.time()

            deduplication_query = f"""
            SELECT
                *
            FROM read_parquet('{file1}')
            UNION
            SELECT
                *
            FROM read_parquet('{file2}')
            ORDER BY order_id
            """

            deduplicated_data = handler.execute_sql(deduplication_query).fetchdf()

            deduplication_time = time.time() - start_time

            print(f"   Deduplication completed in {deduplication_time:.4f} seconds")
            print(f"   Deduplicated dataset: {len(deduplicated_data)} records")

            # Show deduplication statistics
            original_total = len(sales1) + len(sales2)
            duplicates_removed = original_total - len(deduplicated_data)
            deduplication_rate = (duplicates_removed / original_total) * 100

            print(f"\n📊 Deduplication Statistics:")
            print(f"   Original records: {original_total}")
            print(f"   Duplicates removed: {duplicates_removed}")
            print(f"   Deduplication rate: {deduplication_rate:.1f}%")
            print(f"   Final unique records: {len(deduplicated_data)}")

    except Exception as e:
        print(f"❌ Duplicate handling demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_performance_considerations():
    """Demonstrate performance considerations for merging datasets."""

    print("\n⚡ Performance Considerations for Merging")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create multiple datasets of different sizes
        print("Creating datasets of varying sizes...")

        datasets = []
        dataset_sizes = [100, 500, 1000, 2000]  # Different sizes
        total_records = 0

        for i, size in enumerate(dataset_sizes):
            batch_data = create_sample_sales_data(f"PERF{i + 1:03d}", size)
            datasets.append(batch_data)
            total_records += size

        print(
            f"   Created {len(datasets)} datasets with {total_records:,} total records"
        )

        # Test different merge strategies
        print(f"\n🏃 Testing merge strategies...")

        # Strategy 1: Simple concat (all at once)
        print(f"Strategy 1: Simple concat all at once")
        start_time = time.time()

        merged_all = pa.concat_tables(datasets)

        concat_time = time.time() - start_time
        print(f"   Time: {concat_time:.4f} seconds")
        print(f"   Result: {len(merged_all)} records")

        # Strategy 2: Incremental merge
        print(f"\nStrategy 2: Incremental merge")
        start_time = time.time()

        incremental_result = datasets[0]
        for dataset in datasets[1:]:
            incremental_result = pa.concat_tables([incremental_result, dataset])

        incremental_time = time.time() - start_time
        print(f"   Time: {incremental_time:.4f} seconds")
        print(f"   Result: {len(incremental_result)} records")

        # Strategy 3: DuckDB merge (for very large datasets)
        print(f"\nStrategy 3: DuckDB-based merge")

        # Save datasets to files
        dataset_files = []
        for i, dataset in enumerate(datasets):
            file_path = temp_dir / f"dataset_{i}.parquet"
            pq.write_table(dataset, file_path)
            dataset_files.append(file_path)

        start_time = time.time()

        with DuckDBParquetHandler() as handler:
            # Combine using UNION ALL with direct file access
            union_queries = [
                f"SELECT * FROM read_parquet('{file_path}')"
                for file_path in dataset_files
            ]
            union_query = " UNION ALL ".join(union_queries)

            duckdb_result = handler.execute_sql(
                f"SELECT * FROM ({union_query})"
            ).fetchdf()

        duckdb_time = time.time() - start_time
        print(f"   Time: {duckdb_time:.4f} seconds")
        print(f"   Result: {len(duckdb_result)} records")

        # Compare results
        print(f"\n📊 Performance Comparison:")
        print(f"   Simple concat:   {concat_time:.4f}s")
        print(f"   Incremental:    {incremental_time:.4f}s")
        print(f"   DuckDB:        {duckdb_time:.4f}s")

        # Verify all results are equivalent
        results_match = len(merged_all) == len(incremental_result) == len(duckdb_result)

        if results_match:
            print(
                f"   ✅ All strategies produced equivalent results: {len(merged_all)} records"
            )
        else:
            print(
                f"   ❌ Results differ: {len(merged_all)}, {len(incremental_result)}, {len(duckdb_result)}"
            )

        # Memory usage considerations
        print(f"\n💾 Memory Usage Considerations:")
        print(f"   Simple concat:   Holds all datasets in memory simultaneously")
        print(f"   Incremental:    Holds at most 2 datasets at a time")
        print(f"   DuckDB:        Streams data with controlled memory usage")

        print(f"\n💡 Recommendations:")
        print(f"   • Small datasets (< 10K records): Simple concat is fine")
        print(f"   • Medium datasets (10K-100K): Consider incremental approach")
        print(f"   • Large datasets (> 100K): Use DuckDB for memory efficiency")

    except Exception as e:
        print(f"❌ Performance demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def main():
    """Run all simple merging examples."""

    print("🔀 Simple Dataset Merges - Getting Started")
    print("=" * 60)
    print("This example introduces fundamental dataset merging concepts")
    print("using fsspeckit's dataset utilities.")

    try:
        # Run all demonstrations
        demonstrate_basic_append()
        demonstrate_schema_consistency()
        demonstrate_schema_alignment()
        demonstrate_duplicate_handling()
        demonstrate_performance_considerations()

        print("\n" + "=" * 60)
        print("✅ Simple merges completed successfully!")

        print("\n🎯 Key Takeaways:")
        print("• Always verify schema compatibility before merging")
        print("• Handle duplicates appropriately for your use case")
        print("• Consider performance impact for large datasets")
        print("• Use DuckDB for memory-efficient merging of large data")
        print("• Test merges with sample data before full production runs")

        print("\n🔗 Related Examples:")
        print("• DuckDB basics: Database-based merging strategies")
        print("• Schema management: Advanced schema handling")
        print("• Advanced workflows: Complex real-world scenarios")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    import time

    main()
