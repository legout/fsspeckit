"""
Example: DuckDB Dataset Write Operations

This example demonstrates writing parquet datasets using DuckDB with support
for incremental updates, file splitting, and various naming strategies.

The example shows:
1. Basic dataset writes with unique filenames
2. Append mode for incremental data updates
3. Overwrite mode for replacing datasets
4. Splitting large tables across multiple files
5. Custom filename templates
6. Best practices for dataset organization
"""

import tempfile
from pathlib import Path
from datetime import datetime, timedelta

import pyarrow as pa

from fsspeckit.datasets import DuckDBParquetHandler


def create_sample_data(num_rows=100, offset=0):
    """Create sample sales data for demonstration."""
    return {
        "transaction_id": [f"TXN-{i+offset:05d}" for i in range(num_rows)],
        "date": [(datetime(2024, 1, 1) + timedelta(days=i % 30)).strftime("%Y-%m-%d") for i in range(num_rows)],
        "product": [["Laptop", "Mouse", "Keyboard", "Monitor"][i % 4] for i in range(num_rows)],
        "quantity": [(i % 10) + 1 for i in range(num_rows)],
        "amount": [round(100 + (i * 3.5) % 500, 2) for i in range(num_rows)],
        "region": [["North", "South", "East", "West"][i % 4] for i in range(num_rows)],
    }


def example_1_basic_dataset_write():
    """Example 1: Basic dataset write with unique filename."""
    print("=== Example 1: Basic Dataset Write ===")

    data = create_sample_data(50)
    table = pa.table(data)

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "sales_data"

        with DuckDBParquetHandler() as handler:
            # Write to dataset with automatic unique filename
            print(f"Writing data to dataset: {dataset_path}")
            handler.write_parquet_dataset(table, str(dataset_path))

            # List files in dataset
            files = list(dataset_path.glob("*.parquet"))
            print(f"  Created files: {[f.name for f in files]}")
            print(f"  Number of files: {len(files)}")

            # Verify data by reading back
            result = handler.read_parquet(str(dataset_path))
            print(f"  Total rows in dataset: {result.num_rows}")
            print(f"  Columns: {result.column_names}")


def example_2_incremental_updates_append_mode():
    """Example 2: Incremental data updates using append mode."""
    print("\n=== Example 2: Incremental Updates (Append Mode) ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "sales_incremental"

        with DuckDBParquetHandler() as handler:
            # Initial data load
            print("Loading initial batch (Day 1)...")
            batch1 = pa.table(create_sample_data(30, offset=0))
            handler.write_parquet_dataset(batch1, str(dataset_path), mode="append")

            files_after_day1 = list(dataset_path.glob("*.parquet"))
            print(f"  Files after Day 1: {len(files_after_day1)}")

            # Day 2 - append new data
            print("Loading Day 2 batch...")
            batch2 = pa.table(create_sample_data(25, offset=30))
            handler.write_parquet_dataset(batch2, str(dataset_path), mode="append")

            files_after_day2 = list(dataset_path.glob("*.parquet"))
            print(f"  Files after Day 2: {len(files_after_day2)}")

            # Day 3 - append more data
            print("Loading Day 3 batch...")
            batch3 = pa.table(create_sample_data(20, offset=55))
            handler.write_parquet_dataset(batch3, str(dataset_path), mode="append")

            files_after_day3 = list(dataset_path.glob("*.parquet"))
            print(f"  Files after Day 3: {len(files_after_day3)}")

            # Read combined dataset
            result = handler.read_parquet(str(dataset_path))
            print(f"\n  Total rows across all batches: {result.num_rows}")
            print(f"  Expected: {30 + 25 + 20} rows")

            # Query the combined dataset
            query = f"""
            SELECT region, COUNT(*) as transactions, SUM(amount) as total_sales
            FROM parquet_scan('{dataset_path}/*.parquet')
            GROUP BY region
            ORDER BY total_sales DESC
            """
            summary = handler.execute_sql(query)
            print("\n  Sales by region:")
            print(summary.to_pandas())


def example_3_overwrite_mode():
    """Example 3: Overwrite mode to replace entire dataset."""
    print("\n=== Example 3: Overwrite Mode (Replace Dataset) ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "sales_overwrite"

        with DuckDBParquetHandler() as handler:
            # Initial data
            print("Creating initial dataset...")
            old_data = pa.table(create_sample_data(40))
            handler.write_parquet_dataset(old_data, str(dataset_path))

            files_before = list(dataset_path.glob("*.parquet"))
            print(f"  Files before overwrite: {[f.name for f in files_before]}")

            result_before = handler.read_parquet(str(dataset_path))
            print(f"  Rows before overwrite: {result_before.num_rows}")

            # Overwrite with new data
            print("\nOverwriting with new dataset...")
            new_data = pa.table(create_sample_data(60, offset=100))
            handler.write_parquet_dataset(new_data, str(dataset_path), mode="overwrite")

            files_after = list(dataset_path.glob("*.parquet"))
            print(f"  Files after overwrite: {[f.name for f in files_after]}")

            # Verify old files are gone
            old_file_names = {f.name for f in files_before}
            new_file_names = {f.name for f in files_after}
            print(f"  Old files removed: {old_file_names.isdisjoint(new_file_names)}")

            result_after = handler.read_parquet(str(dataset_path))
            print(f"  Rows after overwrite: {result_after.num_rows}")


def example_4_splitting_large_tables():
    """Example 4: Split large tables across multiple files."""
    print("\n=== Example 4: Splitting Large Tables ===")

    # Create larger dataset
    large_data = create_sample_data(500)
    large_table = pa.table(large_data)

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "sales_split"

        with DuckDBParquetHandler() as handler:
            print(f"Splitting {large_table.num_rows} rows across multiple files...")

            # Split into files with max 100 rows each
            handler.write_parquet_dataset(
                large_table,
                str(dataset_path),
                max_rows_per_file=100
            )

            files = list(dataset_path.glob("*.parquet"))
            print(f"  Created {len(files)} files")
            print(f"  Expected: {(large_table.num_rows + 99) // 100} files")

            # Check size of each file
            print("\n  File details:")
            for i, file in enumerate(sorted(files)[:3]):  # Show first 3
                size_kb = file.stat().st_size / 1024
                print(f"    {file.name}: {size_kb:.1f} KB")

            # Verify all data is present
            result = handler.read_parquet(str(dataset_path))
            print(f"\n  Total rows after split: {result.num_rows}")
            print(f"  Data integrity: {'OK' if result.num_rows == large_table.num_rows else 'FAILED'}")


def example_5_custom_filename_templates():
    """Example 5: Custom filename templates."""
    print("\n=== Example 5: Custom Filename Templates ===")

    data = create_sample_data(30)
    table = pa.table(data)

    with tempfile.TemporaryDirectory() as temp_dir:
        with DuckDBParquetHandler() as handler:
            # Template with placeholder
            print("1. Using template with placeholder:")
            path1 = Path(temp_dir) / "dataset1"
            handler.write_parquet_dataset(
                table,
                str(path1),
                basename_template="sales_{}.parquet"
            )
            files1 = list(path1.glob("*.parquet"))
            print(f"   Created: {files1[0].name}")

            # Template for timestamped files
            print("\n2. Using date-based template:")
            path2 = Path(temp_dir) / "dataset2"
            date_str = datetime.now().strftime("%Y%m%d")
            handler.write_parquet_dataset(
                table,
                str(path2),
                basename_template=f"sales_{date_str}_{{}}.parquet"
            )
            files2 = list(path2.glob("*.parquet"))
            print(f"   Created: {files2[0].name}")

            # Multiple files with template
            print("\n3. Split data with custom template:")
            path3 = Path(temp_dir) / "dataset3"
            handler.write_parquet_dataset(
                table,
                str(path3),
                max_rows_per_file=10,
                basename_template="chunk_{}.parquet"
            )
            files3 = sorted(path3.glob("*.parquet"))
            print(f"   Created {len(files3)} files:")
            for f in files3[:3]:
                print(f"     - {f.name}")


def example_6_compression_options():
    """Example 6: Different compression options."""
    print("\n=== Example 6: Compression Options ===")

    data = create_sample_data(100)
    table = pa.table(data)

    with tempfile.TemporaryDirectory() as temp_dir:
        with DuckDBParquetHandler() as handler:
            codecs = ["snappy", "gzip", "zstd", "uncompressed"]

            print("Comparing file sizes with different compression:")
            for codec in codecs:
                dataset_path = Path(temp_dir) / f"dataset_{codec}"
                handler.write_parquet_dataset(
                    table,
                    str(dataset_path),
                    compression=codec
                )

                files = list(dataset_path.glob("*.parquet"))
                size_kb = files[0].stat().st_size / 1024
                print(f"  {codec:15}: {size_kb:7.1f} KB")


def example_7_best_practices():
    """Example 7: Best practices for dataset organization."""
    print("\n=== Example 7: Best Practices ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        base_path = Path(temp_dir) / "best_practices"

        with DuckDBParquetHandler() as handler:
            print("Best Practice #1: Organize by date hierarchy")
            # Create date-based organization
            for day in range(1, 4):
                date = datetime(2024, 1, day)
                date_path = base_path / f"year={date.year}" / f"month={date.month:02d}" / f"day={date.day:02d}"

                daily_data = pa.table(create_sample_data(20, offset=day*20))
                handler.write_parquet_dataset(daily_data, str(date_path))

            print(f"  Created hierarchical structure:")
            for item in sorted(base_path.rglob("*.parquet"))[:3]:
                rel_path = item.relative_to(base_path)
                print(f"    {rel_path}")

            print("\nBest Practice #2: Keep files reasonably sized (10-100 MB)")
            print("  Use max_rows_per_file to control file size")
            print("  Avoid: Too many small files (<1 MB) or very large files (>1 GB)")

            print("\nBest Practice #3: Use append mode for incremental updates")
            print("  - Safer default (no accidental data loss)")
            print("  - Efficient for time-series data")
            print("  - Periodic compaction may be needed")

            print("\nBest Practice #4: Use compression for storage efficiency")
            print("  - 'snappy': Fast, decent compression (default)")
            print("  - 'zstd': Better compression, slightly slower")
            print("  - 'gzip': Good compression, slower")

            print("\nBest Practice #5: Preserve metadata files")
            # Demonstrate metadata preservation
            metadata_path = base_path / "metadata"
            metadata_path.mkdir(parents=True, exist_ok=True)

            # Add metadata file
            readme = metadata_path / "README.txt"
            readme.write_text("Dataset metadata and documentation")

            # Write dataset - metadata will be preserved
            handler.write_parquet_dataset(
                pa.table(create_sample_data(10)),
                str(metadata_path),
                mode="overwrite"
            )

            # Check metadata file still exists
            print(f"\n  Metadata file preserved: {readme.exists()}")


def example_8_reading_datasets():
    """Example 8: Reading and querying datasets."""
    print("\n=== Example 8: Reading and Querying Datasets ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "sales_analysis"

        with DuckDBParquetHandler() as handler:
            # Create sample dataset with multiple files
            for i in range(3):
                batch = pa.table(create_sample_data(50, offset=i*50))
                handler.write_parquet_dataset(batch, str(dataset_path), mode="append")

            print("Dataset created with 3 files")

            # Read entire dataset
            print("\n1. Reading entire dataset:")
            all_data = handler.read_parquet(str(dataset_path))
            print(f"   Total rows: {all_data.num_rows}")

            # Query with filtering
            print("\n2. Query with filtering:")
            query = f"""
            SELECT date, product, SUM(amount) as total_sales
            FROM parquet_scan('{dataset_path}/*.parquet')
            WHERE region = 'North'
            GROUP BY date, product
            ORDER BY total_sales DESC
            LIMIT 5
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Aggregation across dataset
            print("\n3. Aggregation across all files:")
            query = f"""
            SELECT
                product,
                COUNT(*) as transaction_count,
                SUM(quantity) as total_quantity,
                ROUND(AVG(amount), 2) as avg_amount,
                ROUND(SUM(amount), 2) as total_revenue
            FROM parquet_scan('{dataset_path}/*.parquet')
            GROUP BY product
            ORDER BY total_revenue DESC
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())


def main():
    """Run all dataset write examples."""
    print("DuckDB Dataset Write Examples")
    print("=" * 70)

    try:
        example_1_basic_dataset_write()
        example_2_incremental_updates_append_mode()
        example_3_overwrite_mode()
        example_4_splitting_large_tables()
        example_5_custom_filename_templates()
        example_6_compression_options()
        example_7_best_practices()
        example_8_reading_datasets()

        print("\n" + "=" * 70)
        print("All examples completed successfully!")
        print("\nKey Takeaways:")
        print("  • Use append mode for incremental updates (safer default)")
        print("  • Use overwrite mode when replacing entire dataset")
        print("  • Split large tables with max_rows_per_file")
        print("  • Customize filenames with basename_template")
        print("  • Organize datasets hierarchically for better management")
        print("  • All datasets are readable with read_parquet()")

    except Exception as e:
        print(f"\nError running examples: {e}")
        raise


if __name__ == "__main__":
    main()
