"""
Example: DuckDB Integration for Parquet Operations

This example demonstrates how to use fsspeckit's DuckDBParquetHandler for
reading and writing parquet datasets using DuckDB with support for various
filesystems through fsspec.

The example shows:
1. Basic parquet read/write operations with local files
2. Working with directory-based parquet datasets
3. Using DuckDB for SQL queries and aggregations
4. Integration with remote storage (S3, GCS, Azure)
5. Advanced features like compression and column selection
"""

import tempfile
from pathlib import Path

import pyarrow as pa

from fsspeckit import filesystem
from fsspeckit.storage_options import AwsStorageOptions, LocalStorageOptions
from fsspeckit.datasets import DuckDBParquetHandler


def create_sample_data():
    """Create sample data for demonstration."""
    return {
        "customer_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "name": [
            "Alice Johnson",
            "Bob Smith",
            "Charlie Brown",
            "Diana Prince",
            "Eve Wilson",
            "Frank Miller",
            "Grace Kelly",
            "Henry Ford",
            "Iris West",
            "Jack Bauer",
        ],
        "age": [28, 34, 45, 29, 52, 38, 41, 65, 27, 43],
        "city": [
            "New York",
            "London",
            "Paris",
            "Tokyo",
            "Sydney",
            "Berlin",
            "Toronto",
            "Chicago",
            "Mumbai",
            "Singapore",
        ],
        "signup_date": [
            "2023-01-15",
            "2023-02-20",
            "2022-11-10",
            "2023-03-05",
            "2022-09-18",
            "2023-04-12",
            "2023-01-28",
            "2022-12-03",
            "2023-05-21",
            "2022-10-17",
        ],
        "purchase_amount": [150.50, 89.99, 234.75, 67.25, 412.80, 198.40, 345.60, 78.90, 156.75, 289.30],
        "category": ["A", "B", "A", "C", "B", "A", "C", "B", "A", "C"],
        "is_active": [True, True, False, True, True, False, True, True, False, True],
    }


def example_1_basic_local_operations():
    """Example 1: Basic local file operations."""
    print("=== Example 1: Basic Local File Operations ===")

    # Create sample data
    data = create_sample_data()
    table = pa.Table.from_pydict(data)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        parquet_file = temp_path / "customers.parquet"

        # Initialize handler with default local filesystem
        with DuckDBParquetHandler() as handler:
            # Write data to parquet
            print(f"Writing data to {parquet_file}")
            handler.write_parquet(table, str(parquet_file), compression="snappy")

            # Read data back
            print("Reading data back:")
            result = handler.read_parquet(str(parquet_file))
            print(f"  Shape: {result.shape}")
            print(f"  Columns: {result.column_names}")
            print(f"  First 3 rows:\n{result.slice(0, 3).to_pandas()}")

        # Demonstrate reading specific columns
        with DuckDBParquetHandler() as handler:
            print("\nReading specific columns (name, age, city):")
            result = handler.read_parquet(str(parquet_file), columns=["name", "age", "city"])
            print(f"  Shape: {result.shape}")
            print(f"  First 3 rows:\n{result.slice(0, 3).to_pandas()}")


def example_2_dataset_operations():
    """Example 2: Working with directory-based datasets."""
    print("\n=== Example 2: Directory-Based Dataset Operations ===")

    # Create larger sample data
    data = create_sample_data()
    table = pa.Table.from_pydict(data)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        dataset_dir = temp_path / "customer_dataset"

        # Write multiple parquet files to create a dataset
        with DuckDBParquetHandler() as handler:
            print("Creating dataset with multiple files...")

            # Split data and write multiple files
            for i in range(0, len(table), 3):  # Write in chunks of 3 rows
                chunk = table.slice(i, min(3, len(table) - i))
                file_path = dataset_dir / f"part_{i//3}.parquet"
                handler.write_parquet(chunk, str(file_path))

            # Read entire dataset
            print("Reading entire dataset:")
            result = handler.read_parquet(str(dataset_dir))
            print(f"  Shape: {result.shape}")
            print(f"  Total rows: {result.num_rows}")

            # Verify we can read specific columns from dataset
            print("\nReading specific columns from dataset:")
            result = handler.read_parquet(str(dataset_dir), columns=["customer_id", "purchase_amount"])
            total_purchases = result.column("purchase_amount").to_pylist()
            print(f"  Total purchases: ${sum(total_purchases):.2f}")


def example_3_sql_queries():
    """Example 3: SQL queries and aggregations."""
    print("\n=== Example 3: SQL Queries and Aggregations ===")

    data = create_sample_data()
    table = pa.Table.from_pydict(data)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        parquet_file = temp_path / "customers.parquet"

        with DuckDBParquetHandler() as handler:
            # Write data
            handler.write_parquet(table, str(parquet_file))

            # Example 1: Simple filtering
            print("1. Customers older than 40:")
            query = f"""
            SELECT name, age, city
            FROM parquet_scan('{parquet_file}')
            WHERE age > 40
            ORDER BY age DESC
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 2: Aggregation by category
            print("\n2. Purchase statistics by category:")
            query = f"""
            SELECT
                category,
                COUNT(*) as customer_count,
                AVG(purchase_amount) as avg_purchase,
                SUM(purchase_amount) as total_purchase,
                MAX(purchase_amount) as max_purchase
            FROM parquet_scan('{parquet_file}')
            GROUP BY category
            ORDER BY total_purchase DESC
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 3: Complex filtering and calculation
            print("\n3. Active customers with purchases > $100:")
            query = f"""
            SELECT
                name,
                age,
                city,
                purchase_amount,
                CASE
                    WHEN purchase_amount > 300 THEN 'High Value'
                    WHEN purchase_amount > 200 THEN 'Medium Value'
                    ELSE 'Standard'
                END as customer_tier
            FROM parquet_scan('{parquet_file}')
            WHERE is_active = true AND purchase_amount > 100
            ORDER BY purchase_amount DESC
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 4: Parameterized query
            print("\n4. Customers in specific age range (30-50):")
            query = f"""
            SELECT name, age, city, purchase_amount
            FROM parquet_scan('{parquet_file}')
            WHERE age BETWEEN ? AND ?
            ORDER BY age
            """
            result = handler.execute_sql(query, parameters=[30, 50])
            print(result.to_pandas())


def example_4_storage_options():
    """Example 4: Using storage options for different filesystems."""
    print("\n=== Example 4: Storage Options Integration ===")

    data = create_sample_data()
    table = pa.Table.from_pydict(data)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Example 1: Using LocalStorageOptions
        print("1. Using LocalStorageOptions:")
        storage_options = LocalStorageOptions()

        with DuckDBParquetHandler(storage_options=storage_options) as handler:
            parquet_file = temp_path / "local_customers.parquet"
            handler.write_parquet(table, str(parquet_file))

            result = handler.read_parquet(str(parquet_file))
            print(f"   Successfully wrote and read {result.num_rows} rows")

        # Example 2: Using existing filesystem instance
        print("\n2. Using existing filesystem instance:")
        fs = filesystem("file")

        with DuckDBParquetHandler(filesystem=fs) as handler:
            parquet_file = temp_path / "fs_customers.parquet"
            handler.write_parquet(table, str(parquet_file))

            result = handler.read_parquet(str(parquet_file))
            print(f"   Successfully wrote and read {result.num_rows} rows")

        # Example 3: Mock remote storage (commented out - requires actual credentials)
        print("\n3. Remote storage example (commented out):")
        print("""
        # For AWS S3:
        s3_options = AwsStorageOptions(
            access_key_id="YOUR_ACCESS_KEY",
            secret_access_key="YOUR_SECRET_KEY",
            region="us-east-1"
        )

        with DuckDBParquetHandler(storage_options=s3_options) as handler:
            # This would work with actual S3 credentials
            result = handler.read_parquet("s3://your-bucket/data/customers.parquet")

        # For Google Cloud Storage:
        gcs_options = GcsStorageOptions(token="YOUR_GCS_TOKEN")

        with DuckDBParquetHandler(storage_options=gcs_options) as handler:
            result = handler.read_parquet("gs://your-bucket/data/customers/")
        """)


def example_5_advanced_features():
    """Example 5: Advanced features and optimizations."""
    print("\n=== Example 5: Advanced Features ===")

    # Create larger dataset for performance demonstration
    large_data = create_sample_data()
    # Repeat data to make it larger
    for key in large_data:
        large_data[key] = large_data[key] * 100

    table = pa.Table.from_pydict(large_data)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Example 1: Different compression codecs
        print("1. Testing different compression codecs:")
        codecs = ["snappy", "gzip", "lz4", "brotli"]

        for codec in codecs:
            with DuckDBParquetHandler() as handler:
                parquet_file = temp_path / f"customers_{codec}.parquet"
                handler.write_parquet(table, str(parquet_file), compression=codec)

                # Check file size
                file_size = parquet_file.stat().st_size
                print(f"   {codec}: {file_size:,} bytes")

        # Example 2: Writing to nested directory structure
        print("\n2. Writing to nested directory structure:")
        with DuckDBParquetHandler() as handler:
            nested_path = temp_path / "2024" / "01" / "customers.parquet"
            handler.write_parquet(table, str(nested_path))

            if nested_path.exists():
                print(f"   Successfully created nested file: {nested_path}")

        # Example 3: Context manager usage
        print("\n3. Context manager usage:")
        print("   Using 'with' statement for automatic resource cleanup")

        with DuckDBParquetHandler() as handler:
            parquet_file = temp_path / "managed_customers.parquet"
            handler.write_parquet(table, str(parquet_file))
            result = handler.read_parquet(str(parquet_file))
            print(f"   Processed {result.num_rows} rows")
        # Connection is automatically closed here


def main():
    """Run all examples."""
    print("DuckDB Integration Examples")
    print("=" * 50)

    try:
        example_1_basic_local_operations()
        example_2_dataset_operations()
        example_3_sql_queries()
        example_4_storage_options()
        example_5_advanced_features()

        print("\n" + "=" * 50)
        print("All examples completed successfully!")

    except Exception as e:
        print(f"\nError running examples: {e}")
        raise


if __name__ == "__main__":
    main()