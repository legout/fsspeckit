"""
DuckDB Basics - Getting Started with Datasets

This beginner-friendly example introduces the fundamental concepts of using
DuckDBParquetHandler for efficient parquet dataset operations.

The example covers:
1. Basic setup and configuration
2. Creating and reading parquet files
3. Simple queries and filtering
4. Writing data back to parquet
5. Context manager usage for resource management

This is your starting point for learning how to use fsspeckit's
dataset capabilities with DuckDB for high-performance data operations.
"""

from __future__ import annotations

import tempfile
import time
from datetime import datetime
from pathlib import Path

try:
    import pyarrow as pa
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


def create_sample_sales_data() -> pa.Table:
    """Create sample sales data for demonstration."""

    print("📊 Creating sample sales data...")

    # Sample sales records
    sales_data = [
        {
            "order_id": 1001,
            "customer": "Alice",
            "product": "Laptop",
            "quantity": 1,
            "price": 999.99,
            "date": "2024-01-15",
        },
        {
            "order_id": 1002,
            "customer": "Bob",
            "product": "Mouse",
            "quantity": 2,
            "price": 25.50,
            "date": "2024-01-16",
        },
        {
            "order_id": 1003,
            "customer": "Charlie",
            "product": "Keyboard",
            "quantity": 1,
            "price": 79.99,
            "date": "2024-01-17",
        },
        {
            "order_id": 1004,
            "customer": "Diana",
            "product": "Monitor",
            "quantity": 1,
            "price": 299.99,
            "date": "2024-01-18",
        },
        {
            "order_id": 1005,
            "customer": "Eve",
            "product": "Headphones",
            "quantity": 1,
            "price": 149.99,
            "date": "2024-01-19",
        },
        {
            "order_id": 1006,
            "customer": "Frank",
            "product": "Webcam",
            "quantity": 1,
            "price": 89.99,
            "date": "2024-01-20",
        },
        {
            "order_id": 1007,
            "customer": "Grace",
            "product": "USB Hub",
            "quantity": 3,
            "price": 15.99,
            "date": "2024-01-21",
        },
        {
            "order_id": 1008,
            "customer": "Henry",
            "product": "External SSD",
            "quantity": 1,
            "price": 129.99,
            "date": "2024-01-22",
        },
    ]

    # Convert to PyArrow table
    table = pa.Table.from_pylist(sales_data)

    # Add calculated column
    total_values = [record["quantity"] * record["price"] for record in sales_data]
    table = table.add_column(
        len(table.schema), "total", pa.array(total_values, type=pa.float64())
    )

    print(f"Created {len(table)} sales records")
    return table


def demonstrate_basic_duckdb_usage():
    """Demonstrate basic DuckDBParquetHandler usage."""

    print("\n🚀 Basic DuckDBParquetHandler Usage")

    temp_dir = Path(tempfile.mkdtemp())
    sales_data = create_sample_sales_data()
    data_file = temp_dir / "sales.parquet"

    try:
        # Write data using DuckDBParquetHandler
        print(f"💾 Writing data to {data_file}")

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sales_data, str(data_file))

            print(f"✅ Successfully wrote {len(sales_data)} records")

            # Read data back
            print(f"📖 Reading data from {data_file}")
            read_data = handler.read_parquet(str(data_file))

            print(f"✅ Successfully read {len(read_data)} records")

            # Display the data
            print(f"\n📋 Sales Data:")
            print(read_data.to_pandas())

            # Verify data integrity
            original_count = len(sales_data)
            read_count = len(read_data)

            if original_count == read_count:
                print(f"✅ Data integrity verified: {original_count} records")
            else:
                print(
                    f"❌ Data integrity issue: {original_count} -> {read_count} records"
                )

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_sql_queries():
    """Demonstrate basic SQL queries with DuckDBParquetHandler."""

    print("\n🔍 SQL Queries with DuckDBParquetHandler")

    temp_dir = Path(tempfile.mkdtemp())
    sales_data = create_sample_sales_data()
    data_file = temp_dir / "sales.parquet"

    try:
        # First, write the data
        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sales_data, str(data_file))

            print("📊 Running SQL queries on sales data:")

            # Query 1: Basic SELECT using direct file access
            print("\n1. All sales records:")
            result1 = handler.execute_sql(
                f"SELECT * FROM read_parquet('{data_file}') ORDER BY order_id"
            )
            print(result1.fetchdf().to_string())

            # Query 2: Filter with WHERE clause
            print("\n2. High-value orders (total > $200):")
            result2 = handler.execute_sql(f"""
                SELECT order_id, customer, product, total
                FROM read_parquet('{data_file}')
                WHERE total > 200
                ORDER BY total DESC
            """)
            print(result2.fetchdf().to_string())

            # Query 3: Aggregate functions
            print("\n3. Sales summary by customer:")
            result3 = handler.execute_sql(f"""
                SELECT
                    customer,
                    COUNT(*) as order_count,
                    SUM(quantity) as total_quantity,
                    SUM(total) as total_sales,
                    AVG(total) as avg_order_value
                FROM read_parquet('{data_file}')
                GROUP BY customer
                ORDER BY total_sales DESC
            """)
            print(result3.fetchdf().to_string())

            # Query 4: Calculated fields
            print("\n4. Orders with tax calculation (10% tax):")
            result4 = handler.execute_sql(f"""
                SELECT
                    order_id,
                    product,
                    price,
                    quantity,
                    total,
                    total * 1.10 as total_with_tax
                FROM read_parquet('{data_file}')
                ORDER BY total_with_tax DESC
            """)
            print(result4.fetchdf().to_string())

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_context_manager():
    """Demonstrate proper resource management with context managers."""

    print("\n🛡️  Resource Management with Context Managers")

    temp_dir = Path(tempfile.mkdtemp())
    sales_data = create_sample_sales_data()
    data_file = temp_dir / "sales.parquet"

    try:
        # Good practice: Use context manager
        print("✅ Good practice: Using context manager")
        with DuckDBParquetHandler() as handler:
            handler.write_parquet(sales_data, str(data_file))
            print("   Data written successfully within context")

        # The handler is automatically closed when exiting the context

        # Bad practice: Manual resource management (shown for educational purposes)
        print("\n⚠️  Alternative: Manual resource management")
        handler = DuckDBParquetHandler()
        try:
            read_data = handler.read_parquet(str(data_file))
            print(f"   Read {len(read_data)} records manually")
        finally:
            # Always close manually if not using context manager
            handler.close()
            print("   Handler closed manually")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_error_handling():
    """Demonstrate proper error handling patterns."""

    print("\n⚠️  Error Handling Patterns")

    temp_dir = Path(tempfile.mkdtemp())
    sales_data = create_sample_sales_data()

    try:
        # Example 1: Handling file not found
        print("1. Handling missing file:")
        try:
            with DuckDBParquetHandler() as handler:
                missing_data = handler.read_parquet("nonexistent_file.parquet")
        except Exception as e:
            print(f"   ✅ Caught expected error: {type(e).__name__}")

        # Example 2: Handling SQL syntax errors
        print("\n2. Handling SQL syntax errors:")
        try:
            with DuckDBParquetHandler() as handler:
                # Write data first
                data_file = temp_dir / "test.parquet"
                handler.write_parquet(sales_data, str(data_file))

                # Try invalid SQL
                invalid_result = handler.execute_sql(
                    f"SELECT FROM read_parquet('{data_file}') WHERE INVALID"
                )
        except Exception as e:
            print(f"   ✅ Caught SQL error: {type(e).__name__}")

        # Example 3: Validation before operations
        print("\n3. Validating data before writing:")
        try:
            # Create data with potential issues
            problematic_data = pa.Table.from_pydict(
                {
                    "id": pa.array([1, 2, 3, 4, 5]),
                    "name": pa.array(
                        ["Alice", "Bob", None, "Diana", "Eve"]
                    ),  # Has null
                    "value": pa.array([10.5, 20.3, None, 40.1, 50.9]),  # Has null
                }
            )

            with DuckDBParquetHandler() as handler:
                # Validate before writing
                null_counts = [col.null_count for col in problematic_data.itercolumns()]
                total_nulls = sum(null_counts)

                if total_nulls > 0:
                    print(f"   ⚠️  Data contains {total_nulls} null values")
                    print(
                        f"   Null counts by column: {dict(zip(problematic_data.schema.names, null_counts))}"
                    )

                # Write anyway (just to show it works)
                data_file = temp_dir / "problematic.parquet"
                handler.write_parquet(problematic_data, str(data_file))
                print(f"   ✅ Successfully wrote data with nulls")

        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")

    except Exception as e:
        print(f"❌ Error in error handling demo: {e}")
        raise

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_basic_performance_tips():
    """Demonstrate basic performance tips."""

    print("\n⚡ Basic Performance Tips")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create larger dataset for performance testing
        print("Creating larger dataset for performance testing...")
        large_data = create_sample_sales_data()

        # Replicate data to make it larger
        larger_data = pa.concat_tables([large_data] * 50)  # 400 records

        print(f"Created dataset with {len(larger_data)} records")

        data_file = temp_dir / "large_sales.parquet"

        # Tip 1: Use context manager for efficient resource usage
        print("\n💡 Tip 1: Use context managers")
        start_time = time.time()

        with DuckDBParquetHandler() as handler:
            handler.write_parquet(larger_data, str(data_file))

        write_time = time.time() - start_time
        print(f"   Write time: {write_time:.3f} seconds")

        # Tip 2: Register datasets for multiple queries
        print("\n💡 Tip 2: Register datasets for efficient querying")
        start_time = time.time()

        with DuckDBParquetHandler() as handler:
            # Multiple queries on the same dataset using direct file access
            summary_result = handler.execute_sql(f"""
                SELECT COUNT(*) as total_records,
                       SUM(total) as total_sales,
                       AVG(total) as avg_sale
                FROM read_parquet('{data_file}')
            """).fetchdf()

            query_time = time.time() - start_time

        print(f"   Query time: {query_time:.3f} seconds")
        print(f"   Summary:")
        print(summary_result.to_string())

        # Tip 3: Use specific column selection instead of SELECT *
        print("\n💡 Tip 3: Select only needed columns")
        start_time = time.time()

        with DuckDBParquetHandler() as handler:
            # Good: Specific columns
            specific_result = handler.execute_sql(f"""
                SELECT customer, total
                FROM read_parquet('{data_file}')
                WHERE total > 100
            """).fetchdf()

            # Bad: All columns (commented out to avoid unnecessary processing)
            # all_result = handler.execute_sql("SELECT * FROM sales WHERE total > 100")

        selective_time = time.time() - start_time
        print(f"   Selective query time: {selective_time:.3f} seconds")
        print(f"   Result: {len(specific_result)} high-value orders")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir)


def main():
    """Run all basic DuckDB examples."""

    print("🐥 DuckDB Basics - Getting Started")
    print("=" * 50)
    print("This example introduces fundamental DuckDBParquetHandler usage")
    print("for efficient parquet dataset operations.")

    try:
        # Run all demonstrations
        demonstrate_basic_duckdb_usage()
        demonstrate_sql_queries()
        demonstrate_context_manager()
        demonstrate_error_handling()
        demonstrate_basic_performance_tips()

        print("\n" + "=" * 50)
        print("✅ DuckDB basics completed successfully!")

        print("\n🎯 Next Steps:")
        print("• Try modifying the SQL queries to explore different analyses")
        print("• Experiment with larger datasets to test performance")
        print("• Look at the PyArrow optimization example next")
        print("• Check out the schema management examples for data quality")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    import time

    main()
