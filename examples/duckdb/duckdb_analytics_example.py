"""
Advanced Example: DuckDB for Data Analytics

This example demonstrates real-world data analytics scenarios using DuckDB
with fsspeckit for parquet operations across different storage systems.

The example covers:
1. Data ingestion from various sources
2. Complex analytics and reporting
3. Performance optimization strategies
4. Multi-storage analytics
5. Time-series analysis
"""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from fsspeckit.datasets import DuckDBParquetHandler


def generate_sales_data(days=90, daily_transactions=50):
    """Generate realistic sales transaction data."""
    import random

    print(f"Generating {days} days of sales data...")

    transactions = []
    products = [
        "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones",
        "Webcam", "USB Hub", "External SSD", "Docking Station", "Cable Kit"
    ]
    regions = ["North", "South", "East", "West", "Central"]
    sales_reps = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]

    for day in range(days):
        date = datetime(2024, 1, 1) + timedelta(days=day)

        for _ in range(random.randint(20, daily_transactions)):
            transaction = {
                "transaction_id": f"TXN-{day:03d}-{random.randint(1000, 9999)}",
                "date": date.strftime("%Y-%m-%d"),
                "product": random.choice(products),
                "quantity": random.randint(1, 10),
                "unit_price": round(random.uniform(10.0, 500.0), 2),
                "region": random.choice(regions),
                "sales_rep": random.choice(sales_reps),
                "customer_type": random.choice(["New", "Returning", "Enterprise"]),
                "payment_method": random.choice(["Credit Card", "Wire Transfer", "PayPal", "Check"]),
            }
            transaction["total_amount"] = transaction["quantity"] * transaction["unit_price"]
            transactions.append(transaction)

    return transactions


def create_partitioned_dataset(transactions, base_path, handler):
    """Create a partitioned parquet dataset by date and region."""
    print("Creating partitioned dataset...")

    # Group transactions by date and region for partitioning
    by_date_region = {}
    for transaction in transactions:
        date_key = transaction["date"]
        region = transaction["region"]
        partition_key = f"date={date_key}/region={region}"

        if partition_key not in by_date_region:
            by_date_region[partition_key] = []
        by_date_region[partition_key].append(transaction)

    # Write each partition
    for partition_key, partition_data in by_date_region.items():
        partition_path = base_path / partition_key / "data.parquet"

        # Convert to PyArrow table
        table = pa.Table.from_pydict({
            key: [t[key] for t in partition_data]
            for key in partition_data[0].keys()
        })

        # Ensure parent directory exists
        partition_path.parent.mkdir(parents=True, exist_ok=True)

        # Write partition
        handler.write_parquet(table, str(partition_path), compression="snappy")

    print(f"Created {len(by_date_region)} partitions")
    return len(by_date_region)


def example_1_data_ingestion():
    """Example 1: Data ingestion and partitioning."""
    print("=== Example 1: Data Ingestion and Partitioning ===")

    # Generate sample data
    transactions = generate_sales_data(days=30, daily_transactions=100)
    print(f"Generated {len(transactions)} transactions")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        dataset_path = temp_path / "sales_data"

        with DuckDBParquetHandler() as handler:
            # Create partitioned dataset
            num_partitions = create_partitioned_dataset(transactions, dataset_path, handler)

            # Read and analyze the dataset
            print(f"\nReading partitioned dataset ({num_partitions} partitions):")
            result = handler.read_parquet(str(dataset_path))
            print(f"  Total records: {result.num_rows:,}")
            print(f"  Columns: {result.column_names}")
            print(f"  Date range: {result.column('date').to_pylist()[0]} to {result.column('date').to_pylist()[-1]}")


def example_2_business_analytics():
    """Example 2: Business analytics and reporting."""
    print("\n=== Example 2: Business Analytics and Reporting ===")

    transactions = generate_sales_data(days=90, daily_transactions=50)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        parquet_file = temp_path / "sales.parquet"

        with DuckDBParquetHandler() as handler:
            # Write all data
            table = pa.Table.from_pydict({
                key: [t[key] for t in transactions]
                for key in transactions[0].keys()
            })
            handler.write_parquet(table, str(parquet_file))

            # Example 1: Sales performance by region
            print("1. Sales Performance by Region:")
            query = f"""
            SELECT
                region,
                COUNT(*) as transaction_count,
                SUM(total_amount) as total_sales,
                AVG(total_amount) as avg_transaction_value,
                MAX(total_amount) as largest_sale
            FROM parquet_scan('{parquet_file}')
            GROUP BY region
            ORDER BY total_sales DESC
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 2: Product performance
            print("\n2. Top 5 Products by Revenue:")
            query = f"""
            SELECT
                product,
                SUM(quantity) as units_sold,
                SUM(total_amount) as revenue,
                AVG(unit_price) as avg_price,
                COUNT(DISTINCT transaction_id) as unique_transactions
            FROM parquet_scan('{parquet_file}')
            GROUP BY product
            ORDER BY revenue DESC
            LIMIT 5
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 3: Sales representative performance
            print("\n3. Sales Representative Performance:")
            query = f"""
            SELECT
                sales_rep,
                COUNT(*) as transactions,
                SUM(total_amount) as total_sales,
                AVG(total_amount) as avg_sale,
                COUNT(DISTINCT DATE(date)) as active_days,
                SUM(CASE WHEN customer_type = 'Enterprise' THEN 1 ELSE 0 END) as enterprise_clients
            FROM parquet_scan('{parquet_file}')
            GROUP BY sales_rep
            ORDER BY total_sales DESC
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 4: Customer type analysis
            print("\n4. Customer Type Analysis:")
            query = f"""
            SELECT
                customer_type,
                COUNT(*) as transaction_count,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_transaction,
                COUNT(DISTINCT transaction_id) as unique_customers,
                SUM(quantity) as total_units
            FROM parquet_scan('{parquet_file}')
            GROUP BY customer_type
            ORDER BY total_revenue DESC
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())


def example_3_time_series_analysis():
    """Example 3: Time-series analysis and trends."""
    print("\n=== Example 3: Time-Series Analysis ===")

    transactions = generate_sales_data(days=180, daily_transactions=30)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        parquet_file = temp_path / "sales_timeseries.parquet"

        with DuckDBParquetHandler() as handler:
            # Write data
            table = pa.Table.from_pydict({
                key: [t[key] for t in transactions]
                for key in transactions[0].keys()
            })
            handler.write_parquet(table, str(parquet_file))

            # Example 1: Daily sales trends
            print("1. Daily Sales Trends (Last 10 days):")
            query = f"""
            SELECT
                date,
                COUNT(*) as transactions,
                SUM(total_amount) as daily_revenue,
                AVG(total_amount) as avg_transaction,
                SUM(quantity) as units_sold
            FROM parquet_scan('{parquet_file}')
            GROUP BY date
            ORDER BY date DESC
            LIMIT 10
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 2: Weekly performance
            print("\n2. Weekly Performance Analysis:")
            query = f"""
            SELECT
                strftime(date, '%Y-%W') as week,
                COUNT(*) as transactions,
                SUM(total_amount) as weekly_revenue,
                AVG(total_amount) as avg_transaction,
                COUNT(DISTINCT region) as regions_active
            FROM parquet_scan('{parquet_file}')
            GROUP BY week
            ORDER BY week DESC
            LIMIT 8
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 3: Month-over-month growth
            print("\n3. Month-over-Month Growth:")
            query = f"""
            WITH monthly_sales AS (
                SELECT
                    strftime(date, '%Y-%m') as month,
                    SUM(total_amount) as monthly_revenue,
                    COUNT(*) as transaction_count
                FROM parquet_scan('{parquet_file}')
                GROUP BY month
            )
            SELECT
                month,
                monthly_revenue,
                transaction_count,
                LAG(monthly_revenue) OVER (ORDER BY month) as prev_month_revenue,
                ROUND(
                    (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month)) /
                    LAG(monthly_revenue) OVER (ORDER BY month) * 100, 2
                ) as growth_percent
            FROM monthly_sales
            ORDER BY month
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 4: Moving average
            print("\n4. 7-Day Moving Average:")
            query = f"""
            WITH daily_sales AS (
                SELECT
                    date,
                    SUM(total_amount) as daily_revenue
                FROM parquet_scan('{parquet_file}')
                GROUP BY date
            )
            SELECT
                date,
                daily_revenue,
                AVG(daily_revenue) OVER (
                    ORDER BY date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as moving_avg_7day
            FROM daily_sales
            ORDER BY date DESC
            LIMIT 15
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())


def example_4_advanced_analytics():
    """Example 4: Advanced analytics and insights."""
    print("\n=== Example 4: Advanced Analytics ===")

    transactions = generate_sales_data(days=365, daily_transactions=25)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        parquet_file = temp_path / "sales_advanced.parquet"

        with DuckDBParquetHandler() as handler:
            # Write data
            table = pa.Table.from_pydict({
                key: [t[key] for t in transactions]
                for key in transactions[0].keys()
            })
            handler.write_parquet(table, str(parquet_file))

            # Example 1: Customer segmentation using RFM analysis
            print("1. Customer Segmentation (RFM Analysis):")
            query = f"""
            WITH customer_metrics AS (
                SELECT
                    customer_type,
                    COUNT(*) as frequency,
                    SUM(total_amount) as monetary,
                    MAX(date) as last_purchase_date,
                    MIN(date) as first_purchase_date
                FROM parquet_scan('{parquet_file}')
                GROUP BY customer_type
            )
            SELECT
                customer_type,
                frequency,
                ROUND(monetary, 2) as total_revenue,
                last_purchase_date,
                CASE
                    WHEN monetary > 10000 THEN 'High Value'
                    WHEN monetary > 5000 THEN 'Medium Value'
                    ELSE 'Standard'
                END as value_segment,
                CASE
                    WHEN frequency > 200 THEN 'Frequent'
                    WHEN frequency > 100 THEN 'Regular'
                    ELSE 'Occasional'
                END as frequency_segment
            FROM customer_metrics
            ORDER BY monetary DESC
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 2: Product affinity analysis
            print("\n2. Product Affinity Analysis:")
            query = f"""
            WITH product_pairs AS (
                SELECT
                    t1.product as product1,
                    t2.product as product2,
                    COUNT(*) as purchase_count
                FROM parquet_scan('{parquet_file}') t1
                JOIN parquet_scan('{parquet_file}') t2
                    ON DATE(t1.date) = DATE(t2.date)
                    AND t1.region = t2.region
                    AND t1.transaction_id != t2.transaction_id
                WHERE t1.product < t2.product
                GROUP BY t1.product, t2.product
            )
            SELECT
                product1,
                product2,
                purchase_count as bought_together
            FROM product_pairs
            WHERE purchase_count > 5
            ORDER BY purchase_count DESC
            LIMIT 10
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())

            # Example 3: Regional performance insights
            print("\n3. Regional Performance Insights:")
            query = f"""
            WITH regional_metrics AS (
                SELECT
                    region,
                    product,
                    SUM(total_amount) as revenue,
                    COUNT(*) as transactions
                FROM parquet_scan('{parquet_file}')
                GROUP BY region, product
            ),
            region_totals AS (
                SELECT
                    region,
                    SUM(revenue) as total_revenue
                FROM regional_metrics
                GROUP BY region
            )
            SELECT
                rm.region,
                rm.product,
                rm.revenue,
                rm.transactions,
                rt.total_revenue as region_total,
                ROUND(rm.revenue * 100.0 / rt.total_revenue, 2) as revenue_percentage,
                ROW_NUMBER() OVER (PARTITION BY rm.region ORDER BY rm.revenue DESC) as rank_in_region
            FROM regional_metrics rm
            JOIN region_totals rt ON rm.region = rt.region
            WHERE rm.rank_in_region <= 3
            ORDER BY rm.region, rm.rank_in_region
            """
            result = handler.execute_sql(query)
            print(result.to_pandas())


def example_5_performance_optimization():
    """Example 5: Performance optimization strategies."""
    print("\n=== Example 5: Performance Optimization ===")

    # Generate larger dataset for performance testing
    transactions = generate_sales_data(days=730, daily_transactions=100)
    print(f"Generated {len(transactions):,} transactions for performance testing")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        with DuckDBParquetHandler() as handler:
            # Test different compression strategies
            print("\n1. Compression Performance Comparison:")
            codecs = ["snappy", "gzip", "lz4", "zstd"]

            table = pa.Table.from_pydict({
                key: [t[key] for t in transactions[:1000]]  # Sample for compression test
                for key in transactions[0].keys()
            })

            for codec in codecs:
                parquet_file = temp_path / f"sales_{codec}.parquet"
                handler.write_parquet(table, str(parquet_file), compression=codec)

                file_size = parquet_file.stat().st_size
                print(f"   {codec:8}: {file_size:,} bytes")

            # Test column selection performance
            print("\n2. Column Selection Performance:")
            large_parquet_file = temp_path / "large_sales.parquet"
            large_table = pa.Table.from_pydict({
                key: [t[key] for t in transactions]
                for key in transactions[0].keys()
            })
            handler.write_parquet(large_table, str(large_parquet_file))

            import time

            # Read all columns
            start_time = time.time()
            all_columns = handler.read_parquet(str(large_parquet_file))
            all_columns_time = time.time() - start_time

            # Read specific columns
            start_time = time.time()
            selected_columns = handler.read_parquet(
                str(large_parquet_file),
                columns=["date", "product", "total_amount"]
            )
            selected_columns_time = time.time() - start_time

            print(f"   All columns ({len(all_columns.column_names)}): {all_columns_time:.3f}s")
            print(f"   Selected columns (3): {selected_columns_time:.3f}s")
            print(f"   Performance improvement: {(all_columns_time / selected_columns_time):.1f}x")

            # Test query optimization
            print("\n3. Query Performance with Filters:")
            query = f"""
            SELECT region, SUM(total_amount) as revenue
            FROM parquet_scan('{large_parquet_file}')
            WHERE date >= '2024-06-01' AND customer_type = 'Enterprise'
            GROUP BY region
            """

            start_time = time.time()
            result = handler.execute_sql(query)
            query_time = time.time() - start_time

            print(f"   Filtered query completed in: {query_time:.3f}s")
            print(f"   Returned {result.num_rows} rows")


def main():
    """Run all advanced examples."""
    print("Advanced DuckDB Analytics Examples")
    print("=" * 60)

    try:
        example_1_data_ingestion()
        example_2_business_analytics()
        example_3_time_series_analysis()
        example_4_advanced_analytics()
        example_5_performance_optimization()

        print("\n" + "=" * 60)
        print("All advanced examples completed successfully!")

    except Exception as e:
        print(f"\nError running examples: {e}")
        raise


if __name__ == "__main__":
    main()