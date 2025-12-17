"""
Cloud Datasets Workflow Example (Local-First)

This intermediate-level example demonstrates working with datasets using
fsspeckit's cloud storage integration patterns. The example runs entirely
offline using local filesystem simulation of cloud storage structures.

The example covers:
1. Setting up connections to different cloud storage providers (local simulation)
2. Working with cloud-like partitioned dataset structures
3. Performance optimization for cloud-based operations
4. Authentication and security considerations (configuration patterns)
5. Cost optimization strategies
6. Error handling and retry logic patterns

This example demonstrates local-first development with optional real cloud
configuration for production use.

To enable real cloud operations, set environment variables:
- AWS: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
- Azure: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
- GCP: GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

# Import fsspeckit components
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.storage_options import (
    AwsStorageOptions,
    AzureStorageOptions,
    GcsStorageOptions,
)


def create_sample_sales_data() -> pa.Table:
    """Create comprehensive sample sales data for cloud examples."""

    print("📊 Creating sample sales data for cloud workflows...")

    import random
    from datetime import datetime, timedelta

    # Generate realistic sales data
    products = [
        "Laptop Pro 15",
        "Wireless Mouse",
        "Mechanical Keyboard",
        "4K Monitor",
        "Noise-Cancelling Headphones",
        "HD Webcam",
        "USB-C Hub",
        "External SSD 1TB",
        "Gaming Chair",
        "Standing Desk",
        "Bluetooth Speakers",
        "Graphics Tablet",
    ]

    regions = [
        "North America",
        "Europe",
        "Asia Pacific",
        "Latin America",
        "Middle East",
    ]
    sales_reps = [
        "Alice Chen",
        "Bob Johnson",
        "Charlie Davis",
        "Diana Wilson",
        "Eve Brown",
    ]
    channels = ["Online", "Retail", "Partner", "Direct", "Distributor"]

    records = []
    base_date = datetime(2024, 1, 1)

    for i in range(5000):  # Larger dataset for cloud scenarios
        sale_date = base_date + timedelta(days=random.randint(0, 270))

        record = {
            "sale_id": f"S{2024}{i + 1:06d}",
            "date": sale_date.strftime("%Y-%m-%d"),
            "quarter": f"Q{(sale_date.month - 1) // 3 + 1}2024",
            "product": random.choice(products),
            "category": random.choice(
                ["Electronics", "Accessories", "Furniture", "Audio"]
            ),
            "quantity": random.randint(1, 50),
            "unit_price": round(random.uniform(25.0, 2500.0), 2),
            "total_amount": 0,  # Will be calculated
            "customer_region": random.choice(regions),
            "sales_rep": random.choice(sales_reps),
            "channel": random.choice(channels),
            "commission_rate": round(random.uniform(0.02, 0.15), 4),
            "shipping_method": random.choice(
                ["Standard", "Express", "Overnight", "Freight"]
            ),
        }

        # Calculate total and commission
        record["total_amount"] = record["quantity"] * record["unit_price"]
        record["commission"] = record["total_amount"] * record["commission_rate"]

        records.append(record)

    # Convert list of dicts to table using proper PyArrow API
    if records:
        # Extract column names from first record
        column_names = list(records[0].keys())
        # Convert to columnar format
        columns = {}
        for name in column_names:
            columns[name] = [record[name] for record in records]
        data = pa.table(columns)
    else:
        data = pa.table({})

    print(f"Created sales dataset with {len(data):,} records")
    return data


def demonstrate_local_s3_workflow():
    """Demonstrate working with S3-compatible storage (MinIO/local simulation)."""

    print("\n🌐 S3-Compatible Storage Workflow")

    # Create local data for demonstration
    sales_data = create_sample_sales_data()
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Save data locally (simulating S3)
        local_s3_path = temp_dir / "s3_data"
        local_s3_path.mkdir()

        # Partition data for realistic S3 structure
        print("Creating partitioned data structure...")

        # Partition by quarter and region
        for quarter in ["Q12024", "Q22024", "Q32024", "Q42024"]:
            for region in ["North_America", "Europe", "Asia_Pacific", "Latin_America"]:
                # Filter data for this partition
                quarter_filter = pc.equal(sales_data.column("quarter"), quarter)
                region_filter = pc.equal(sales_data.column("customer_region"), region)
                combined_filter = pc.and_(quarter_filter, region_filter)

                partition_data = sales_data.filter(combined_filter)

                if len(partition_data) > 0:
                    partition_dir = (
                        local_s3_path / f"quarter={quarter}" / f"region={region}"
                    )
                    partition_dir.mkdir(parents=True, exist_ok=True)

                    file_path = partition_dir / "sales.parquet"
                    pq.write_table(partition_data, file_path)

                    print(
                        f"  Created {quarter}/{region}: {len(partition_data)} records"
                    )

        # Simulate S3 storage options
        print(f"\n🔧 Configuring AWS Storage Options:")

        # Create AWS storage options (uses environment variables in production)
        import os

        aws_options = AwsStorageOptions(
            access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),  # None for local demo
            secret_access_key=os.environ.get(
                "AWS_SECRET_ACCESS_KEY"
            ),  # None for local demo
            region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            endpoint_url=str(local_s3_path),  # Local override for demo
            anonymous=True,  # Set to True for local/public access
            allow_http=True,  # Set to True for local testing
        )

        print("  AWS Storage Options configured:")
        print(f"    Region: {aws_options.region}")
        print(f"    Endpoint: {aws_options.endpoint_url}")
        print(f"    Anonymous: {aws_options.anonymous}")
        print(f"    HTTP: {aws_options.allow_http}")

        # Demonstrate using the storage options
        print(f"\n📖 Reading partitioned data:")

        # Simulate reading partitioned data
        print("  Simulating S3 read operations:")
        print(f"    Available files: {len(list(local_s3_path.rglob('*.parquet')))}")

        # Read all partitioned data
        all_files = list(local_s3_path.rglob("*.parquet"))
        partitioned_tables = [pq.read_table(f) for f in all_files]
        combined_data = pa.concat_tables(partitioned_tables)

        print(f"    Combined dataset: {len(combined_data):,} records")

        # Analyze the partitioned data
        print(f"\n📈 Partition Analysis:")
        quarter_stats = {}
        region_stats = {}

        for field_name in ["quarter", "customer_region"]:
            column = combined_data.column(field_name)
            unique_values, counts = pc.value_counts(column, null_expansion=False)

            if field_name == "quarter":
                quarter_stats = dict(zip(unique_values.to_pylist(), counts.to_pylist()))
            else:
                region_stats = dict(zip(unique_values.to_pylist(), counts.to_pylist()))

        print("  Quarterly distribution:")
        for quarter, count in sorted(quarter_stats.items()):
            print(f"    {quarter}: {count:,} records")

        print("  Regional distribution:")
        for region, count in sorted(region_stats.items()):
            print(f"    {region}: {count:,} records")

        # Performance test
        print(f"\n⚡ Performance Test:")
        start_time = time.time()

        # Simulate S3 read with partition pruning
        target_quarter = "Q22024"
        target_region = "Europe"

        # In real S3, this would only read relevant partitions
        quarter_filter = pc.equal(combined_data.column("quarter"), target_quarter)
        region_filter = pc.equal(combined_data.column("customer_region"), target_region)
        filtered = combined_data.filter(pc.and_(quarter_filter, region_filter))

        read_time = time.time() - start_time
        print(
            f"  Filtered read ({target_quarter}, {target_region}): {read_time:.4f}s, {len(filtered)} records"
        )

        return sales_data, local_s3_path

    except Exception as e:
        print(f"❌ S3 workflow demo failed: {e}")
        raise

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_azure_blob_workflow():
    """Demonstrate working with Azure Blob Storage."""

    print("\n☁️ Azure Blob Storage Workflow")

    sales_data = create_sample_sales_data()
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create Azure-like directory structure
        azure_path = temp_dir / "azure_container"
        azure_path.mkdir()

        print("Creating Azure Blob-style data structure...")

        # Azure typically organizes by date and category
        for year_month in ["2024-01", "2024-02", "2024-03"]:
            for category in ["Electronics", "Accessories", "Furniture", "Audio"]:
                # Filter data for this partition
                date_filter = pc.startswith(sales_data.column("date"), year_month[:7])
                category_filter = pc.equal(sales_data.column("category"), category)
                combined_filter = pc.and_(date_filter, category_filter)

                partition_data = sales_data.filter(combined_filter)

                if len(partition_data) > 0:
                    azure_dir = (
                        azure_path
                        / f"year={year_month[:4]}"
                        / f"month={year_month[5:7]}"
                        / f"category={category}"
                    )
                    azure_dir.mkdir(parents=True, exist_ok=True)

                    file_path = (
                        azure_dir / f"sales_{year_month}_{category.lower()}.parquet"
                    )
                    pq.write_table(partition_data, file_path)

                    print(
                        f"  Created {year_month}/{category}: {len(partition_data)} records"
                    )

        # Configure Azure storage options
        print(f"\n🔧 Configuring Azure Storage Options:")

        # Azure storage options (uses environment variables in production)
        azure_options = AzureStorageOptions(
            account_name=os.environ.get("AZURE_STORAGE_ACCOUNT"),
            account_key=os.environ.get("AZURE_STORAGE_KEY"),
            container_name="sales-data",
            connection_string=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
            endpoint_url=str(azure_path),  # Local override for demo
            allow_http=True,  # Set to True for local testing
        )

        print("  Azure Storage Options configured:")
        print(f"    Account: {azure_options.account_name}")
        print(f"    Container: {azure_options.container_name}")
        print(f"    HTTP: {azure_options.allow_http}")

        # Demonstrate Azure-specific operations
        print(f"\n📊 Azure Blob Analytics:")

        # Read all Azure-structured data
        all_files = list(azure_path.rglob("*.parquet"))
        azure_tables = [pq.read_table(f) for f in all_files]
        azure_combined = pa.concat_tables(azure_tables)

        # Azure-specific analytics
        print("  Monthly sales trends:")
        months = sorted(set(azure_combined.column("date").to_pylist()))
        monthly_totals = {}

        for month in months[:6]:  # Show first 6 months
            month_filter = pc.startswith(azure_combined.column("date"), month[:7])
            month_data = azure_combined.filter(month_filter)
            month_total = pc.sum(month_data.column("total_amount")).as_py()
            monthly_totals[month] = month_total

        for month, total in monthly_totals.items():
            print(f"    {month}: ${total:,.2f}")

        print(f"\n  Category performance:")
        category_stats = {}
        for category in ["Electronics", "Accessories", "Furniture", "Audio"]:
            category_filter = pc.equal(azure_combined.column("category"), category)
            category_data = azure_combined.filter(category_filter)
            category_total = pc.sum(category_data.column("total_amount")).as_py()
            category_count = pc.count(
                category_data.filter(
                    pc.greater(category_data.column("total_amount"), 1000)
                )
            ).as_py()
            category_stats[category] = {
                "total": category_total,
                "high_value_orders": category_count,
            }

        for category, stats in category_stats.items():
            print(
                f"    {category}: ${stats['total']:,.2f} total, {stats['high_value_orders']} high-value orders"
            )

        return sales_data, azure_path

    except Exception as e:
        print(f"❌ Azure workflow demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_gcs_workflow():
    """Demonstrate working with Google Cloud Storage."""

    print("\n🌍 Google Cloud Storage Workflow")

    sales_data = create_sample_sales_data()
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create GCS-like directory structure
        gcs_path = temp_dir / "gcs_bucket"
        gcs_path.mkdir()

        print("Creating GCS-style data structure...")

        # GCS often organizes by business unit and date
        for business_unit in ["Enterprise", "SMB", "Consumer", "Government"]:
            for year_quarter in ["2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4"]:
                # Simulate business unit segmentation
                unit_filter = pc.greater(
                    sales_data.column("total_amount"),
                    {
                        "Enterprise": 5000,
                        "SMB": 1000,
                        "Consumer": 100,
                        "Government": 10000,
                    }[business_unit],
                )

                quarter_filter = pc.equal(sales_data.column("quarter"), year_quarter)
                combined_filter = pc.and_(unit_filter, quarter_filter)

                partition_data = sales_data.filter(combined_filter)

                if len(partition_data) > 0:
                    gcs_dir = (
                        gcs_path
                        / f"business_unit={business_unit}"
                        / f"quarter={year_quarter}"
                    )
                    gcs_dir.mkdir(parents=True, exist_ok=True)

                    file_path = (
                        gcs_dir
                        / f"sales_{business_unit.lower()}_{year_quarter.replace('-', '')}.parquet"
                    )
                    pq.write_table(partition_data, file_path)

                    print(
                        f"  Created {business_unit}/{year_quarter}: {len(partition_data)} records"
                    )

        # Configure GCS storage options
        print(f"\n🔧 Configuring GCS Storage Options:")

        # GCS storage options (uses environment variables in production)
        gcs_options = GcsStorageOptions(
            project=os.environ.get("GCP_PROJECT"),
            token=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
            bucket="sales-data-bucket",
            endpoint_url=str(gcs_path),  # Local override for demo
            allow_http=True,
        )

        print("  GCS Storage Options configured:")
        print(f"    Project: {gcs_options.project}")
        print(f"    Bucket: {gcs_options.bucket}")
        print(f"    HTTP: {gcs_options.allow_http}")

        # GCS-specific analytics
        print(f"\n📈 Business Unit Analysis:")

        # Read all GCS-structured data
        all_files = list(gcs_path.rglob("*.parquet"))
        gcs_tables = [pq.read_table(f) for f in all_files]
        gcs_combined = pa.concat_tables(gcs_tables)

        # Business unit performance analysis
        print("  Business unit performance:")
        for unit in ["Enterprise", "SMB", "Consumer", "Government"]:
            if (
                unit in gcs_combined.column("customer_region").to_pylist()
            ):  # Add unit column for demo
                # In real implementation, business_unit would be a separate column
                continue

        # Simulate business unit analysis (would use actual business_unit column)
        large_deals = gcs_combined.filter(
            pc.greater(gcs_combined.column("total_amount"), 10000)
        )
        high_value_products = gcs_combined.filter(
            pc.greater(gcs_combined.column("unit_price"), 500)
        )

        print(f"  Large deals (>$10k): {len(large_deals):,} orders")
        print(f"  High-value products: {len(high_value_products):,} orders")

        # Channel analysis
        print(f"\n  Channel performance:")
        for channel in ["Online", "Retail", "Partner", "Direct", "Distributor"]:
            if channel in gcs_combined.column("channel").to_pylist():
                channel_filter = pc.equal(gcs_combined.column("channel"), channel)
                channel_data = gcs_combined.filter(channel_filter)
                channel_revenue = pc.sum(channel_data.column("total_amount")).as_py()
                channel_orders = len(channel_data)
                avg_order = (
                    channel_revenue / channel_orders if channel_orders > 0 else 0
                )
                print(
                    f"    {channel}: {channel_orders:,} orders, ${channel_revenue:,.0f} revenue, ${avg_order:.2f} avg"
                )

        return sales_data, gcs_path

    except Exception as e:
        print(f"❌ GCS workflow demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_performance_optimization():
    """Demonstrate performance optimization for cloud operations."""

    print("\n⚡ Cloud Performance Optimization")

    sales_data = create_sample_sales_data()
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create different data organizations for performance testing
        print("Creating test datasets for performance comparison...")

        # Organization 1: Flat structure (less optimal for cloud)
        flat_path = temp_dir / "flat_structure"
        flat_path.mkdir()

        # Write one large file (poor for parallel access)
        pq.write_table(sales_data, flat_path / "all_sales.parquet")
        print("  Created flat structure: 1 large file")

        # Organization 2: Partitioned structure (optimal for cloud)
        partitioned_path = temp_dir / "partitioned_structure"
        partitioned_path.mkdir()

        # Partition by quarter and channel (good for cloud pruning)
        for quarter in ["Q12024", "Q22024", "Q32024", "Q42024"]:
            quarter_path = partitioned_path / f"quarter={quarter}"
            quarter_path.mkdir()

            for channel in ["Online", "Retail", "Partner", "Direct", "Distributor"]:
                channel_filter = pc.equal(sales_data.column("channel"), channel)
                quarter_filter = pc.equal(sales_data.column("quarter"), quarter)
                combined_filter = pc.and_(channel_filter, quarter_filter)

                channel_data = sales_data.filter(combined_filter)

                if len(channel_data) > 0:
                    channel_file = quarter_path / f"channel={channel}.parquet"
                    pq.write_table(channel_data, channel_file)

        file_count = len(list(partitioned_path.rglob("*.parquet")))
        print(f"  Created partitioned structure: {file_count} files")

        # Performance comparison
        print(f"\n🏃 Performance Comparison:")

        # Test 1: Full table scan (flat vs partitioned)
        print("  Test 1: Full table scan")

        # Flat structure read
        start_time = time.time()
        flat_data = pq.read_table(flat_path / "all_sales.parquet")
        flat_time = time.time() - start_time

        # Partitioned structure read
        start_time = time.time()
        partitioned_files = list(partitioned_path.rglob("*.parquet"))
        partitioned_tables = [pq.read_table(f) for f in partitioned_files]
        partitioned_data = pa.concat_tables(partitioned_tables)
        partitioned_time = time.time() - start_time

        print(f"    Flat structure: {flat_time:.4f}s")
        print(f"    Partitioned:    {partitioned_time:.4f}s")
        print(f"    Speedup:        {flat_time / partitioned_time:.2f}x")

        # Test 2: Selective read (partition pruning benefit)
        print("  Test 2: Selective read (Q2 Online sales only)")

        # This would only read relevant partitions in a real cloud system
        start_time = time.time()

        # Simulate partition pruning by filtering after read
        quarter_filter = pc.equal(partitioned_data.column("quarter"), "Q22024")
        channel_filter = pc.equal(partitioned_data.column("channel"), "Online")
        selective_data = partitioned_data.filter(
            pc.and_(quarter_filter, channel_filter)
        )

        selective_time = time.time() - start_time
        print(
            f"    Selective read: {selective_time:.4f}s, {len(selective_data)} records"
        )

        # Estimate theoretical cloud savings (would only read 1/20 of files)
        theoretical_savings = (
            (len(partitioned_files) - 1) / len(partitioned_files) * 100
        )
        print(
            f"    Theoretical cloud savings: {theoretical_savings:.1f}% (partition pruning)"
        )

        # Test 3: Column projection
        print("  Test 3: Column projection")

        start_time = time.time()
        essential_columns = sales_data.select(
            ["sale_id", "date", "total_amount", "customer_region"]
        )
        projection_time = time.time() - start_time

        memory_savings = (sales_data.nbytes - essential_columns.nbytes) / 1024 / 1024
        print(f"    Column projection: {projection_time:.4f}s")
        print(f"    Memory savings: {memory_savings:.2f} MB")
        print(
            f"    Columns reduced: {len(sales_data.schema)} -> {len(essential_columns.schema)}"
        )

        # Cloud optimization recommendations
        print(f"\n💡 Cloud Optimization Recommendations:")
        print("  ✅ Use partitioning for frequently filtered columns")
        print("  ✅ Choose appropriate file sizes (100MB-1GB per file)")
        print("  ✅ Use column projection to reduce I/O")
        print("  ✅ Compress files appropriately (snappy for balance)")
        print("  ✅ Consider data locality for related operations")
        print("  ✅ Use caching for frequently accessed data")

    except Exception as e:
        print(f"❌ Performance optimization demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_error_handling():
    """Demonstrate robust error handling for cloud operations."""

    print("\n🛡️  Error Handling for Cloud Operations")

    try:
        # Simulate various cloud storage errors
        print("Testing error handling patterns...")

        # Pattern 1: Connection retry logic
        print("  Pattern 1: Connection retry with exponential backoff")

        def cloud_operation_with_retry(operation_func, max_retries=3, base_delay=1):
            """Execute cloud operation with retry logic."""
            for attempt in range(max_retries):
                try:
                    # Simulate cloud operation (random failure for demo)
                    import random

                    if random.random() < 0.3:  # 30% failure rate
                        raise ConnectionError(
                            f"Simulated cloud connection error (attempt {attempt + 1})"
                        )

                    return operation_func()

                except ConnectionError as e:
                    if attempt == max_retries - 1:
                        print(f"    ❌ Final attempt failed: {e}")
                        raise

                    delay = base_delay * (2**attempt)  # Exponential backoff
                    print(
                        f"    ⚠️  Attempt {attempt + 1} failed, retrying in {delay}s..."
                    )
                    time.sleep(min(delay, 10))  # Cap delay for demo

        # Test the retry pattern
        try:
            result = cloud_operation_with_retry(lambda: "Success")
            print(f"    ✅ Operation succeeded: {result}")
        except Exception as e:
            print(f"    ❌ Operation failed after retries: {e}")

        # Pattern 2: Graceful degradation
        print("\n  Pattern 2: Graceful degradation")

        def get_data_with_fallback(primary_source, fallback_source):
            """Get data with fallback to alternative source."""
            try:
                # Try primary source (cloud)
                print("    Attempting primary cloud source...")
                # Simulate primary source
                if random.random() < 0.4:  # 40% failure
                    raise ConnectionError("Primary cloud source unavailable")

                return primary_source()

            except Exception as e:
                print(f"    ⚠️  Primary source failed: {e}")
                print("    Falling back to local source...")

                # Use fallback (local cache, alternative cloud provider, etc.)
                return fallback_source()

        # Test graceful degradation
        try:
            result = get_data_with_fallback(
                lambda: "Cloud data", lambda: "Local cached data"
            )
            print(f"    ✅ Data retrieved: {result}")
        except Exception as e:
            print(f"    ❌ All sources failed: {e}")

        # Pattern 3: Validation and recovery
        print("\n  Pattern 3: Data validation and recovery")

        def validate_and_recover(data):
            """Validate data and attempt recovery if corrupted."""
            if data is None:
                raise ValueError("Data is None")

            if len(data) == 0:
                raise ValueError("Data is empty")

            # Check for common data quality issues
            null_counts = data.null_count
            total_nulls = sum(null_counts.values())

            if total_nulls > len(data) * 0.1:  # More than 10% nulls
                print(f"    ⚠️  High null count detected: {total_nulls}")
                # Could implement data cleaning or fallback here

            return data

        # Test validation
        sample_data = pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5]),
                "value": pa.array([10, None, 30, None, 50]),
            }
        )

        try:
            validated_data = validate_and_recover(sample_data)
            print(f"    ✅ Data validation passed: {len(validated_data)} records")
        except Exception as e:
            print(f"    ❌ Data validation failed: {e}")

        print(f"\n💡 Error Handling Best Practices:")
        print("  ✅ Implement retry logic with exponential backoff")
        print("  ✅ Use graceful degradation with fallback sources")
        print("  ✅ Validate data integrity and recover from corruption")
        print("  ✅ Log errors for debugging and monitoring")
        print("  ✅ Set appropriate timeouts for cloud operations")
        print("  ✅ Use circuit breakers for cascading failures")

    except Exception as e:
        print(f"❌ Error handling demo failed: {e}")


def main():
    """Run all cloud datasets workflow examples."""

    print("☁️ Cloud Datasets Workflow Example")
    print("=" * 60)
    print("This example demonstrates working with datasets in cloud storage")
    print("systems using fsspeckit's integrated cloud storage capabilities.")

    try:
        # Run all cloud workflow demonstrations
        sales_data, s3_path = demonstrate_local_s3_workflow()
        _, azure_path = demonstrate_azure_blob_workflow()
        _, gcs_path = demonstrate_gcs_workflow()
        demonstrate_performance_optimization()
        demonstrate_error_handling()

        print("\n" + "=" * 60)
        print("✅ Cloud datasets workflows completed successfully!")

        print("\n🎯 Key Takeaways:")
        print("• Use partitioning for optimal cloud query performance")
        print("• Choose appropriate file sizes for your cloud provider")
        print("• Implement robust error handling and retry logic")
        print("• Consider data transfer costs in your architecture")
        print("• Use security best practices for cloud credentials")
        print("• Monitor cloud storage usage and optimize costs")

        print("\n🔗 Related Examples:")
        print("• Advanced workflows: Production cloud deployments")
        print("• Cross-domain integration: Multi-cloud strategies")
        print("• Storage options: Authentication and security")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()
