"""
Performance Optimization Workflow Example

This intermediate-level example demonstrates advanced performance optimization
techniques for large-scale dataset operations using fsspeckit.

The example covers:
1. Query performance optimization strategies
2. Memory management for large datasets
3. Parallel processing for CPU-bound operations
4. Caching strategies for frequently accessed data
5. I/O optimization techniques
6. Performance monitoring and profiling

This example helps you understand how to squeeze maximum performance
from your dataset operations.
"""

from __future__ import annotations

import tempfile
import time
import psutil
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from fsspeckit.datasets import (
    DuckDBParquetHandler,
    optimize_parquet_dataset_pyarrow,
    compact_parquet_dataset_pyarrow,
)
from fsspeckit.common.misc import run_parallel


def create_large_analytics_dataset() -> pa.Table:
    """Create a large dataset for performance testing."""

    print("📊 Creating large analytics dataset...")

    import random

    # Generate comprehensive analytics data
    products = [
        "Laptop Pro 15",
        "MacBook Air",
        "Surface Pro",
        "ThinkPad X1",
        "iPad Pro",
        "Surface Book",
        "Galaxy Tab",
        "Kindle Oasis",
    ]

    regions = [
        "North America",
        "Europe",
        "Asia Pacific",
        "Latin America",
        "Middle East",
        "Africa",
        "Oceania",
    ]

    channels = ["Online", "Retail", "Partner", "Direct", "Marketplace", "Mobile"]
    customer_segments = ["Enterprise", "SMB", "Consumer", "Education", "Government"]

    records = []
    base_date = datetime(2023, 1, 1)

    print("  Generating records (this may take a moment)...")

    # Create smaller dataset (10K records for smoke testing, adjust as needed for performance testing)
    for i in range(10000):
        if i % 1000 == 0:
            print(f"    Progress: {i:,} / 10,000")

        sale_date = base_date + timedelta(days=random.randint(0, 730))

        record = {
            "transaction_id": f"TXN_{i + 1:08d}",
            "timestamp": sale_date.isoformat(),
            "date": sale_date.strftime("%Y-%m-%d"),
            "year": sale_date.year,
            "quarter": f"Q{(sale_date.month - 1) // 3 + 1}{sale_date.year}",
            "month": sale_date.strftime("%Y-%m"),
            "product": random.choice(products),
            "category": random.choice(
                ["Electronics", "Accessories", "Software", "Services"]
            ),
            "quantity": random.randint(1, 100),
            "unit_price": round(random.uniform(10.0, 5000.0), 2),
            "discount_percent": round(random.uniform(0.0, 30.0), 1),
            "region": random.choice(regions),
            "country": f"Country_{random.randint(1, 50):02d}",
            "channel": random.choice(channels),
            "customer_segment": random.choice(customer_segments),
            "customer_id": f"CUST_{random.randint(1, 50000):06d}",
            "sales_rep_id": f"REP_{random.randint(1, 500):04d}",
            "campaign_id": f"CAMPAIGN_{random.randint(1, 100):04d}",
            "is_returned": random.choice([True, False]) if i % 20 == 0 else False,
            "return_reason": random.choice(
                ["Defective", "Wrong Item", "No Longer Needed", "Gift Return"]
            )
            if i % 20 == 0
            else None,
        }

        # Calculate fields
        record["total_amount"] = record["quantity"] * record["unit_price"]
        record["discount_amount"] = record["total_amount"] * (
            record["discount_percent"] / 100
        )
        record["net_amount"] = record["total_amount"] - record["discount_amount"]
        record["commission_rate"] = round(random.uniform(0.01, 0.20), 4)
        record["commission"] = record["net_amount"] * record["commission_rate"]

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
    print(f"✅ Created analytics dataset with {len(data):,} records")
    print(f"   Memory usage: {data.nbytes / 1024 / 1024:.2f} MB")
    print(f"   Columns: {len(data.schema)}")

    return data


def profile_operation(operation_func, operation_name, iterations=3):
    """Profile an operation and return performance metrics."""

    print(f"\n⏱️  Profiling: {operation_name}")

    results = []

    for i in range(iterations):
        # Monitor system resources
        process = psutil.Process()
        cpu_before = process.cpu_percent()
        memory_before = process.memory_info().rss / 1024 / 1024

        start_time = time.time()
        start_cpu = process.cpu_percent()

        try:
            result = operation_func()

            end_time = time.time()
            end_cpu = process.cpu_percent()
            memory_after = process.memory_info().rss / 1024 / 1024

            execution_time = end_time - start_time
            cpu_usage = (start_cpu + end_cpu) / 2
            memory_delta = memory_after - memory_before

            result_size = len(result) if hasattr(result, "__len__") else 0

            results.append(
                {
                    "iteration": i + 1,
                    "execution_time": execution_time,
                    "cpu_usage": cpu_usage,
                    "memory_delta": memory_delta,
                    "result_size": result_size,
                    "throughput": result_size / execution_time
                    if execution_time > 0
                    else 0,
                }
            )

            if i == 0:
                print(
                    f"    Iteration {i + 1}: {execution_time:.4f}s, CPU: {cpu_usage:.1f}%, Memory Δ: {memory_delta:.1f}MB"
                )

        except Exception as e:
            print(f"    ❌ Operation failed: {e}")
            results.append(
                {
                    "iteration": i + 1,
                    "execution_time": float("inf"),
                    "cpu_usage": 0,
                    "memory_delta": 0,
                    "result_size": 0,
                    "throughput": 0,
                    "error": str(e),
                }
            )

    # Calculate averages
    successful_results = [r for r in results if "error" not in r]
    if successful_results:
        avg_time = sum(r["execution_time"] for r in successful_results) / len(
            successful_results
        )
        avg_cpu = sum(r["cpu_usage"] for r in successful_results) / len(
            successful_results
        )
        avg_memory = sum(r["memory_delta"] for r in successful_results) / len(
            successful_results
        )
        avg_throughput = sum(r["throughput"] for r in successful_results) / len(
            successful_results
        )

        print(
            f"    Average: {avg_time:.4f}s, CPU: {avg_cpu:.1f}%, Memory Δ: {avg_memory:.1f}MB, Throughput: {avg_throughput:.0f} records/s"
        )

    return results


def demonstrate_query_optimization():
    """Demonstrate query optimization techniques."""

    print("\n🚀 Query Performance Optimization")

    temp_dir = Path(tempfile.mkdtemp())
    analytics_data = create_large_analytics_dataset()

    try:
        # Save data for DuckDB operations
        data_file = temp_dir / "analytics.parquet"
        pq.write_table(analytics_data, data_file)

        with DuckDBParquetHandler() as handler:
            # Register the parquet file directly in DuckDB using SQL
            handler.execute_sql(f"""
                CREATE TABLE sales AS 
                SELECT * FROM read_parquet('{data_file}')
            """)

            print("\n📊 Query Optimization Strategies:")

            # Strategy 1: Column projection
            print("\n1. Column Projection:")
            print("   a) Full table scan (all columns):")
            full_results = profile_operation(
                lambda: handler.execute_sql("SELECT * FROM sales"), "Full table scan", 2
            )

            print("   b) Selective column projection:")
            selective_results = profile_operation(
                lambda: handler.execute_sql(
                    "SELECT transaction_id, net_amount FROM sales"
                ),
                "Selective projection",
                2,
            )

            # Strategy 2: Early filtering
            print("\n2. Early Filtering:")
            print("   a) Filter after loading:")
            late_filter_results = profile_operation(
                lambda: handler.execute_sql(
                    "SELECT * FROM sales WHERE net_amount > 1000"
                ),
                "Late filtering",
                2,
            )

            print("   b) Optimized filter with index simulation:")
            early_filter_results = profile_operation(
                lambda: handler.execute_sql("""
                    SELECT transaction_id, net_amount, product
                    FROM sales
                    WHERE net_amount > 1000
                    AND year = 2024
                """),
                "Early filtering with compound condition",
                2,
            )

            # Strategy 3: Aggregation optimization
            print("\n3. Aggregation Optimization:")
            print("   a) Simple aggregation:")
            simple_agg_results = profile_operation(
                lambda: handler.execute_sql("""
                    SELECT region, COUNT(*) as count, SUM(net_amount) as total
                    FROM sales
                    GROUP BY region
                """),
                "Simple aggregation",
                2,
            )

            print("   b) Optimized aggregation with filters:")
            optimized_agg_results = profile_operation(
                lambda: handler.execute_sql("""
                    SELECT region, COUNT(*) as count, SUM(net_amount) as total
                    FROM sales
                    WHERE net_amount > 100
                      AND year >= 2023
                    GROUP BY region
                    HAVING COUNT(*) > 1000
                """),
                "Optimized aggregation",
                2,
            )

            # Strategy 4: Query batching
            print("\n4. Query Batching:")
            print("   a) Large single query:")
            single_query_results = profile_operation(
                lambda: handler.execute_sql("""
                    SELECT
                        region,
                        channel,
                        customer_segment,
                        COUNT(*) as transaction_count,
                        SUM(net_amount) as total_sales,
                        AVG(net_amount) as avg_sale,
                        MAX(net_amount) as max_sale
                    FROM sales
                    WHERE year = 2024
                    GROUP BY region, channel, customer_segment
                    ORDER BY total_sales DESC
                """),
                "Large single query",
                2,
            )

            print("   b) Batched smaller queries:")
            batched_results = profile_operation(
                lambda: [
                    handler.execute_sql(f"""
                        SELECT region, channel, customer_segment,
                               COUNT(*) as count, SUM(net_amount) as total
                        FROM sales
                        WHERE year = 2024 AND region = '{region}'
                        GROUP BY region, channel, customer_segment
                    """)
                    for region in ["North America", "Europe", "Asia Pacific"]
                ],
                "Batched queries",
                1,
            )

            print("\n📈 Optimization Summary:")
            optimizations = [
                ("Column Projection", selective_results, full_results),
                ("Early Filtering", early_filter_results, late_filter_results),
                ("Optimized Aggregation", optimized_agg_results, simple_agg_results),
            ]

            for opt_name, opt_results, base_results in optimizations:
                if opt_results and base_results:
                    opt_time = sum(
                        r["execution_time"] for r in opt_results if "error" not in r
                    )
                    base_time = sum(
                        r["execution_time"] for r in base_results if "error" not in r
                    )
                    if base_time > 0:
                        improvement = ((base_time - opt_time) / base_time) * 100
                        print(
                            f"   {opt_name}: {improvement:.1f}% performance improvement"
                        )

    except Exception as e:
        print(f"❌ Query optimization demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_memory_optimization():
    """Demonstrate memory optimization techniques."""

    print("\n💾 Memory Optimization Strategies")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create large dataset
        large_data = create_large_analytics_dataset()
        original_memory = large_data.nbytes / 1024 / 1024

        print(f"\n📊 Original Dataset:")
        print(f"   Records: {len(large_data):,}")
        print(f"   Memory: {original_memory:.2f} MB")

        # Strategy 1: Type optimization
        print("\n1. Data Type Optimization:")
        optimized_schema = pa.schema(
            [
                pa.field("transaction_id", pa.string()),
                pa.field("timestamp", pa.timestamp("s")),
                pa.field("date", pa.string()),
                pa.field("year", pa.int16()),
                pa.field("quarter", pa.dictionary(pa.int8(), pa.string())),
                pa.field("month", pa.string()),
                pa.field("product", pa.dictionary(pa.int16(), pa.string())),
                pa.field("category", pa.dictionary(pa.int8(), pa.string())),
                pa.field("quantity", pa.int16()),
                pa.field("unit_price", pa.float32()),
                pa.field("discount_percent", pa.float32()),
                pa.field("total_amount", pa.float64()),
                pa.field("discount_amount", pa.float32()),
                pa.field("net_amount", pa.float64()),
                pa.field("commission_rate", pa.float32()),
                pa.field("commission", pa.float32()),
                pa.field("region", pa.dictionary(pa.int8(), pa.string())),
                pa.field("country", pa.dictionary(pa.int16(), pa.string())),
                pa.field("channel", pa.dictionary(pa.int8(), pa.string())),
                pa.field("customer_segment", pa.dictionary(pa.int8(), pa.string())),
                pa.field("customer_id", pa.string()),
                pa.field("sales_rep_id", pa.string()),
                pa.field("campaign_id", pa.string()),
                pa.field("is_returned", pa.bool_()),
                pa.field("return_reason", pa.dictionary(pa.int8(), pa.string())),
            ]
        )

        try:
            optimized_data = large_data.cast(optimized_schema)
            optimized_memory = optimized_data.nbytes / 1024 / 1024
            memory_reduction = (
                (original_memory - optimized_memory) / original_memory
            ) * 100

            print(f"   Optimized memory: {optimized_memory:.2f} MB")
            print(f"   Memory reduction: {memory_reduction:.1f}%")
            print(f"   Memory saved: {original_memory - optimized_memory:.2f} MB")

        except Exception as e:
            print(f"   ⚠️  Type optimization failed: {e}")
            optimized_data = large_data
            optimized_memory = original_memory

        # Strategy 2: Column selection
        print("\n2. Column Selection (Projection):")
        essential_columns = [
            "transaction_id",
            "date",
            "product",
            "quantity",
            "unit_price",
            "net_amount",
            "region",
            "channel",
        ]

        start_time = time.time()
        selected_data = optimized_data.select(essential_columns)
        selection_time = time.time() - start_time

        selected_memory = selected_data.nbytes / 1024 / 1024
        projection_reduction = (
            (optimized_memory - selected_memory) / optimized_memory
        ) * 100

        print(
            f"   Selected {len(essential_columns)}/{len(optimized_data.schema)} columns"
        )
        print(f"   Memory after projection: {selected_memory:.2f} MB")
        print(f"   Projection reduction: {projection_reduction:.1f}%")
        print(f"   Selection time: {selection_time:.4f}s")

        # Strategy 3: Row filtering
        print("\n3. Row Filtering:")
        start_time = time.time()

        # Filter high-value transactions
        high_value_filter = pc.greater(selected_data.column("net_amount"), 1000)
        filtered_data = selected_data.filter(high_value_filter)

        filtering_time = time.time() - start_time
        filter_reduction = (1 - len(filtered_data) / len(selected_data)) * 100

        filtered_memory = filtered_data.nbytes / 1024 / 1024

        print(f"   Filtered for net_amount > $1000")
        print(
            f"   Records reduced: {len(selected_data):,} -> {len(filtered_data):,} ({filter_reduction:.1f}%)"
        )
        print(f"   Memory after filtering: {filtered_memory:.2f} MB")
        print(f"   Filtering time: {filtering_time:.4f}s")

        # Strategy 4: Chunked processing
        print("\n4. Chunked Processing:")
        chunk_size = 10000
        chunk_results = []

        start_time = time.time()

        for i in range(0, len(optimized_data), chunk_size):
            chunk = optimized_data.slice(i, min(chunk_size, len(optimized_data) - i))

            # Process chunk (e.g., simple aggregation)
            chunk_sum = pc.sum(chunk.column("net_amount")).as_py()
            chunk_results.append(chunk_sum)

        chunked_time = time.time() - start_time
        total_processed = sum(chunk_results)

        print(f"   Processed in {len(chunk_results)} chunks of {chunk_size:,} records")
        print(f"   Total net_amount sum: ${total_processed:,.2f}")
        print(f"   Chunked processing time: {chunked_time:.4f}s")
        print(f"   Average chunk time: {chunked_time / len(chunk_results):.4f}s")

        # Memory efficiency summary
        print(f"\n💡 Memory Efficiency Summary:")
        print(f"   Original dataset: {original_memory:.2f} MB")
        print(
            f"   Type optimized:  {optimized_memory:.2f} MB ({((original_memory - optimized_memory) / original_memory) * 100:.1f}% reduction)"
        )
        print(
            f"   Column projected: {selected_memory:.2f} MB ({((optimized_memory - selected_memory) / optimized_memory) * 100:.1f}% reduction)"
        )
        print(
            f"   Row filtered:    {filtered_memory:.2f} MB ({((selected_memory - filtered_memory) / selected_memory) * 100:.1f}% reduction)"
        )

    except Exception as e:
        print(f"❌ Memory optimization demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_parallel_processing():
    """Demonstrate parallel processing for CPU-intensive operations."""

    print("\n⚡ Parallel Processing Optimization")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create test dataset
        test_data = create_large_analytics_dataset()
        data_file = temp_dir / "analytics.parquet"
        pq.write_table(test_data, data_file)

        # CPU-intensive operation: complex analytics
        def complex_analytics(chunk_data):
            """Perform complex analytics on data chunk."""
            if len(chunk_data) == 0:
                return {"records": 0, "avg_amount": 0, "high_value_count": 0}

            # Multiple calculations
            avg_amount = pc.mean(chunk_data.column("net_amount")).as_py()
            high_value_count = pc.sum(
                pc.greater(chunk_data.column("net_amount"), 1000)
            ).as_py()

            # Complex calculation (price * quantity correlation)
            correlation_data = pc.multiply(
                chunk_data.column("unit_price"), chunk_data.column("quantity")
            )
            avg_correlation = pc.mean(correlation_data).as_py()

            # Category-wise analysis
            categories = pc.unique(chunk_data.column("category"))
            category_counts = {}

            for category in categories.to_pylist():
                if category:
                    category_filter = pc.equal(chunk_data.column("category"), category)
                    category_data = chunk_data.filter(category_filter)
                    category_counts[category] = len(category_data)

            return {
                "records": len(chunk_data),
                "avg_amount": avg_amount,
                "high_value_count": high_value_count,
                "avg_correlation": avg_correlation,
                "category_counts": category_counts,
            }

        # Test different worker counts
        chunk_size = 5000
        chunks = []
        for i in range(0, len(test_data), chunk_size):
            chunk = test_data.slice(i, min(chunk_size, len(test_data) - i))
            chunks.append(chunk)

        print(f"Created {len(chunks)} chunks of ~{chunk_size:,} records")

        worker_counts = [1, 2, 4, 8]
        results = {}

        for workers in worker_counts:
            print(f"\nTesting with {workers} workers:")

            start_time = time.time()
            worker_results = run_parallel(complex_analytics, chunks, n_jobs=workers)
            total_time = time.time() - start_time

            # Aggregate results
            total_records = sum(r["records"] for r in worker_results)
            avg_amounts = [
                r["avg_amount"] for r in worker_results if r["avg_amount"] > 0
            ]
            high_value_total = sum(r["high_value_count"] for r in worker_results)

            print(f"   Processing time: {total_time:.4f}s")
            print(f"   Total records processed: {total_records:,}")
            print(f"   High-value transactions: {high_value_total:,}")
            print(f"   Throughput: {total_records / total_time:.0f} records/s")

            results[workers] = {
                "time": total_time,
                "throughput": total_records / total_time,
                "records": total_records,
            }

        # Calculate speedup
        if results[1]["time"] > 0:
            speedup_4_workers = results[1]["time"] / results[4]["time"]
            speedup_8_workers = results[1]["time"] / results[8]["time"]

            print(f"\n📊 Parallel Processing Results:")
            print(f"   4 workers speedup: {speedup_4_workers:.2f}x")
            print(f"   8 workers speedup: {speedup_8_workers:.2f}x")

            # Determine optimal worker count
            best_throughput = max(results.values(), key=lambda x: x["throughput"])
            optimal_workers = [
                workers
                for workers, result in results.items()
                if result["throughput"] == best_throughput["throughput"]
            ][0]

            print(
                f"   Optimal workers: {optimal_workers} ({best_throughput['throughput']:.0f} records/s)"
            )

    except Exception as e:
        print(f"❌ Parallel processing demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_io_optimization():
    """Demonstrate I/O optimization techniques."""

    print("\n💾 I/O Optimization Techniques")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create test data
        analytics_data = create_large_analytics_dataset()
        dataset_path = temp_dir / "io_test"
        dataset_path.mkdir()

        print(f"\n📊 Testing with {len(analytics_data):,} records")

        # Test 1: File organization impact
        print("\n1. File Organization Impact:")

        # Single large file
        single_file = dataset_path / "single_file.parquet"
        start_time = time.time()
        pq.write_table(analytics_data, single_file)
        single_write_time = time.time() - start_time

        start_time = time.time()
        single_read = pq.read_table(single_file)
        single_read_time = time.time() - start_time

        print(
            f"   Single file: Write {single_write_time:.3f}s, Read {single_read_time:.3f}s"
        )

        # Multiple smaller files
        multi_dir = dataset_path / "multi_files"
        multi_dir.mkdir()

        chunk_size = 10000
        multi_write_times = []
        multi_read_times = []

        start_time = time.time()
        for i in range(0, len(analytics_data), chunk_size):
            chunk = analytics_data.slice(i, min(chunk_size, len(analytics_data) - i))
            file_path = multi_dir / f"chunk_{i // chunk_size:03d}.parquet"
            pq.write_table(chunk, file_path)
        multi_write_time = time.time() - start_time

        start_time = time.time()
        multi_files = list(multi_dir.glob("*.parquet"))
        multi_tables = [pq.read_table(f) for f in multi_files]
        multi_combined = pa.concat_tables(multi_tables)
        multi_read_time = time.time() - start_time

        print(
            f"   Multiple files: Write {multi_write_time:.3f}s, Read {multi_read_time:.3f}s"
        )

        # Test 2: Compression codecs
        print("\n2. Compression Codec Impact:")

        codecs = ["snappy", "gzip", "brotli"]
        compression_results = {}

        for codec in codecs:
            codec_file = dataset_path / f"test_{codec}.parquet"

            start_time = time.time()
            pq.write_table(analytics_data, codec_file, compression=codec)
            write_time = time.time() - start_time

            file_size = codec_file.stat().st_size

            start_time = time.time()
            compressed_read = pq.read_table(codec_file)
            read_time = time.time() - start_time

            compression_ratio = file_size / single_file.stat().st_size

            compression_results[codec] = {
                "write_time": write_time,
                "read_time": read_time,
                "file_size": file_size,
                "compression_ratio": compression_ratio,
            }

            print(
                f"   {codec}: {write_time:.3f}s write, {read_time:.3f}s read, {compression_ratio:.3f}x compression"
            )

        # Test 3: Partitioning for query performance
        print("\n3. Partitioning Strategy:")

        # Create partitioned dataset
        partitioned_path = dataset_path / "partitioned"
        partitioned_path.mkdir()

        # Partition by quarter and region
        for quarter in ["Q12024", "Q22024", "Q32024", "Q42024"]:
            for region in ["North America", "Europe", "Asia Pacific"]:
                quarter_filter = pc.equal(analytics_data.column("quarter"), quarter)
                region_filter = pc.equal(analytics_data.column("region"), region)
                combined_filter = pc.and_(quarter_filter, region_filter)

                partition_data = analytics_data.filter(combined_filter)

                if len(partition_data) > 0:
                    partition_dir = (
                        partitioned_path / f"quarter={quarter}" / f"region={region}"
                    )
                    partition_dir.mkdir(parents=True, exist_ok=True)
                    pq.write_table(partition_data, partition_dir / "data.parquet")

        partition_files = list(partitioned_path.rglob("*.parquet"))
        print(f"   Created {len(partition_files)} partitioned files")

        # Test selective read performance
        start_time = time.time()
        partitioned_tables = [pq.read_table(f) for f in partition_files]
        partitioned_combined = pa.concat_tables(partitioned_tables)
        partitioned_read_time = time.time() - start_time

        print(f"   Partitioned read: {partitioned_read_time:.3f}s")

        # Test selective partition reading (simulating pruning)
        start_time = time.time()
        target_files = list(partitioned_path.glob("quarter=Q22024/**/*.parquet"))
        selective_tables = [pq.read_table(f) for f in target_files]
        selective_combined = pa.concat_tables(selective_tables)
        selective_read_time = time.time() - start_time

        files_skipped = len(partition_files) - len(target_files)
        theoretical_savings = (files_skipped / len(partition_files)) * 100

        print(f"   Selective read (Q2 only): {selective_read_time:.3f}s")
        print(
            f"   Files skipped: {files_skipped}/{len(partition_files)} ({theoretical_savings:.1f}% theoretical savings)"
        )

        # I/O Optimization Summary
        print(f"\n💡 I/O Optimization Summary:")

        best_codec = min(
            compression_results.items(), key=lambda x: x[1]["compression_ratio"]
        )
        print(
            f"   Best compression: {best_codec[0]} ({best_codec[1]['compression_ratio']:.2f}x ratio)"
        )

        if single_read_time > 0:
            partition_improvement = (
                (single_read_time - partitioned_read_time) / single_read_time
            ) * 100
            print(f"   Partitioning improvement: {partition_improvement:.1f}%")

        print(f"\n🎯 I/O Optimization Recommendations:")
        print("   • Use snappy for best balance of speed vs compression")
        print("   • Partition by frequently filtered columns")
        print("   • Choose appropriate file sizes (100MB-500MB per file)")
        print("   • Consider columnar storage formats for analytics")
        print("   • Use compression for storage efficiency")

    except Exception as e:
        print(f"❌ I/O optimization demo failed: {e}")
        raise

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_monitoring():
    """Demonstrate performance monitoring and profiling."""

    print("\n📊 Performance Monitoring and Profiling")

    try:
        # Monitor system resources
        process = psutil.Process()

        print(f"\n🖥️  System Resource Monitoring:")
        print(f"   CPU Cores: {psutil.cpu_count()}")
        print(f"   Memory Total: {psutil.virtual_memory().total / 1024 / 1024:.1f} MB")
        print(
            f"   Memory Available: {psutil.virtual_memory().available / 1024 / 1024:.1f} MB"
        )
        print(f"   Memory Used: {psutil.virtual_memory().percent:.1f}%")

        # Create monitoring function
        def monitor_operation(operation_func, operation_name):
            """Monitor an operation with detailed metrics."""

            print(f"\n🔍 Monitoring: {operation_name}")

            # Baseline metrics
            baseline_cpu = process.cpu_percent(interval=1)
            baseline_memory = process.memory_info().rss / 1024 / 1024
            baseline_threads = process.num_threads()

            start_time = time.time()

            # Execute operation
            result = operation_func()

            end_time = time.time()

            # Final metrics
            final_cpu = process.cpu_percent()
            final_memory = process.memory_info().rss / 1024 / 1024
            final_threads = process.num_threads()

            execution_time = end_time - start_time
            cpu_usage = (baseline_cpu + final_cpu) / 2
            memory_delta = final_memory - baseline_memory
            thread_delta = final_threads - baseline_threads

            print(f"   Execution time: {execution_time:.4f}s")
            print(f"   CPU usage: {cpu_usage:.1f}%")
            print(f"   Memory delta: {memory_delta:+.1f} MB")
            print(f"   Thread delta: {thread_delta:+d}")

            # Calculate efficiency metrics
            if hasattr(result, "__len__"):
                throughput = len(result) / execution_time
                memory_per_record = memory_delta / len(result) if len(result) > 0 else 0
                print(f"   Throughput: {throughput:.0f} records/s")
                print(f"   Memory per record: {memory_per_record:.4f} MB")

            return {
                "execution_time": execution_time,
                "cpu_usage": cpu_usage,
                "memory_delta": memory_delta,
                "thread_delta": thread_delta,
                "result_size": len(result) if hasattr(result, "__len__") else 0,
            }

        # Test different operations
        print("\n📈 Monitoring Different Operations:")

        # Operation 1: Simple read
        test_data = create_large_analytics_dataset()
        temp_dir = Path(tempfile.mkdtemp())
        test_file = temp_dir / "test.parquet"
        pq.write_table(test_data, test_file)

        read_results = monitor_operation(
            lambda: pq.read_table(test_file), "Simple Parquet Read"
        )

        # Operation 2: Complex filter
        filter_results = monitor_operation(
            lambda: test_data.filter(
                pc.and_(
                    pc.greater(test_data.column("net_amount"), 1000),
                    pc.equal(test_data.column("year"), 2024),
                )
            ),
            "Complex Filtering",
        )

        # Operation 3: Aggregation
        agg_results = monitor_operation(
            lambda: test_data.group_by("region").aggregate(
                [("net_amount", "sum"), ("quantity", "mean")]
            ),
            "Group Aggregation",
        )

        # Operation 4: Type conversion
        conversion_results = monitor_operation(
            lambda: test_data.cast(
                pa.schema(
                    [
                        pa.field("net_amount", pa.float32()),
                        pa.field("quantity", pa.int16()),
                    ]
                )
            ),
            "Type Conversion",
        )

        # Performance comparison
        print(f"\n📊 Performance Comparison:")
        operations = [
            ("Read", read_results),
            ("Filter", filter_results),
            ("Aggregation", agg_results),
            ("Conversion", conversion_results),
        ]

        for op_name, results in operations:
            if "error" not in results:
                print(
                    f"   {op_name:12} {results['execution_time']:.4f}s | "
                    f"CPU: {results['cpu_usage']:5.1f}% | "
                    f"Memory: {results['memory_delta']:+6.1f}MB | "
                    f"Size: {results['result_size']:6}"
                )

        import shutil

        shutil.rmtree(temp_dir)

        print(f"\n💡 Monitoring Best Practices:")
        print("   • Monitor memory usage to detect leaks")
        print("   • Track CPU usage for optimization opportunities")
        print("   • Profile with realistic data sizes")
        print("   • Consider system resource constraints")
        print("   • Use logging for production monitoring")

    except Exception as e:
        print(f"❌ Monitoring demo failed: {e}")


def main():
    """Run all performance optimization examples."""

    print("⚡ Performance Optimization Workflow Example")
    print("=" * 60)
    print("This example demonstrates advanced performance optimization")
    print("techniques for large-scale dataset operations.")

    try:
        # Run all performance demonstrations
        demonstrate_query_optimization()
        demonstrate_memory_optimization()
        demonstrate_parallel_processing()
        demonstrate_io_optimization()
        demonstrate_monitoring()

        print("\n" + "=" * 60)
        print("✅ Performance optimization completed successfully!")

        print("\n🎯 Key Performance Takeaways:")
        print("• Query optimization: Project early, filter early")
        print("• Memory optimization: Use appropriate types and projections")
        print("• Parallel processing: Scale with CPU cores, not threads")
        print("• I/O optimization: Choose appropriate compression and partitioning")
        print("• Monitoring: Profile realistic workloads and track resources")
        print("• Balance: Optimize for your specific use case and constraints")

        print("\n🔗 Related Examples:")
        print("• Cloud datasets: Cloud-specific performance considerations")
        print("• Advanced workflows: Production-scale optimizations")
        print("• Cross-domain integration: End-to-end performance")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()
