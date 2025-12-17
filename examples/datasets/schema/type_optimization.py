"""
Data Type Optimization Example

This example demonstrates advanced data type optimization techniques
for improving storage efficiency, query performance, and memory usage
in PyArrow datasets.

The example covers:
1. Automatic type detection and optimization
2. Large type handling and categorization
3. Dictionary encoding for repeated values
4. Memory layout optimization
5. Storage format optimization
6. Performance impact measurement

This example is designed for users who need to optimize large datasets
for better performance and reduced storage costs.
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from fsspeckit.datasets import opt_dtype_pa, cast_schema


def create_realistic_sample_data() -> pa.Table:
    """Create realistic sample data with optimization opportunities."""

    print("Creating realistic sample data...")

    import random
    from datetime import datetime, timedelta

    # Generate diverse data that can be optimized
    records = []
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Toys"]
    brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]
    statuses = ["active", "inactive", "discontinued"]
    regions = ["North", "South", "East", "West", "Central"]

    for i in range(10000):  # 10K records
        record = {
            "product_id": f"PROD-{i:06d}",
            "name": f"Product {i}",
            "category": random.choice(categories),
            "brand": random.choice(brands),
            "price": round(random.uniform(10.0, 1000.0), 2),
            "cost": round(random.uniform(5.0, 500.0), 2),
            "quantity": random.randint(1, 1000),
            "reorder_level": random.randint(10, 100),
            "weight": round(random.uniform(0.1, 50.0), 2),
            "status": random.choice(statuses),
            "region": random.choice(regions),
            "created_date": (
                datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
            ).strftime("%Y-%m-%d"),
            "last_modified": (
                datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d"),
            "rating": round(random.uniform(1.0, 5.0), 1),
            "review_count": random.randint(0, 1000),
            "is_featured": random.choice([True, False]),
            "discount_percent": round(random.uniform(0.0, 50.0), 1),
            "supplier_id": f"SUP-{random.randint(1, 100):03d}",
        }
        records.append(record)

    # Convert list of dicts to table using proper PyArrow API
    if records:
        # Extract column names from first record
        column_names = list(records[0].keys())
        # Convert to columnar format
        columns = {}
        for name in column_names:
            columns[name] = [record[name] for record in records]
        table = pa.table(columns)
    else:
        table = pa.table({})
    print(f"Created dataset with {len(table):,} rows and {len(table.schema)} columns")
    return table


def analyze_current_types(table: pa.Table) -> dict:
    """Analyze current data types and optimization opportunities."""

    print("\n🔍 Current Data Type Analysis")

    analysis = {"total_rows": len(table), "total_memory": table.nbytes, "columns": []}

    for field in table.schema:
        column = table.column(field.name)
        memory_usage = column.nbytes
        null_count = column.null_count

        column_info = {
            "name": field.name,
            "type": str(field.type),
            "memory_mb": memory_usage / (1024 * 1024),
            "null_percentage": (null_count / len(column)) * 100
            if len(column) > 0
            else 0,
        }

        # Add specific analysis based on type
        if pa.types.is_integer(field.type):
            if field.name in ["quantity", "reorder_level", "review_count"]:
                min_max_result = pc.min_max(column).as_py()
                min_val = int(min_max_result["min"])
                max_val = int(min_max_result["max"])
                column_info["range"] = f"{min_val} to {max_val}"
                column_info["optimal_type"] = suggest_optimal_int_type(min_val, max_val)
            else:
                column_info["optimizable"] = "Check range for smaller int type"

        elif pa.types.is_floating(field.type):
            if field.name in ["price", "cost", "weight", "discount_percent"]:
                column_info["optimizable"] = "Consider float32 for storage efficiency"
                column_info["precision_check"] = (
                    "Check if decimal precision is required"
                )

        elif pa.types.is_string(field.type):
            unique_count = len(pc.unique(column))
            cardinality = (unique_count / len(column)) * 100

            column_info["unique_values"] = unique_count
            column_info["cardinality_percent"] = cardinality

            if cardinality < 50:  # Low cardinality
                column_info["optimizable"] = (
                    f"Dictionary encoding (low cardinality: {cardinality:.1f}%)"
                )
            elif len(str(column)) > 1000:  # Long strings
                column_info["optimizable"] = (
                    "Check for string compression opportunities"
                )

        analysis["columns"].append(column_info)

    # Sort by memory usage
    analysis["columns"].sort(key=lambda x: x["memory_mb"], reverse=True)

    print(f"Dataset Overview:")
    print(f"  Total rows: {analysis['total_rows']:,}")
    print(f"  Total memory: {analysis['total_memory'] / (1024 * 1024):.2f} MB")
    print(f"  Columns: {len(analysis['columns'])}")

    print(f"\nTop 10 columns by memory usage:")
    for i, col in enumerate(analysis["columns"][:10], 1):
        print(
            f"  {i:2d}. {col['name']:<20} {col['type']:<25} {col['memory_mb']:>6.2f} MB"
        )

    return analysis


def suggest_optimal_int_type(min_val: int, max_val: int) -> str:
    """Suggest the optimal integer type for a given range."""

    if min_val >= -128 and max_val <= 127:
        return "int8"
    elif min_val >= 0 and max_val <= 255:
        return "uint8"
    elif min_val >= -32768 and max_val <= 32767:
        return "int16"
    elif min_val >= 0 and max_val <= 65535:
        return "uint16"
    elif min_val >= -2147483648 and max_val <= 2147483647:
        return "int32"
    elif min_val >= 0 and max_val <= 4294967295:
        return "uint32"
    else:
        return "int64"


def demonstrate_integer_optimization(table: pa.Table) -> pa.Table:
    """Demonstrate integer type optimization."""

    print("\n🔢 Integer Type Optimization")

    integer_columns = []
    for field in table.schema:
        if pa.types.is_integer(field.type):
            integer_columns.append(field.name)

    print(f"Found {len(integer_columns)} integer columns")

    optimized_schema = table.schema
    optimizations = []

    for col_name in integer_columns:
        column = table.column(col_name)
        min_max_result = pc.min_max(column).as_py()
        min_val = int(min_max_result["min"])
        max_val = int(min_max_result["max"])
        current_type = str(table.schema.field(col_name).type)
        optimal_type = suggest_optimal_int_type(min_val, max_val)

        if optimal_type != current_type.split("[")[0]:  # Extract base type
            # Replace with optimized type
            for i, field in enumerate(optimized_schema):
                if field.name == col_name:
                    # Map string type names to pa.type calls
                    type_mapping = {
                        "int8": pa.int8(),
                        "uint8": pa.uint8(),
                        "int16": pa.int16(),
                        "uint16": pa.uint16(),
                        "int32": pa.int32(),
                        "uint32": pa.uint32(),
                        "int64": pa.int64(),
                        "uint64": pa.uint64(),
                    }
                    optimized_field = pa.field(
                        field.name,
                        type_mapping.get(optimal_type, pa.int64()),
                        nullable=field.nullable,
                    )
                    optimized_schema = optimized_schema.set(i, optimized_field)
                    optimizations.append(
                        {
                            "column": col_name,
                            "current": current_type,
                            "optimal": optimal_type,
                            "range": f"{min_val} to {max_val}",
                        }
                    )
                    break

    if optimizations:
        print("Optimizations found:")
        for opt in optimizations:
            print(
                f"  {opt['column']:<20} {opt['current']:<15} -> {opt['optimal']:<15} (range: {opt['range']})"
            )

        # Apply optimizations
        optimized_table = table.cast(optimized_schema)

        # Calculate memory savings
        original_memory = table.nbytes
        optimized_memory = optimized_table.nbytes
        savings = original_memory - optimized_memory

        print(f"\nMemory Impact:")
        print(f"  Original:  {original_memory / (1024 * 1024):.2f} MB")
        print(f"  Optimized: {optimized_memory / (1024 * 1024):.2f} MB")
        print(
            f"  Savings:   {savings / (1024 * 1024):.2f} MB ({(savings / original_memory) * 100:.1f}%)"
        )

        return optimized_table
    else:
        print("No integer optimizations needed")
        return table


def demonstrate_floating_point_optimization(table: pa.Table) -> pa.Table:
    """Demonstrate floating-point type optimization."""

    print("\n🔢 Floating-Point Type Optimization")

    float_columns = []
    for field in table.schema:
        if pa.types.is_floating(field.type):
            float_columns.append(field.name)

    print(f"Found {len(float_columns)} floating-point columns")

    optimized_schema = table.schema
    optimizations = []

    for col_name in float_columns:
        column = table.column(col_name)
        current_type = str(table.schema.field(col_name).type)

        # Check if we can safely convert to float32
        if current_type.startswith("double") or current_type.startswith("float64"):
            # Test a sample to see if float32 preserves precision
            sample = column.slice(0, min(1000, len(column)))
            converted = sample.cast(pa.float32())
            back_converted = converted.cast(pa.float64())

            # Check if conversion preserves values within acceptable tolerance
            diff = pc.abs(pc.subtract(sample, back_converted))
            max_diff = pc.max(diff).as_py() if len(diff) > 0 else 0

            if max_diff < 1e-6:  # Very small difference acceptable
                # Replace with float32
                for i, field in enumerate(optimized_schema):
                    if field.name == col_name:
                        optimized_field = pa.field(
                            field.name, pa.float32(), nullable=field.nullable
                        )
                        optimized_schema = optimized_schema.set(i, optimized_field)
                        optimizations.append(
                            {
                                "column": col_name,
                                "current": current_type,
                                "optimal": "float32",
                                "max_precision_loss": max_diff,
                            }
                        )
                        break

    if optimizations:
        print("Optimizations found:")
        for opt in optimizations:
            print(
                f"  {opt['column']:<20} {opt['current']:<15} -> {opt['optimal']:<15} (max loss: {opt['max_precision_loss']:.2e})"
            )

        # Apply optimizations
        optimized_table = table.cast(optimized_schema)

        # Calculate memory savings
        original_memory = table.nbytes
        optimized_memory = optimized_table.nbytes
        savings = original_memory - optimized_memory

        print(f"\nMemory Impact:")
        print(f"  Original:  {original_memory / (1024 * 1024):.2f} MB")
        print(f"  Optimized: {optimized_memory / (1024 * 1024):.2f} MB")
        print(
            f"  Savings:   {savings / (1024 * 1024):.2f} MB ({(savings / original_memory) * 100:.1f}%)"
        )

        return optimized_table
    else:
        print("No floating-point optimizations needed")
        return table


def demonstrate_dictionary_encoding(table: pa.Table) -> pa.Table:
    """Demonstrate dictionary encoding for string columns."""

    print("\n📚 Dictionary Encoding for Strings")

    string_columns = []
    for field in table.schema:
        if pa.types.is_string(field.type):
            string_columns.append(field.name)

    print(f"Found {len(string_columns)} string columns")

    optimized_schema = table.schema
    optimizations = []

    for col_name in string_columns:
        column = table.column(col_name)
        unique_values = pc.unique(column)
        unique_count = len(unique_values)
        total_count = len(column)
        cardinality = (unique_count / total_count) * 100

        current_memory = column.nbytes

        # Dictionary encoding is beneficial when cardinality is low (< 50%)
        if cardinality < 50:
            # Test dictionary encoding
            try:
                dict_array = pc.dictionary_encode(column)
                dict_memory = dict_array.nbytes
                savings = current_memory - dict_memory

                optimizations.append(
                    {
                        "column": col_name,
                        "unique_values": unique_count,
                        "total_values": total_count,
                        "cardinality_percent": cardinality,
                        "current_memory_mb": current_memory / (1024 * 1024),
                        "dict_memory_mb": dict_memory / (1024 * 1024),
                        "savings_mb": savings / (1024 * 1024),
                        "savings_percent": (savings / current_memory) * 100,
                    }
                )

                # Update schema
                for i, field in enumerate(optimized_schema):
                    if field.name == col_name:
                        dict_type = pa.dictionary(pa.int32(), pa.string())
                        optimized_field = pa.field(
                            field.name, dict_type, nullable=field.nullable
                        )
                        optimized_schema = optimized_schema.set(i, optimized_field)
                        break

            except Exception as e:
                print(f"  Could not dictionary encode {col_name}: {e}")

    if optimizations:
        print("Dictionary encoding opportunities:")
        for opt in optimizations:
            print(
                f"  {opt['column']:<20} {opt['cardinality_percent']:.1f}% cardinality -> {opt['savings_percent']:.1f}% savings"
            )

        total_savings = sum(opt["savings_mb"] for opt in optimizations)

        if total_savings > 0:
            print(f"\nTotal dictionary encoding savings: {total_savings:.2f} MB")

            # Apply dictionary encoding
            try:
                optimized_table = table.cast(optimized_schema)
                return optimized_table
            except Exception as e:
                print(f"Error applying dictionary encoding: {e}")
                return table

    else:
        print("No good candidates for dictionary encoding found")
        return table


def demonstrate_comprehensive_optimization(table: pa.Table) -> pa.Table:
    """Demonstrate comprehensive optimization combining all techniques."""

    print("\n⚡ Comprehensive Data Type Optimization")

    original_table = table
    original_memory = table.nbytes

    # Apply optimizations step by step
    print("Step 1: Integer optimization...")
    table = demonstrate_integer_optimization(table)

    print("\nStep 2: Floating-point optimization...")
    table = demonstrate_floating_point_optimization(table)

    print("\nStep 3: Dictionary encoding...")
    table = demonstrate_dictionary_encoding(table)

    final_memory = table.nbytes
    total_savings = original_memory - final_memory
    savings_percent = (total_savings / original_memory) * 100

    print(f"\n📊 Comprehensive Optimization Results:")
    print(f"  Original memory:  {original_memory / (1024 * 1024):.2f} MB")
    print(f"  Final memory:     {final_memory / (1024 * 1024):.2f} MB")
    print(
        f"  Total savings:    {total_savings / (1024 * 1024):.2f} MB ({savings_percent:.1f}%)"
    )

    # Compare schemas
    print(f"\n🔍 Schema Comparison:")
    print("Original schema:")
    for field in original_table.schema:
        print(f"  {field.name}: {field.type}")

    print("\nOptimized schema:")
    for field in table.schema:
        print(f"  {field.name}: {field.type}")

    return table


def benchmark_optimization_performance(
    original_table: pa.Table, optimized_table: pa.Table
):
    """Benchmark the performance impact of optimization."""

    print("\n⏱️  Performance Benchmarking")

    # Setup test queries
    test_queries = [
        (
            "filter_by_category",
            lambda t: t.filter(pc.equal(t.column("category"), "Electronics")),
        ),
        (
            "filter_by_price_range",
            lambda t: t.filter(
                pc.and_(
                    pc.greater_equal(t.column("price"), 100),
                    pc.less_equal(t.column("price"), 500),
                )
            ),
        ),
        (
            "aggregate_by_brand",
            lambda t: t.group_by("brand").aggregate(
                [("price", "mean"), ("quantity", "sum")]
            ),
        ),
        ("sort_by_price", lambda t: t.sort_by([("price", "descending")])),
    ]

    print("Running performance benchmarks...")

    for query_name, query_func in test_queries:
        print(f"\n🏃 Benchmarking: {query_name}")

        # Benchmark original table
        start_time = time.time()
        original_result = query_func(original_table)
        original_time = time.time() - start_time

        # Benchmark optimized table
        start_time = time.time()
        optimized_result = query_func(optimized_table)
        optimized_time = time.time() - start_time

        # Calculate improvement
        speedup = original_time / optimized_time if optimized_time > 0 else float("inf")
        improvement = ((original_time - optimized_time) / original_time) * 100

        print(f"  Original time:  {original_time:.4f}s")
        print(f"  Optimized time: {optimized_time:.4f}s")
        print(f"  Speedup:        {speedup:.2f}x")
        print(f"  Improvement:    {improvement:.1f}%")

        # Verify results are equivalent (for filters and sorts)
        if query_name in [
            "filter_by_category",
            "filter_by_price_range",
            "sort_by_price",
        ]:
            if len(original_result) == len(optimized_result):
                print(f"  ✅ Result sizes match: {len(original_result)} rows")
            else:
                print(
                    f"  ⚠️  Result size mismatch: {len(original_result)} vs {len(optimized_result)}"
                )


def demonstrate_storage_optimization(table: pa.Table):
    """Demonstrate storage format optimization."""

    print("\n💾 Storage Format Optimization")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Test different compression codecs
        codecs = ["none", "snappy", "gzip", "brotli", "zstd"]
        compression_results = []

        for codec in codecs:
            file_path = temp_dir / f"test_{codec}.parquet"

            start_time = time.time()
            pq.write_table(table, file_path, compression=codec)
            write_time = time.time() - start_time

            file_size = file_path.stat().st_size

            start_time = time.time()
            read_table = pq.read_table(file_path)
            read_time = time.time() - start_time

            compression_results.append(
                {
                    "codec": codec,
                    "write_time": write_time,
                    "read_time": read_time,
                    "file_size_mb": file_size / (1024 * 1024),
                    "compression_ratio": file_size / table.nbytes,
                }
            )

        print("Compression comparison:")
        print(
            f"{'Codec':<10} {'Write (s)':<10} {'Read (s)':<9} {'Size (MB)':<10} {'Ratio':<8}"
        )
        print("-" * 55)

        for result in compression_results:
            print(
                f"{result['codec']:<10} "
                f"{result['write_time']:<10.4f} "
                f"{result['read_time']:<9.4f} "
                f"{result['file_size_mb']:<10.2f} "
                f"{result['compression_ratio']:<8.3f}"
            )

        # Find best codec based on size/speed tradeoff
        best_codec = min(compression_results, key=lambda x: x["file_size_mb"])
        print(f"\n💡 Recommendation: Use '{best_codec['codec']}' for best compression")
        print(f"   Size reduction: {(1 - best_codec['compression_ratio']) * 100:.1f}%")

    finally:
        import shutil

        shutil.rmtree(temp_dir)


def main():
    """Run all type optimization examples."""

    print("⚡ Data Type Optimization Example")
    print("=" * 60)

    # Create realistic test data
    original_table = create_realistic_sample_data()

    try:
        # Analyze current types
        analysis = analyze_current_types(original_table)

        # Demonstrate individual optimization techniques
        integer_optimized = demonstrate_integer_optimization(original_table)
        float_optimized = demonstrate_floating_point_optimization(original_table)
        dict_optimized = demonstrate_dictionary_encoding(original_table)

        # Demonstrate comprehensive optimization
        fully_optimized = demonstrate_comprehensive_optimization(original_table)

        # Benchmark performance
        benchmark_optimization_performance(original_table, fully_optimized)

        # Test storage optimization
        demonstrate_storage_optimization(fully_optimized)

        print("\n" + "=" * 60)
        print("✅ All type optimization examples completed!")

        # Summary
        original_memory = original_table.nbytes
        final_memory = fully_optimized.nbytes
        total_savings = (original_memory - final_memory) / (1024 * 1024)

        print(f"\n📋 Optimization Summary:")
        print(f"  Dataset size: {len(original_table):,} rows")
        print(
            f"  Memory savings: {total_savings:.2f} MB ({((original_memory - final_memory) / original_memory) * 100:.1f}%)"
        )
        print(
            f"  Columns optimized: {len([c for c in analysis['columns'] if 'optimizable' in c])}"
        )

    finally:
        print(f"\n🧹 Optimization analysis complete")


if __name__ == "__main__":
    main()
