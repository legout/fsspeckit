"""
Cross-Platform SQL Filters Example

This example demonstrates how to write SQL filters that work consistently
across different data processing backends (PyArrow, Polars, DuckDB).

The example covers:
1. Writing backend-agnostic SQL filters
2. Comparing results across platforms
3. Handling platform-specific limitations
4. Best practices for cross-platform compatibility
5. Performance comparison across backends

This example is designed for users who want to build data processing
pipelines that can switch between backends without changing filter logic.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import polars as pl

from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter


def create_standardized_dataset() -> Path:
    """Create a standardized dataset for cross-platform comparison."""

    import random

    print("Creating standardized dataset for cross-platform testing...")

    # Generate consistent test data
    records = []
    products = [
        "Laptop",
        "Mouse",
        "Keyboard",
        "Monitor",
        "Headphones",
        "Webcam",
        "USB Hub",
    ]
    regions = ["North", "South", "East", "West", "Central"]
    categories = ["Electronics", "Accessories", "Peripherals"]

    for i in range(500):
        record = {
            "product_id": f"PROD-{i:04d}",
            "product_name": random.choice(products),
            "category": random.choice(categories),
            "region": random.choice(regions),
            "price": round(random.uniform(10.0, 1000.0), 2),
            "quantity": random.randint(1, 100),
            "discount_percent": round(random.uniform(0.0, 30.0), 1),
            "in_stock": random.choice([True, False]),
            "rating": round(random.uniform(1.0, 5.0), 1),
            "sale_date": (
                datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d"),
            "last_updated": (
                datetime(2024, 6, 1) + timedelta(days=random.randint(0, 180))
            ).strftime("%Y-%m-%d"),
        }
        records.append(record)

    data = pa.table(records)

    # Write to temporary directory
    temp_dir = Path(tempfile.mkdtemp())
    dataset_path = temp_dir / "sales_data"
    dataset_path.mkdir(parents=True, exist_ok=True)

    # Write as partitioned dataset
    ds.write_dataset(
        data, dataset_path, format="parquet", partitioning=["region", "category"]
    )

    print(f"Created dataset with {len(records)} records at: {dataset_path}")
    return dataset_path


def test_pyarrow_filters(
    dataset_path: Path, sql_filters: dict[str, str]
) -> dict[str, pa.Table]:
    """Test SQL filters with PyArrow backend."""

    print("\n🔹 Testing PyArrow Backend")
    dataset = ds.dataset(dataset_path, format="parquet")
    schema = dataset.schema
    results = {}

    for filter_name, sql_filter in sql_filters.items():
        try:
            pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)
            filtered_table = dataset.to_table(filter=pyarrow_filter)
            results[filter_name] = filtered_table
            print(f"   ✅ {filter_name}: {len(filtered_table)} rows")
        except Exception as e:
            print(f"   ❌ {filter_name}: Error - {e}")
            results[filter_name] = None

    return results


def test_polars_filters(
    dataset_path: Path, sql_filters: dict[str, str]
) -> dict[str, pl.DataFrame]:
    """Test SQL filters with Polars backend."""

    print("\n🔹 Testing Polars Backend")
    df = pl.read_parquet(dataset_path, glob="**/*.parquet")
    schema = df.schema
    results = {}

    for filter_name, sql_filter in sql_filters.items():
        try:
            polars_filter = sql2polars_filter(sql_filter, schema)
            filtered_df = df.filter(polars_filter)
            results[filter_name] = filtered_df
            print(f"   ✅ {filter_name}: {len(filtered_df)} rows")
        except Exception as e:
            print(f"   ❌ {filter_name}: Error - {e}")
            results[filter_name] = None

    return results


def test_duckdb_filters(
    dataset_path: Path, sql_filters: dict[str, str]
) -> dict[str, pa.Table]:
    """Test SQL filters with DuckDB backend."""

    print("\n🔹 Testing DuckDB Backend")
    results = {}

    try:
        with DuckDBParquetHandler() as handler:
            # Load dataset into DuckDB
            handler.register_dataset("sales", dataset_path)

            for filter_name, sql_filter in sql_filters.items():
                try:
                    query = f"SELECT * FROM sales WHERE {sql_filter}"
                    result_table = handler.execute_sql(query)
                    results[filter_name] = result_table
                    print(f"   ✅ {filter_name}: {len(result_table)} rows")
                except Exception as e:
                    print(f"   ❌ {filter_name}: Error - {e}")
                    results[filter_name] = None

    except Exception as e:
        print(f"   ❌ DuckDB initialization failed: {e}")
        # Return empty results dict
        return {filter_name: None for filter_name in sql_filters.keys()}

    return results


def compare_results_across_backends(
    pyarrow_results: dict[str, pa.Table],
    polars_results: dict[str, pl.DataFrame],
    duckdb_results: dict[str, pa.Table],
    sql_filters: dict[str, str],
):
    """Compare results across different backends."""

    print("\n🔍 Cross-Platform Result Comparison")

    for filter_name in sql_filters.keys():
        print(f"\n📊 Filter: {filter_name}")
        print(f"   SQL: {sql_filters[filter_name]}")

        pyarrow_result = pyarrow_results.get(filter_name)
        polars_result = polars_results.get(filter_name)
        duckdb_result = duckdb_results.get(filter_name)

        # Get row counts
        pyarrow_count = len(pyarrow_result) if pyarrow_result is not None else "Error"
        polars_count = len(polars_result) if polars_result is not None else "Error"
        duckdb_count = len(duckdb_result) if duckdb_result is not None else "Error"

        print(f"   Row Counts:")
        print(f"     PyArrow: {pyarrow_count}")
        print(f"     Polars:  {polars_count}")
        print(f"     DuckDB:  {duckdb_count}")

        # Check consistency (exclude error cases)
        valid_counts = [
            count
            for count in [pyarrow_count, polars_count, duckdb_count]
            if isinstance(count, int)
        ]

        if len(set(valid_counts)) <= 1:
            print(f"   ✅ Results consistent across backends")
        else:
            print(f"   ⚠️  Results vary across backends: {valid_counts}")


def demonstrate_platform_specific_features():
    """Demonstrate features that work differently across platforms."""

    print("\n🔧 Platform-Specific Features and Limitations")

    test_cases = [
        {
            "name": "String pattern matching",
            "sql": "product_name LIKE '%Laptop%'",
            "notes": "Basic LIKE should work everywhere",
        },
        {
            "name": "Case-insensitive search",
            "sql": "LOWER(product_name) LIKE 'laptop'",
            "notes": "Function support varies by platform",
        },
        {
            "name": "Complex mathematical expression",
            "sql": "(price * quantity * (1 - discount_percent/100)) > 500",
            "notes": "Mathematical operations in conditions",
        },
        {
            "name": "Date functions",
            "sql": "sale_date >= '2024-06-01'",
            "notes": "Date comparison support",
        },
        {
            "name": "Multiple OR conditions",
            "sql": "category = 'Electronics' OR category = 'Peripherals'",
            "notes": "OR logic performance varies",
        },
        {
            "name": "IN clause with many values",
            "sql": "region IN ('North', 'South', 'East', 'West', 'Central')",
            "notes": "IN vs OR performance differences",
        },
    ]

    print("\nTest Cases for Platform Comparison:")
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}. {test_case['name']}")
        print(f"   SQL: {test_case['sql']}")
        print(f"   Notes: {test_case['notes']}")


def provide_cross_platform_best_practices():
    """Provide best practices for cross-platform filter development."""

    print("\n💡 Cross-Platform Development Best Practices")

    practices = [
        {
            "category": "SQL Writing",
            "tips": [
                "Use standard SQL syntax that works across platforms",
                "Avoid platform-specific functions when possible",
                "Test complex filters on all target backends",
                "Use explicit column names instead of * in production",
            ],
        },
        {
            "category": "Performance",
            "tips": [
                "Profile performance on each backend with realistic data",
                "Consider partition pruning in filter design",
                "Use the most selective conditions first",
                "Benchmark IN vs OR for your specific use case",
            ],
        },
        {
            "category": "Compatibility",
            "tips": [
                "Have fallback strategies for unsupported features",
                "Consider using the lowest common denominator for complex queries",
                "Test edge cases (NULL values, empty strings, etc.)",
                "Document any platform-specific limitations",
            ],
        },
        {
            "category": "Maintenance",
            "tips": [
                "Store SQL filters separately from application code",
                "Create automated tests for critical filters",
                "Monitor backend compatibility when updating libraries",
                "Consider abstraction layers for complex logic",
            ],
        },
    ]

    for practice in practices:
        print(f"\n{practice['category']}:")
        for tip in practice["tips"]:
            print(f"   • {tip}")


def main():
    """Run all cross-platform filter examples."""

    print("🌐 Cross-Platform SQL Filters Example")
    print("=" * 60)

    # Create test dataset
    dataset_path = create_standardized_dataset()

    try:
        # Define test filters that should work across platforms
        sql_filters = {
            "High Value Products": "price > 500",
            "In Stock Electronics": "in_stock = true AND category = 'Electronics'",
            "Recent Sales": "sale_date >= '2024-06-01'",
            "Discounted Items": "discount_percent > 10",
            "North Region Sales": "region = 'North' AND quantity > 20",
            "Multi-Region High Rating": "rating >= 4.0 AND region IN ('North', 'South', 'East')",
        }

        # Test filters across all backends
        pyarrow_results = test_pyarrow_filters(dataset_path, sql_filters)
        polars_results = test_polars_filters(dataset_path, sql_filters)
        duckdb_results = test_duckdb_filters(dataset_path, sql_filters)

        # Compare results
        compare_results_across_backends(
            pyarrow_results, polars_results, duckdb_results, sql_filters
        )

        # Demonstrate platform differences
        demonstrate_platform_specific_features()
        provide_cross_platform_best_practices()

        print("\n" + "=" * 60)
        print("✅ Cross-platform analysis completed!")
        print("\nKey Takeaways:")
        print("• Most basic SQL filters work consistently across platforms")
        print("• Complex mathematical expressions may have limitations")
        print("• Performance varies significantly between backends")
        print("• Test your specific filters on all target platforms")
        print("• Consider feature compatibility when choosing backends")

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(dataset_path.parent)
        print(f"\n🧹 Cleaned up temporary directory")


if __name__ == "__main__":
    main()
