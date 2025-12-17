"""
Advanced SQL Filters Example

This example demonstrates advanced SQL filter capabilities including:
1. Complex WHERE clauses with multiple conditions
2. SQL aggregation functions in filters
3. Date and time operations
4. String pattern matching and functions
5. Subqueries and nested conditions
6. Performance considerations

This example is designed for users who want to understand advanced
SQL filtering capabilities and how to optimize them.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import polars as pl

from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter


def create_complex_dataset() -> Path:
    """Create a comprehensive dataset for advanced filtering examples."""

    import random

    print("Creating complex sample dataset...")

    # Generate more comprehensive data
    records = []
    departments = ["Engineering", "Sales", "Marketing", "Finance", "HR", "Operations"]
    offices = ["New York", "San Francisco", "London", "Tokyo", "Singapore", "Remote"]

    for emp_id in range(100):
        base_date = datetime(2020, 1, 1)
        hire_date = base_date + timedelta(days=random.randint(0, 1500))

        record = {
            "employee_id": emp_id + 1,
            "first_name": f"Employee_{emp_id + 1}",
            "last_name": f"LastName_{emp_id + 1}",
            "email": f"employee{emp_id + 1}@company.com",
            "age": random.randint(22, 65),
            "salary": random.randint(40000, 150000),
            "bonus": random.randint(0, 20000),
            "department": random.choice(departments),
            "office_location": random.choice(offices),
            "hire_date": hire_date.strftime("%Y-%m-%d"),
            "last_promotion_date": (
                hire_date + timedelta(days=random.randint(365, 1000))
            ).strftime("%Y-%m-%d"),
            "performance_score": round(random.uniform(2.5, 5.0), 1),
            "remote_worker": random.choice([True, False]),
            "full_time": random.choice([True, False])
            if emp_id % 10 == 0
            else True,  # Some contractors
        }
        records.append(record)

    data = pa.table(records)

    # Write to temporary directory with partitioning
    temp_dir = Path(tempfile.mkdtemp())
    dataset_path = temp_dir / "employees_complex"
    dataset_path.mkdir(parents=True, exist_ok=True)

    # Write as partitioned dataset
    ds.write_dataset(
        data,
        dataset_path,
        format="parquet",
        partitioning=["department", "office_location"],
    )

    print(f"Created complex dataset with {len(records)} records at: {dataset_path}")
    return dataset_path


def demonstrate_complex_pyarrow_filters(dataset_path: Path):
    """Demonstrate complex SQL filters with PyArrow."""

    print("\n=== Advanced PyArrow Filter Examples ===")

    dataset = ds.dataset(dataset_path, format="parquet")
    schema = dataset.schema

    # Example 1: Complex boolean logic
    sql_filter = """
        (age >= 30 AND age <= 50) AND
        (salary >= 60000 OR bonus > 10000) AND
        department IN ('Engineering', 'Sales')
    """
    pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)

    print(f"\n1. Complex Boolean Logic:")
    print(f"   SQL: {sql_filter.strip()}")
    print(f"   PyArrow filter: {pyarrow_filter}")

    filtered_table = dataset.to_table(filter=pyarrow_filter)
    print(f"   Results: {len(filtered_table)} rows found")

    # Example 2: Date range and string pattern
    sql_filter = """
        hire_date >= '2021-01-01' AND
        hire_date <= '2023-12-31' AND
        (email LIKE '%@company.com' AND first_name NOT LIKE 'Employee_%')
    """
    try:
        pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)

        print(f"\n2. Date Range with String Patterns:")
        print(f"   SQL: {sql_filter.strip()}")
        print(f"   PyArrow filter: {pyarrow_filter}")

        filtered_table = dataset.to_table(filter=pyarrow_filter)
        print(f"   Results: {len(filtered_table)} rows found")
    except Exception as e:
        print(f"   Note: This filter may not be fully supported: {e}")

    # Example 3: Nested conditions with NULL checks
    sql_filter = """
        (performance_score > 4.0 OR remote_worker = true) AND
        salary IS NOT NULL AND
        (full_time = true OR department = 'Engineering')
    """
    pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)

    print(f"\n3. Nested Conditions with NULL Checks:")
    print(f"   SQL: {sql_filter.strip()}")
    print(f"   PyArrow filter: {pyarrow_filter}")

    filtered_table = dataset.to_table(filter=pyarrow_filter)
    print(f"   Results: {len(filtered_table)} rows found")

    # Example 4: Performance score with mathematical operations
    sql_filter = """
        (salary + bonus) > 100000 AND
        performance_score >= 3.5 AND
        age BETWEEN 25 AND 45
    """
    pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)

    print(f"\n4. Mathematical Operations in Filters:")
    print(f"   SQL: {sql_filter.strip()}")
    print(f"   PyArrow filter: {pyarrow_filter}")

    filtered_table = dataset.to_table(filter=pyarrow_filter)
    print(f"   Results: {len(filtered_table)} rows found")


def demonstrate_advanced_polars_filters(dataset_path: Path):
    """Demonstrate advanced SQL filters with Polars."""

    print("\n=== Advanced Polars Filter Examples ===")

    df = pl.read_parquet(dataset_path, glob="**/*.parquet")
    schema = df.schema

    # Example 1: Complex filtering with Polars expressions
    sql_filter = """
        department IN ('Engineering', 'Sales', 'Finance') AND
        (salary > 80000 OR (bonus > 15000 AND performance_score > 4.0))
    """
    polars_filter = sql2polars_filter(sql_filter, schema)

    print(f"\n1. Complex Department and Compensation Filter:")
    print(f"   SQL: {sql_filter.strip()}")
    print(f"   Polars filter: {polars_filter}")

    filtered_df = df.filter(polars_filter)
    print(f"   Results: {len(filtered_df)} rows found")

    # Example 2: Date-based filtering with string operations
    sql_filter = """
        office_location IN ('New York', 'San Francisco') AND
        remote_worker = false AND
        email LIKE '%@company.com'
    """
    polars_filter = sql2polars_filter(sql_filter, schema)

    print(f"\n2. Office Location and Work Arrangement:")
    print(f"   SQL: {sql_filter.strip()}")
    print(f"   Polars filter: {polars_filter}")

    filtered_df = df.filter(polars_filter)
    print(f"   Results: {len(filtered_df)} rows found")

    # Example 3: Performance-based filtering
    sql_filter = """
        performance_score >= 4.0 AND
        full_time = true AND
        age BETWEEN 28 AND 50
    """
    polars_filter = sql2polars_filter(sql_filter, schema)

    print(f"\n3. High Performers Filter:")
    print(f"   SQL: {sql_filter.strip()}")
    print(f"   Polars filter: {polars_filter}")

    filtered_df = df.filter(polars_filter)
    print(f"   Results: {len(filtered_df)} rows found")


def demonstrate_performance_comparison(dataset_path: Path):
    """Compare performance of different filter strategies."""

    print("\n=== Performance Comparison ===")

    dataset = ds.dataset(dataset_path, format="parquet")
    df = pl.read_parquet(dataset_path, glob="**/*.parquet")
    schema = dataset.schema
    polars_schema = df.schema

    # Test filters of varying complexity
    test_filters = [
        "age > 30",
        "age > 30 AND salary > 70000",
        "age > 30 AND salary > 70000 AND department IN ('Engineering', 'Sales')",
        "age > 30 AND salary > 70000 AND department IN ('Engineering', 'Sales') AND performance_score > 4.0",
    ]

    import time

    for i, sql_filter in enumerate(test_filters, 1):
        print(f"\n{i}. Filter: {sql_filter}")

        # PyArrow timing
        pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)
        start_time = time.time()
        pyarrow_result = dataset.to_table(filter=pyarrow_filter)
        pyarrow_time = time.time() - start_time

        # Polars timing
        polars_filter = sql2polars_filter(sql_filter, polars_schema)
        start_time = time.time()
        polars_result = df.filter(polars_filter)
        polars_time = time.time() - start_time

        print(f"   PyArrow: {len(pyarrow_result)} rows in {pyarrow_time:.4f}s")
        print(f"   Polars:  {len(polars_result)} rows in {polars_time:.4f}s")
        print(f"   Results match: {len(pyarrow_result) == len(polars_result)}")


def demonstrate_filter_optimization_tips():
    """Provide tips for optimizing SQL filters."""

    print("\n=== Filter Optimization Tips ===")

    tips = [
        "1. Use indexed columns first in multi-column filters",
        "2. Prefer range filters (BETWEEN) over multiple inequalities",
        "3. Use IN clauses instead of multiple OR conditions when possible",
        "4. Place highly selective conditions early in complex filters",
        "5. Consider partition pruning with partition columns",
        "6. Use boolean columns for early filtering when possible",
        "7. Test filter performance with realistic data volumes",
        "8. Profile both PyArrow and Polars backends for your specific use case",
    ]

    for tip in tips:
        print(f"   {tip}")

    print("\n=== Supported SQL Features ===")
    features = [
        "✅ Basic comparisons: =, !=, >, <, >=, <=",
        "✅ Logical operators: AND, OR, NOT",
        "✅ IN clauses with lists",
        "✅ BETWEEN for range filters",
        "✅ LIKE for string pattern matching",
        "✅ NULL checks: IS NULL, IS NOT NULL",
        "✅ Boolean values: true, false",
        "✅ Date and time comparisons",
        "✅ Mathematical expressions: +, -, *, /",
        "⚠️  Subqueries (limited support)",
        "❌ Aggregate functions in WHERE clauses",
        "❌ Window functions",
    ]

    for feature in features:
        print(f"   {feature}")


def main():
    """Run all advanced SQL filter examples."""

    print("🚀 Advanced SQL Filters Example")
    print("=" * 60)

    # Create comprehensive dataset
    dataset_path = create_complex_dataset()

    try:
        # Demonstrate advanced filtering capabilities
        demonstrate_complex_pyarrow_filters(dataset_path)
        demonstrate_advanced_polars_filters(dataset_path)
        demonstrate_performance_comparison(dataset_path)
        demonstrate_filter_optimization_tips()

        print("\n" + "=" * 60)
        print("✅ All advanced examples completed successfully!")

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(dataset_path.parent)
        print(f"\n🧹 Cleaned up temporary directory")


if __name__ == "__main__":
    main()
