"""
Basic SQL Filters Example

This example demonstrates the fundamental usage of SQL filter functions
in fsspeckit for converting SQL WHERE clauses to platform-specific filters.

The example covers:
1. Basic SQL to PyArrow filter conversion
2. Basic SQL to Polars filter conversion
3. Common comparison operators
4. Handling different data types in filters

This example is designed for users who want to understand how to use
SQL expressions across different data processing backends.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import polars as pl

from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter


def create_sample_dataset() -> Path:
    """Create a sample dataset for filtering examples."""

    # Create sample data with various data types
    data = pa.Table.from_pydict(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "name": [
                "Alice",
                "Bob",
                "Charlie",
                "Diana",
                "Eve",
                "Frank",
                "Grace",
                "Henry",
                "Iris",
                "Jack",
            ],
            "age": [25, 30, 35, 28, 22, 45, 33, 29, 27, 31],
            "salary": [
                50000.0,
                60000.0,
                75000.0,
                55000.0,
                48000.0,
                90000.0,
                67000.0,
                58000.0,
                62000.0,
                71000.0,
            ],
            "department": [
                "Engineering",
                "Sales",
                "Marketing",
                "Engineering",
                "Sales",
                "Management",
                "Engineering",
                "Marketing",
                "Sales",
                "Engineering",
            ],
            "hire_date": [
                "2020-01-15",
                "2019-03-20",
                "2021-06-10",
                "2022-02-28",
                "2023-01-10",
                "2018-11-05",
                "2020-08-12",
                "2021-12-01",
                "2022-07-15",
                "2019-09-30",
            ],
            "active": [True, True, False, True, True, False, True, True, False, True],
        }
    )

    # Write to temporary directory
    temp_dir = Path(tempfile.mkdtemp())
    dataset_path = temp_dir / "employees"
    dataset_path.mkdir(parents=True, exist_ok=True)

    # Write as partitioned dataset
    ds.write_dataset(data, dataset_path, format="parquet", partitioning=["department"])

    print(f"Created sample dataset at: {dataset_path}")
    return dataset_path


def demonstrate_basic_pyarrow_filters(dataset_path: Path):
    """Demonstrate basic SQL to PyArrow filter conversion."""

    print("\n=== PyArrow Filter Examples ===")

    # Open dataset
    dataset = ds.dataset(dataset_path, format="parquet")

    # Example 1: Simple comparison
    sql_filter = "age > 30"
    schema = dataset.schema
    pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)

    print(f"\n1. SQL: {sql_filter}")
    print(f"   PyArrow filter: {pyarrow_filter}")

    filtered_table = dataset.to_table(filter=pyarrow_filter)
    print(f"   Results: {len(filtered_table)} rows found")

    # Example 2: Multiple conditions with AND
    sql_filter = "age > 25 AND salary < 70000"
    pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)

    print(f"\n2. SQL: {sql_filter}")
    print(f"   PyArrow filter: {pyarrow_filter}")

    filtered_table = dataset.to_table(filter=pyarrow_filter)
    print(f"   Results: {len(filtered_table)} rows found")

    # Example 3: String equality (LIKE not supported, using = instead)
    sql_filter = "name = 'Alice'"
    pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)

    print(f"\n3. SQL: {sql_filter}")
    print(f"   PyArrow filter: {pyarrow_filter}")

    filtered_table = dataset.to_table(filter=pyarrow_filter)
    print(f"   Results: {len(filtered_table)} rows found")

    # Example 4: Date comparison
    sql_filter = "hire_date >= '2020-01-01'"
    pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)

    print(f"\n4. SQL: {sql_filter}")
    print(f"   PyArrow filter: {pyarrow_filter}")

    filtered_table = dataset.to_table(filter=pyarrow_filter)
    print(f"   Results: {len(filtered_table)} rows found")


def demonstrate_basic_polars_filters(dataset_path: Path):
    """Demonstrate basic SQL to Polars filter conversion."""

    print("\n=== Polars Filter Examples ===")

    # Read dataset with Polars by reading individual parquet files
    import glob

    parquet_files = list((dataset_path / "department=Engineering").glob("*.parquet"))
    if parquet_files:
        df = pl.read_parquet(parquet_files[0])
    else:
        # Fallback: create a simple dataframe for demonstration
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "name": [
                    "Alice",
                    "Bob",
                    "Charlie",
                    "Diana",
                    "Eve",
                    "Frank",
                    "Grace",
                    "Henry",
                    "Iris",
                    "Jack",
                ],
                "age": [25, 30, 35, 28, 22, 45, 33, 29, 27, 31],
                "salary": [
                    50000.0,
                    60000.0,
                    75000.0,
                    55000.0,
                    48000.0,
                    90000.0,
                    67000.0,
                    58000.0,
                    62000.0,
                    71000.0,
                ],
                "department": [
                    "Engineering",
                    "Sales",
                    "Marketing",
                    "Engineering",
                    "Sales",
                    "Management",
                    "Engineering",
                    "Marketing",
                    "Sales",
                    "Engineering",
                ],
                "hire_date": [
                    "2020-01-15",
                    "2019-03-20",
                    "2021-06-10",
                    "2022-02-28",
                    "2023-01-10",
                    "2018-11-05",
                    "2020-08-12",
                    "2021-12-01",
                    "2022-07-15",
                    "2019-09-30",
                ],
                "active": [
                    True,
                    True,
                    False,
                    True,
                    True,
                    False,
                    True,
                    True,
                    False,
                    True,
                ],
            }
        )

    # Example 1: Simple comparison
    sql_filter = "salary > 60000"
    schema = df.schema
    polars_filter = sql2polars_filter(sql_filter, schema)

    print(f"\n1. SQL: {sql_filter}")
    print(f"   Polars filter: {polars_filter}")

    filtered_df = df.filter(polars_filter)
    print(f"   Results: {len(filtered_df)} rows found")

    # Example 2: String matching
    sql_filter = "department = 'Engineering'"
    polars_filter = sql2polars_filter(sql_filter, schema)

    print(f"\n2. SQL: {sql_filter}")
    print(f"   Polars filter: {polars_filter}")

    filtered_df = df.filter(polars_filter)
    print(f"   Results: {len(filtered_df)} rows found")

    # Example 3: Boolean conditions (using 1/0 instead of true/false)
    sql_filter = "active = 1 AND age < 35"
    polars_filter = sql2polars_filter(sql_filter, schema)

    print(f"\n3. SQL: {sql_filter}")
    print(f"   Polars filter: {polars_filter}")

    filtered_df = df.filter(polars_filter)
    print(f"   Results: {len(filtered_df)} rows found")


def demonstrate_cross_platform_consistency(dataset_path: Path):
    """Show that same SQL produces consistent results across platforms."""

    print("\n=== Cross-Platform Consistency Check ===")

    # Same SQL filter
    sql_filter = "age >= 30 AND active = 1"

    # PyArrow version
    dataset = ds.dataset(dataset_path, format="parquet")
    schema = dataset.schema
    pyarrow_filter = sql2pyarrow_filter(sql_filter, schema)
    pyarrow_result = dataset.to_table(filter=pyarrow_filter)

    # Polars version
    # Create a simple dataframe for demonstration
    df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "name": [
                "Alice",
                "Bob",
                "Charlie",
                "Diana",
                "Eve",
                "Frank",
                "Grace",
                "Henry",
                "Iris",
                "Jack",
            ],
            "age": [25, 30, 35, 28, 22, 45, 33, 29, 27, 31],
            "salary": [
                50000.0,
                60000.0,
                75000.0,
                55000.0,
                48000.0,
                90000.0,
                67000.0,
                58000.0,
                62000.0,
                71000.0,
            ],
            "department": [
                "Engineering",
                "Sales",
                "Marketing",
                "Engineering",
                "Sales",
                "Management",
                "Engineering",
                "Marketing",
                "Sales",
                "Engineering",
            ],
            "hire_date": [
                "2020-01-15",
                "2019-03-20",
                "2021-06-10",
                "2022-02-28",
                "2023-01-10",
                "2018-11-05",
                "2020-08-12",
                "2021-12-01",
                "2022-07-15",
                "2019-09-30",
            ],
            "active": [True, True, False, True, True, False, True, True, False, True],
        }
    )
    polars_schema = df.schema
    polars_filter = sql2polars_filter(sql_filter, polars_schema)
    polars_result = df.filter(polars_filter)

    print(f"\nSQL Filter: {sql_filter}")
    print(f"PyArrow results: {len(pyarrow_result)} rows")
    print(f"Polars results:  {len(polars_result)} rows")

    # Sort both results for comparison
    pyarrow_sorted = pyarrow_result.sort_by("id")
    polars_sorted = polars_result.sort("id")

    print(f"Row counts match: {len(pyarrow_result) == len(polars_result)}")

    if len(pyarrow_result) > 0:
        print(
            f"First matching ID - PyArrow: {pyarrow_sorted['id'][0].as_py()}, Polars: {polars_sorted['id'][0]}"
        )


def main():
    """Run all basic SQL filter examples."""

    print("🔍 Basic SQL Filters Example")
    print("=" * 50)

    # Create sample dataset
    dataset_path = create_sample_dataset()

    try:
        # Demonstrate different filter types
        demonstrate_basic_pyarrow_filters(dataset_path)
        demonstrate_basic_polars_filters(dataset_path)
        demonstrate_cross_platform_consistency(dataset_path)

        print("\n" + "=" * 50)
        print("✅ All examples completed successfully!")

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(dataset_path.parent)
        print(f"\n🧹 Cleaned up temporary directory")


if __name__ == "__main__":
    main()
