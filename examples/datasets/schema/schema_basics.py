"""
Schema Management Basics Example

This example demonstrates fundamental schema operations using fsspeckit's
dataset utilities for handling data schemas across different backends.

The example covers:
1. Basic schema validation and inspection
2. Schema casting and type conversion
3. Handling missing or inconsistent schemas
4. Working with different data types
5. Schema evolution basics

This example is designed for users who need to understand and manage
data schemas in their datasets.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from fsspeckit.datasets import cast_schema, opt_dtype_pa


def create_sample_datasets_with_different_schemas() -> tuple[Path, Path]:
    """Create two sample datasets with different schemas for testing."""

    print("Creating sample datasets with different schemas...")

    temp_dir = Path(tempfile.mkdtemp())

    # Dataset 1: Employee data (Version 1)
    employees_v1 = pa.table(
        {
            "employee_id": pa.array([1, 2, 3, 4, 5]),
            "name": pa.array(["Alice", "Bob", "Charlie", "Diana", "Eve"]),
            "age": pa.array([28, 32, 45, 29, 35]),
            "department": pa.array(
                ["Engineering", "Sales", "Marketing", "Engineering", "HR"]
            ),
            "salary": pa.array([75000.0, 65000.0, 80000.0, 72000.0, 68000.0]),
        }
    )

    dataset1_path = temp_dir / "employees_v1"
    dataset1_path.mkdir(parents=True, exist_ok=True)

    # Write first dataset
    pq.write_table(employees_v1, dataset1_path / "data.parquet")

    # Dataset 2: Employee data (Version 2 - with changes)
    employees_v2 = pa.table(
        {
            "employee_id": pa.array([6, 7, 8, 9, 10]),
            "full_name": pa.array(["Frank", "Grace", "Henry", "Iris", "Jack"]),
            "age": pa.array([41, 26, 38, 31, 44]),
            "department": pa.array(
                ["Finance", "Sales", "Engineering", "Marketing", "Operations"]
            ),
            "salary": pa.array([85000.0, 60000.0, 78000.0, 70000.0, 82000.0]),
            "hire_date": pa.array(
                ["2020-01-15", "2021-03-20", "2019-06-10", "2022-02-28", "2018-11-05"]
            ),
            "is_active": pa.array([True, True, False, True, True]),
        }
    )

    dataset2_path = temp_dir / "employees_v2"
    dataset2_path.mkdir(parents=True, exist_ok=True)

    # Write second dataset
    pq.write_table(employees_v2, dataset2_path / "data.parquet")

    print(f"Created datasets at: {temp_dir}")
    return dataset1_path, dataset2_path


def inspect_schema(dataset_path: Path, name: str):
    """Inspect and display schema information."""

    print(f"\n🔍 Schema Inspection: {name}")

    # Read the dataset
    table = pq.read_table(dataset_path / "data.parquet")

    print(f"Schema:")
    print(f"  Rows: {len(table)}")
    print(f"  Columns: {len(table.schema)}")

    for i, field in enumerate(table.schema):
        print(f"  {i + 1}. {field.name}: {field.type}")
        if pa.types.is_string(field.type):
            sample_values = table.column(field.name).to_pylist()[:3]
            print(f"     Sample values: {sample_values}")
        elif pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
            stats = table.column(field.name)
            if len(stats) > 0:
                import pyarrow.compute as pc

                min_max = pc.min_max(stats).as_py()
                print(f"     Min: {min_max['min']}, Max: {min_max['max']}")


def demonstrate_basic_type_conversion():
    """Demonstrate basic data type conversion and optimization."""

    print(f"\n🔧 Basic Type Conversion")

    # Create sample data with suboptimal types
    data = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),  # String instead of int
            "price": pa.array(
                [10.0, 20.5, 30.0, 40.5, 50.0]
            ),  # Float64, could be Float32
            "category": pa.array(["A", "B", "A", "C", "B"]),  # Could be dictionary
            "quantity": pa.array([1, 2, 3, 4, 5]),  # Int64, could be Int32
        }
    )

    print("Original schema:")
    for field in data.schema:
        print(f"  {field.name}: {field.type}")

    # Optimize data types
    optimized_data = data.cast(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("price", pa.float32()),
                pa.field("category", pa.dictionary(pa.int32(), pa.string())),
                pa.field("quantity", pa.int32()),
            ]
        )
    )

    print("\nOptimized schema:")
    for field in optimized_data.schema:
        print(f"  {field.name}: {field.type}")

    # Demonstrate memory efficiency
    original_buffer = data.schema.serialize()
    optimized_buffer = optimized_data.schema.serialize()

    print(
        f"\nSchema size reduction: {len(original_buffer)} -> {len(optimized_buffer)} bytes"
    )


def demonstrate_schema_casting():
    """Demonstrate schema casting operations."""

    print(f"\n🔄 Schema Casting Operations")

    # Create data that needs casting
    data = pa.table(
        {
            "timestamp": pa.array(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "value": pa.array(["10.5", "20.3", "30.7"]),  # String numbers
            "active": pa.array(["true", "false", "true"]),  # String booleans
        }
    )

    print("Original data:")
    print(data)
    print(f"Schema: {data.schema}")

    # Define target schema
    target_schema = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("s")),
            pa.field("value", pa.float32()),
            pa.field("active", pa.bool_()),
        ]
    )

    print(f"\nTarget schema: {target_schema}")

    # Apply schema casting
    try:
        casted_data = cast_schema(data, target_schema)
        print(f"\n✅ Successfully casted data:")
        print(casted_data)
        print(f"Casted schema: {casted_data.schema}")
    except Exception as e:
        print(f"\n❌ Casting failed: {e}")


def demonstrate_type_optimization():
    """Demonstrate automatic type optimization."""

    print(f"\n⚡ Automatic Type Optimization")

    # Create data with various types that can be optimized
    data = pa.table(
        {
            "small_numbers": pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], type=pa.int64()),
            "large_text": pa.array(
                [
                    "short",
                    "medium length text",
                    "very long text that could be compressed",
                    "short",
                    "medium length text",
                    "very long text that could be compressed",
                    "short",
                    "medium length text",
                    "very long text that could be compressed",
                    "short",
                ]
            ),
            "repeated_values": pa.array(
                ["Category A", "Category B", "Category A", "Category C", "Category B"]
                * 2
            ),
            "precise_floats": pa.array(
                [10.5, 20.3, 30.7, 40.1, 50.9, 10.5, 20.3, 30.7, 40.1, 50.9]
            ),
        }
    )

    print("Original schema:")
    for field in data.schema:
        print(f"  {field.name}: {field.type}")

    # Optimize specific columns
    optimized_schema = data.schema

    for i, field in enumerate(optimized_schema):
        if field.name == "small_numbers":
            # Optimize int64 to int8 since values are small
            optimized_field = pa.field(field.name, pa.int8())
            optimized_schema = optimized_schema.set(i, optimized_field)
        elif field.name == "repeated_values":
            # Convert to dictionary type for repeated values
            optimized_field = pa.field(
                field.name, pa.dictionary(pa.int8(), pa.string())
            )
            optimized_schema = optimized_schema.set(i, optimized_field)
        elif field.name == "precise_floats":
            # Convert to float32 if precision allows
            optimized_field = pa.field(field.name, pa.float32())
            optimized_schema = optimized_schema.set(i, optimized_field)

    optimized_data = data.cast(optimized_schema)

    print(f"\nOptimized schema:")
    for field in optimized_data.schema:
        print(f"  {field.name}: {field.type}")


def demonstrate_schema_validation():
    """Demonstrate schema validation and consistency checks."""

    print(f"\n✅ Schema Validation")

    # Define expected schema
    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("age", pa.int32()),
            pa.field("department", pa.string()),
            pa.field("salary", pa.float64()),
        ]
    )

    # Create data that matches
    valid_data = pa.table(
        {
            "id": pa.array([1, 2, 3]),
            "name": pa.array(["Alice", "Bob", "Charlie"]),
            "age": pa.array([25, 30, 35]),
            "department": pa.array(["Engineering", "Sales", "Marketing"]),
            "salary": pa.array([75000.0, 65000.0, 80000.0]),
        }
    )

    # Create data that doesn't match
    invalid_data = pa.table(
        {
            "id": pa.array([4, 5, 6]),
            "name": pa.array(["Diana", "Eve", "Frank"]),
            "age": pa.array([28, 32, 45]),  # Correct
            "department": pa.array(["HR", "Finance", "IT"]),
            "salary": pa.array(["70000", "68000", "90000"]),  # String instead of float
        }
    )

    print("Expected schema:")
    for field in expected_schema:
        print(f"  {field.name}: {field.type}")

    # Validate valid data
    print(f"\nValid data schema validation:")
    if valid_data.schema.equals(expected_schema):
        print("✅ Schema matches expected structure")
    else:
        print("❌ Schema does not match")

    # Validate invalid data
    print(f"\nInvalid data schema validation:")
    if invalid_data.schema.equals(expected_schema):
        print("✅ Schema matches expected structure")
    else:
        print("❌ Schema does not match")
        print("Expected types vs actual types:")
        for expected_field in expected_schema:
            actual_field = invalid_data.schema.field(expected_field.name)
            if expected_field.type != actual_field.type:
                print(
                    f"  {expected_field.name}: expected {expected_field.type}, got {actual_field.type}"
                )


def demonstrate_null_handling():
    """Demonstrate null handling in schema operations."""

    print(f"\n🚫 Null Handling in Schema Operations")

    # Create data with nulls
    data_with_nulls = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5]),
            "name": pa.array(["Alice", None, "Charlie", "Diana", "Eve"]),
            "age": pa.array([25, None, 35, 29, None]),
            "score": pa.array([85.5, None, 92.3, 78.1, None]),
        }
    )

    print("Data with nulls:")
    print(data_with_nulls)

    # Show null counts
    print(f"\nNull counts:")
    for column_name in data_with_nulls.column_names:
        column = data_with_nulls.column(column_name)
        null_count = column.null_count
        print(
            f"  {column_name}: {null_count} nulls ({null_count / len(column) * 100:.1f}%)"
        )

    # Demonstrate schema with nullable fields
    schema_with_nulls = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("age", pa.int32(), nullable=True),
            pa.field("score", pa.float32(), nullable=True),
        ]
    )

    print(f"\nSchema with nullability:")
    for field in schema_with_nulls:
        nullable_str = "nullable" if field.nullable else "non-nullable"
        print(f"  {field.name}: {field.type} ({nullable_str})")


def main():
    """Run all schema management examples."""

    print("📋 Schema Management Basics Example")
    print("=" * 50)

    # Create sample datasets
    dataset1_path, dataset2_path = create_sample_datasets_with_different_schemas()

    try:
        # Inspect different schemas
        inspect_schema(dataset1_path, "Employees V1")
        inspect_schema(dataset2_path, "Employees V2")

        # Demonstrate various schema operations
        demonstrate_basic_type_conversion()
        demonstrate_schema_casting()
        demonstrate_type_optimization()
        demonstrate_schema_validation()
        demonstrate_null_handling()

        print("\n" + "=" * 50)
        print("✅ All schema basics examples completed!")

    finally:
        # Cleanup
        import shutil

        temp_dir = dataset1_path.parent
        shutil.rmtree(temp_dir)
        print(f"\n🧹 Cleaned up temporary directory")


if __name__ == "__main__":
    main()
