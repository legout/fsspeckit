"""
Schema Unification Example

This example demonstrates how to handle and unify schemas across multiple
datasets that may have different structures, column names, or data types.

The example covers:
1. Schema detection and comparison
2. Automatic schema unification
3. Handling missing columns
4. Data type reconciliation
5. Schema evolution scenarios
6. Working with partitioned datasets

This example is designed for users who need to combine data from
multiple sources with potentially inconsistent schemas.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from fsspeckit.datasets import unify_schemas_pa, cast_schema


def create_multiple_datasets_with_varying_schemas() -> list[Path]:
    """Create multiple datasets with different schemas for unification testing."""

    print("Creating datasets with varying schemas...")

    temp_dir = Path(tempfile.mkdtemp())
    dataset_paths = []

    # Dataset 1: Sales data (Version 1)
    sales_v1 = pa.table(
        {
            "sale_id": pa.array([101, 102, 103, 104, 105]),
            "customer_name": pa.array(
                ["Alice Corp", "Bob Inc", "Charlie Ltd", "Diana Co", "Eve LLC"]
            ),
            "product": pa.array(
                ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]
            ),
            "amount": pa.array([1200.50, 25.99, 89.99, 450.00, 125.50]),
            "date": pa.array(
                ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19"]
            ),
        }
    )

    dataset1_path = temp_dir / "sales_v1"
    dataset1_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(sales_v1, dataset1_path / "data.parquet")
    dataset_paths.append(dataset1_path)

    # Dataset 2: Sales data (Version 2 - different column names)
    sales_v2 = pa.table(
        {
            "transaction_id": pa.array([201, 202, 203, 204]),
            "client": pa.array(
                [
                    "Frank Industries",
                    "Grace Enterprises",
                    "Henry Solutions",
                    "Iris Tech",
                ]
            ),
            "item": pa.array(["Webcam", "USB Hub", "External SSD", "Docking Station"]),
            "price": pa.array([75.99, 35.50, 189.99, 149.99]),
            "sale_date": pa.array(
                ["2024-02-01", "2024-02-02", "2024-02-03", "2024-02-04"]
            ),
            "category": pa.array(
                ["Electronics", "Accessories", "Storage", "Accessories"]
            ),
        }
    )

    dataset2_path = temp_dir / "sales_v2"
    dataset2_path.mkdir(parents=True)
    pq.write_table(sales_v2, dataset2_path / "data.parquet")
    dataset_paths.append(dataset2_path)

    # Dataset 3: Sales data (Version 3 - additional columns and type changes)
    sales_v3 = pa.table(
        {
            "order_id": pa.array([301, 302]),
            "customer": pa.array(["Jack Systems", "Kate Manufacturing"]),
            "product_name": pa.array(["Cable Kit", "Power Bank"]),
            "total_amount": pa.array([29.99, 45.50]),
            "order_date": pa.array(["2024-03-01", "2024-03-02"]),
            "quantity": pa.array([2, 1]),
            "discount": pa.array([5.0, 0.0]),
            "status": pa.array(["completed", "shipped"]),
        }
    )

    dataset3_path = temp_dir / "sales_v3"
    dataset3_path.mkdir(parents=True)
    pq.write_table(sales_v3, dataset3_path / "data.parquet")
    dataset_paths.append(dataset3_path)

    # Dataset 4: Sales data (Version 4 - some columns renamed, some missing)
    sales_v4 = pa.table(
        {
            "sale_id": pa.array([401, 402]),
            "customer_name": pa.array(["Leo Corp", "Maya Inc"]),
            "product": pa.array(["Router", "Switch"]),
            "amount": pa.array([299.99, 449.99]),
            "date": pa.array(["2024-03-15", "2024-03-16"]),
            "region": pa.array(["West", "East"]),
        }
    )

    dataset4_path = temp_dir / "sales_v4"
    dataset4_path.mkdir(parents=True)
    pq.write_table(sales_v4, dataset4_path / "data.parquet")
    dataset_paths.append(dataset4_path)

    print(f"Created {len(dataset_paths)} datasets at: {temp_dir}")
    return dataset_paths


def analyze_schema_differences(dataset_paths: list[Path]):
    """Analyze and display differences between dataset schemas."""

    print("\n🔍 Schema Analysis")

    schemas = []
    dataset_names = []

    for i, path in enumerate(dataset_paths, 1):
        table = pq.read_table(path / "data.parquet")
        schemas.append(table.schema)
        dataset_names.append(f"Dataset {i}")

    # Display each schema
    for name, schema in zip(dataset_names, schemas):
        print(f"\n{name}:")
        for field in schema:
            print(f"  {field.name}: {field.type}")

    # Find all unique columns
    all_columns = set()
    for schema in schemas:
        all_columns.update(field.name for field in schema)

    print(f"\n📊 Schema Summary:")
    print(f"Total unique columns: {len(all_columns)}")
    print(f"Columns across all datasets: {list(all_columns)}")

    # Show column presence matrix
    print(f"\n📋 Column Presence Matrix:")
    print("Column".ljust(20), end="")
    for name in dataset_names:
        print(f"{name}".ljust(12), end="")
    print()

    for column in sorted(all_columns):
        print(f"{column}".ljust(20), end="")
        for schema in schemas:
            has_column = any(field.name == column for field in schema)
            print(f"{'✓'.ljust(12) if has_column else '✗'.ljust(12)}", end="")
        print()


def demonstrate_schema_unification(dataset_paths: list[Path]):
    """Demonstrate automatic schema unification across datasets."""

    print("\n🔄 Schema Unification")

    # Load all datasets
    tables = []
    for path in dataset_paths:
        table = pq.read_table(path / "data.parquet")
        tables.append(table)
        print(f"Loaded {len(table)} rows from {path.name}")

    # Create a unified schema manually (since unify_schemas_pa may not be available)
    print("\nCreating unified schema...")

    # Collect all columns and their types
    all_columns = {}

    for table in tables:
        for field in table.schema:
            if field.name not in all_columns:
                all_columns[field.name] = field
            else:
                # If column exists, choose the more general type
                existing_field = all_columns[field.name]
                if str(field.type) != str(existing_field.type):
                    print(
                        f"  Type conflict for '{field.name}': {existing_field.type} vs {field.type}"
                    )
                    # For simplicity, use string as common type for conflicts
                    all_columns[field.name] = pa.field(field.name, pa.string())

    # Build unified schema
    unified_schema = pa.schema(list(all_columns.values()))

    print(f"\nUnified schema ({len(unified_schema)} columns):")
    for field in unified_schema:
        print(f"  {field.name}: {field.type}")

    # Create unified tables by adding missing columns with nulls
    unified_tables = []

    for i, table in enumerate(tables):
        # Create arrays for unified table
        unified_arrays = []

        for field in unified_schema:
            if field.name in table.column_names:
                # Cast to unified schema type
                original_array = table.column(field.name)
                try:
                    casted_array = original_array.cast(field.type)
                    unified_arrays.append(casted_array)
                except Exception:
                    # If casting fails, create null array
                    null_array = pa.array([None] * len(table), type=field.type)
                    unified_arrays.append(null_array)
            else:
                # Add null array for missing column
                null_array = pa.array([None] * len(table), type=field.type)
                unified_arrays.append(null_array)

        # Create unified table
        unified_table = pa.Table.from_arrays(unified_arrays, schema=unified_schema)
        unified_tables.append(unified_table)

        print(f"  Dataset {i + 1}: {len(table)} -> {len(unified_table)} rows")

    # Combine all unified tables
    combined_table = pa.concat_tables(unified_tables)

    print(f"\n✅ Unified dataset created:")
    print(f"  Total rows: {len(combined_table)}")
    print(f"  Total columns: {len(combined_table.schema)}")
    print(f"  Columns: {list(combined_table.schema.names)}")

    # Show sample of unified data
    print(f"\n📄 Sample of unified data (first 5 rows):")
    sample_data = combined_table.slice(0, min(5, len(combined_table)))
    for column_name in sample_data.column_names:
        column = sample_data.column(column_name)
        values = column.to_pylist()
        # Truncate long string values
        if isinstance(values[0], str) and len(str(values[0])) > 15:
            values = [str(v)[:15] + "..." if v else v for v in values]
        print(f"  {column_name}: {values}")


def demonstrate_column_mapping():
    """Demonstrate manual column mapping for complex scenarios."""

    print("\n🗺️  Manual Column Mapping")

    # Define mapping between different column naming conventions
    column_mappings = {
        "identifier": ["sale_id", "transaction_id", "order_id"],
        "customer": ["customer_name", "client", "customer"],
        "product": ["product", "item", "product_name"],
        "amount": ["amount", "price", "total_amount"],
        "date": ["date", "sale_date", "order_date"],
    }

    print("Column mappings:")
    for standard_name, variants in column_mappings.items():
        print(f"  {standard_name}: {variants}")

    # Create sample data with different column names
    data1 = pa.table(
        {
            "sale_id": pa.array([1, 2, 3]),
            "customer_name": pa.array(["Alice", "Bob", "Charlie"]),
            "product": pa.array(["A", "B", "C"]),
            "amount": pa.array([100.0, 200.0, 300.0]),
        }
    )

    data2 = pa.table(
        {
            "transaction_id": pa.array([4, 5]),
            "client": pa.array(["Diana", "Eve"]),
            "item": pa.array(["D", "E"]),
            "price": pa.array([150.0, 250.0]),
        }
    )

    print(f"\n📊 Applying column mapping:")

    # Create standardized schema
    standard_schema = pa.schema(
        [
            pa.field("identifier", pa.int64()),
            pa.field("customer", pa.string()),
            pa.field("product", pa.string()),
            pa.field("amount", pa.float64()),
        ]
    )

    # Apply mapping to first dataset
    print(f"\nDataset 1 mapping:")
    mapped_arrays1 = []
    for field in standard_schema:
        variants = column_mappings[field.name]
        original_column = None

        for variant in variants:
            if variant in data1.column_names:
                original_column = data1.column(variant)
                break

        if original_column is not None:
            casted_array = original_column.cast(field.type)
            mapped_arrays1.append(casted_array)
            print(f"  {field.name}: {variant} -> {field.type}")
        else:
            null_array = pa.array([None] * len(data1), type=field.type)
            mapped_arrays1.append(null_array)
            print(f"  {field.name}: [MISSING] -> null")

    mapped_table1 = pa.Table.from_arrays(mapped_arrays1, schema=standard_schema)

    # Apply mapping to second dataset
    print(f"\nDataset 2 mapping:")
    mapped_arrays2 = []
    for field in standard_schema:
        variants = column_mappings[field.name]
        original_column = None

        for variant in variants:
            if variant in data2.column_names:
                original_column = data2.column(variant)
                break

        if original_column is not None:
            casted_array = original_column.cast(field.type)
            mapped_arrays2.append(casted_array)
            print(f"  {field.name}: {variant} -> {field.type}")
        else:
            null_array = pa.array([None] * len(data2), type=field.type)
            mapped_arrays2.append(null_array)
            print(f"  {field.name}: [MISSING] -> null")

    mapped_table2 = pa.Table.from_arrays(mapped_arrays2, schema=standard_schema)

    # Combine mapped tables
    combined_mapped = pa.concat_tables([mapped_table1, mapped_table2])

    print(f"\n✅ Combined mapped data:")
    print(combined_mapped)


def demonstrate_type_reconciliation():
    """Demonstrate data type reconciliation across schemas."""

    print("\n🔧 Data Type Reconciliation")

    # Create datasets with same column names but different types
    data1 = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "score": pa.array([85.5, 90.0, 78.5], type=pa.float64()),
            "active": pa.array([True, False, True], type=pa.bool_()),
        }
    )

    data2 = pa.table(
        {
            "id": pa.array([4, 5], type=pa.int64()),  # Different int type
            "score": pa.array(
                ["92.3", "88.1"], type=pa.string()
            ),  # String instead of float
            "active": pa.array([1, 0], type=pa.int8()),  # Int instead of bool
        }
    )

    print("Dataset 1 types:")
    for field in data1.schema:
        print(f"  {field.name}: {field.type}")

    print("\nDataset 2 types:")
    for field in data2.schema:
        print(f"  {field.name}: {field.type}")

    # Define reconciliation strategy
    reconciled_schema = pa.schema(
        [
            pa.field("id", pa.int64()),  # Use largest int type
            pa.field("score", pa.float64()),  # Convert string to float
            pa.field("active", pa.bool_()),  # Convert int to bool
        ]
    )

    print(f"\nReconciled schema:")
    for field in reconciled_schema:
        print(f"  {field.name}: {field.type}")

    # Apply reconciliation
    reconciled_data1 = cast_schema(data1, reconciled_schema)

    print(f"\nDataset 1 after reconciliation:")
    print(reconciled_data1)

    # For dataset 2, need special handling for string->float conversion
    try:
        # Convert score from string to float
        score_array = data2.column("score").cast(pa.float64())

        # Convert active from int to bool
        active_array = data2.column("active").cast(pa.bool_())

        # Build reconciled table
        reconciled_data2 = pa.table(
            {
                "id": data2.column("id").cast(pa.int64()),
                "score": score_array,
                "active": active_array,
            },
            schema=reconciled_schema,
        )

        print(f"\nDataset 2 after reconciliation:")
        print(reconciled_data2)

        # Combine reconciled data
        combined = pa.concat_tables([reconciled_data1, reconciled_data2])
        print(f"\n✅ Combined reconciled data:")
        print(combined)

    except Exception as e:
        print(f"\n❌ Reconciliation failed: {e}")


def demonstrate_partitioned_schema_handling():
    """Demonstrate schema handling in partitioned datasets."""

    print("\n📁 Partitioned Dataset Schema Handling")

    temp_dir = Path(tempfile.mkdtemp())
    partitioned_path = temp_dir / "partitioned_data"

    # Create partitioned dataset with schema evolution
    # Partition 1: Initial schema
    data_v1 = pa.table(
        {
            "id": pa.array([1, 2, 3]),
            "value": pa.array([10.0, 20.0, 30.0]),
            "category": pa.array(["A", "B", "A"]),
        }
    )

    # Partition 2: Schema with additional column
    data_v2 = pa.table(
        {
            "id": pa.array([4, 5, 6]),
            "value": pa.array([40.0, 50.0, 60.0]),
            "category": pa.array(["B", "C", "A"]),
            "description": pa.array(["desc1", "desc2", "desc3"]),
        }
    )

    # Partition 3: Schema with different data types
    data_v3 = pa.table(
        {
            "id": pa.array([7, 8]),
            "value": pa.array(["70.0", "80.0"]),  # String instead of float
            "category": pa.array(["C", "B"]),
            "metadata": pa.array([{"key": "val"}, {"key2": "val2"}]),  # Struct type
        }
    )

    # Write as partitioned dataset
    partitions = [
        (data_v1, "version=v1"),
        (data_v2, "version=v2"),
        (data_v3, "version=v3"),
    ]

    for data, partition in partitions:
        part_path = partitioned_path / partition
        part_path.mkdir(parents=True, exist_ok=True)
        pq.write_table(data, part_path / "data.parquet")

    print(f"Created partitioned dataset at: {partitioned_path}")

    # Load as Arrow dataset
    try:
        dataset = ds.dataset(partitioned_path, format="parquet", partitioning="version")

        print(f"\nDataset schema:")
        print(dataset.schema)

        # Try to read all data
        print(f"\n📊 Reading all data:")
        try:
            full_table = dataset.to_table()
            print(f"Successfully read {len(full_table)} rows")
            print(f"Final schema: {full_table.schema}")
        except Exception as e:
            print(f"❌ Failed to read all data: {e}")

            # Try reading each partition separately
            print(f"\nReading partitions separately:")
            for partition in ["v1", "v2", "v3"]:
                try:
                    partition_filter = ds.field("version") == partition
                    part_table = dataset.to_table(filter=partition_filter)
                    print(
                        f"  {partition}: {len(part_table)} rows, schema: {part_table.schema}"
                    )
                except Exception as pe:
                    print(f"  {partition}: Error - {pe}")

    except Exception as e:
        print(f"❌ Failed to create dataset: {e}")

    # Cleanup
    import shutil

    shutil.rmtree(temp_dir)


def main():
    """Run all schema unification examples."""

    print("🔀 Schema Unification Example")
    print("=" * 50)

    # Create test datasets
    dataset_paths = create_multiple_datasets_with_varying_schemas()

    try:
        # Run all demonstrations
        analyze_schema_differences(dataset_paths)
        demonstrate_schema_unification(dataset_paths)
        demonstrate_column_mapping()
        demonstrate_type_reconciliation()
        demonstrate_partitioned_schema_handling()

        print("\n" + "=" * 50)
        print("✅ All schema unification examples completed!")

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(dataset_paths[0].parent)
        print(f"\n🧹 Cleaned up temporary directory")


if __name__ == "__main__":
    main()
