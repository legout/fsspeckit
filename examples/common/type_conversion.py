"""
Type Conversion Example

This example demonstrates how to use fsspeckit's type conversion utilities
to handle data transformations between different formats and systems.

The example covers:
1. Converting between PyArrow and other formats
2. Pandas DataFrame conversion and optimization
3. Polars DataFrame conversion
4. Type-safe conversion patterns
5. Memory-efficient conversion strategies
6. Handling conversion errors and edge cases

This example is designed for users who need to work with data across
different processing frameworks and formats.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Try to import pandas and polars, but handle gracefully if not available
try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None

from fsspeckit.common.types import to_pyarrow_table, dict_to_dataframe


def create_sample_data() -> pa.Table:
    """Create sample data for conversion testing."""

    print("Creating sample data...")

    # Generate diverse data with different types
    records = []
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]

    for i in range(1000):
        record = {
            "id": i + 1,
            "name": f"Product_{i:03d}",
            "category": categories[i % len(categories)],
            "brand": brands[i % len(brands)],
            "price": round((i * 1.5 + 10), 2),
            "quantity": i % 100 + 1,
            "in_stock": i % 3 != 0,  # Some out of stock
            "created_date": (datetime(2024, 1, 1) + timedelta(days=i % 365)).strftime(
                "%Y-%m-%d"
            ),
            "rating": round((i % 50) / 10 + 1, 1),
            "tags": [f"tag_{j}" for j in range(i % 5)],  # List data
        }
        records.append(record)

    # Convert to PyArrow table using pa.Table.from_pydict
    data = pa.Table.from_pydict(
        {
            "id": [r["id"] for r in records],
            "name": [r["name"] for r in records],
            "category": [r["category"] for r in records],
            "brand": [r["brand"] for r in records],
            "price": [r["price"] for r in records],
            "quantity": [r["quantity"] for r in records],
            "in_stock": [r["in_stock"] for r in records],
            "created_date": [r["created_date"] for r in records],
            "rating": [r["rating"] for r in records],
            "tags": [r["tags"] for r in records],
        }
    )
    print(f"Created dataset with {len(data)} rows and {len(data.schema)} columns")
    return data


def demonstrate_basic_pyarrow_conversions():
    """Demonstrate basic PyArrow table operations."""

    print("\n🔹 Basic PyArrow Table Operations")

    # Create sample data
    data = create_sample_data()

    print("Original table schema:")
    for field in data.schema:
        print(f"  {field.name}: {field.type}")

    # Basic conversions and transformations
    print(f"\n📊 Table Information:")
    print(f"  Rows: {len(data):,}")
    print(f"  Columns: {len(data.schema)}")
    print(f"  Memory usage: {data.nbytes / 1024 / 1024:.2f} MB")

    # Type conversions within PyArrow
    print(f"\n🔄 Type Conversions:")

    # Convert price to float32 for memory efficiency
    price_col = data.column("price")
    price_float32 = price_col.cast(pa.float32())
    print(f"  Price: {price_col.type} -> {price_float32.type} (memory reduction)")

    # Convert quantity to smaller integer type if possible
    quantity_col = data.column("quantity")
    quantity_stats = pc.min_max(quantity_col).as_py()
    # Handle the case where min_max returns string keys
    if isinstance(quantity_stats, dict):
        quantity_min = quantity_stats.get("min", 0)
        quantity_max = quantity_stats.get("max", 255)
    else:
        quantity_min, quantity_max = quantity_stats

    try:
        quantity_min = int(quantity_min) if quantity_min is not None else 0
        quantity_max = int(quantity_max) if quantity_max is not None else 255
    except (ValueError, TypeError):
        quantity_min, quantity_max = 0, 255

    if quantity_min >= 0 and quantity_max <= 255:
        quantity_uint8 = quantity_col.cast(pa.uint8())
        print(
            f"  Quantity: {quantity_col.type} -> uint8 (range: {quantity_min}-{quantity_max})"
        )

    # Convert category to dictionary for efficiency
    category_col = data.column("category")
    category_dict = pc.dictionary_encode(category_col)
    print(
        f"  Category: {category_col.type} -> {category_dict.type} (dictionary encoding)"
    )

    # Create optimized table
    optimized_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field("category", pa.dictionary(pa.int8(), pa.string())),
            pa.field("brand", pa.dictionary(pa.int8(), pa.string())),
            pa.field("price", pa.float32()),
            pa.field("quantity", pa.uint8()),
            pa.field("in_stock", pa.bool_()),
            pa.field("created_date", pa.string()),
            pa.field("rating", pa.float32()),
            pa.field("tags", pa.list_(pa.string())),
        ]
    )

    try:
        optimized_table = data.cast(optimized_schema)
        memory_reduction = (data.nbytes - optimized_table.nbytes) / data.nbytes * 100

        print(f"\n💾 Memory Optimization:")
        print(f"  Original:  {data.nbytes / 1024 / 1024:.2f} MB")
        print(f"  Optimized: {optimized_table.nbytes / 1024 / 1024:.2f} MB")
        print(f"  Reduction: {memory_reduction:.1f}%")

        return optimized_table

    except Exception as e:
        print(f"  ⚠️  Optimization failed: {e}")
        return data


def demonstrate_pandas_conversion():
    """Demonstrate PyArrow to Pandas conversion."""

    if not PANDAS_AVAILABLE:
        print("\n🐼 Pandas Conversion")
        print("  ⚠️  Pandas not available. Install with: pip install pandas")
        return None

    print("\n🐼 PyArrow to Pandas Conversion")

    # Get sample data
    arrow_table = create_sample_data()

    # Convert to pandas DataFrame
    print("Converting PyArrow table to pandas DataFrame...")

    try:
        # Convert to pandas DataFrame directly
        pandas_df = arrow_table.to_pandas()

        print("✅ Conversion successful")
        print(f"  DataFrame shape: {pandas_df.shape}")
        print(
            f"  Memory usage: {pandas_df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
        )

        # Show data types comparison
        print(f"\n📋 Data Type Comparison:")
        print("  PyArrow -> Pandas")
        for field in arrow_table.schema:
            pandas_dtype = str(pandas_df[field.name].dtype)
            print(f"  {field.name}: {field.type} -> {pandas_dtype}")

        # Demonstrate pandas-specific optimizations
        print(f"\n⚡ Pandas Optimizations:")

        # Convert categorical columns
        categorical_cols = ["category", "brand"]
        for col in categorical_cols:
            if col in pandas_df.columns:
                pandas_df[col] = pandas_df[col].astype("category")
                print(f"  {col}: converted to category")

        # Convert numeric columns to optimal types
        for col in ["price", "quantity", "rating"]:
            if col in pandas_df.columns:
                pandas_df[col] = pd.to_numeric(
                    pandas_df[col], downcast="float" if "price" in col else "integer"
                )
                print(f"  {col}: downcast to {pandas_df[col].dtype}")

        # Calculate memory savings
        original_memory = arrow_table.nbytes
        optimized_memory = pandas_df.memory_usage(deep=True).sum()
        savings = (original_memory - optimized_memory) / original_memory * 100

        print(f"\n💾 Memory Comparison:")
        print(f"  PyArrow: {original_memory / 1024 / 1024:.2f} MB")
        print(f"  Pandas:   {optimized_memory / 1024 / 1024:.2f} MB")
        print(f"  Savings:  {savings:.1f}%")

        return pandas_df

    except Exception as e:
        print(f"❌ Conversion failed: {e}")
        return None


def demonstrate_polars_conversion():
    """Demonstrate PyArrow to Polars conversion."""

    if not POLARS_AVAILABLE:
        print("\n🔥 Polars Conversion")
        print("  ⚠️  Polars not available. Install with: pip install polars")
        return None

    print("\n🔥 PyArrow to Polars Conversion")

    # Get sample data
    arrow_table = create_sample_data()

    # Convert to Polars DataFrame
    print("Converting PyArrow table to Polars DataFrame...")

    try:
        # Convert to Polars DataFrame directly
        polars_df = pl.from_arrow(arrow_table)

        print("✅ Conversion successful")
        print(f"  DataFrame shape: {polars_df.shape}")
        print(f"  Estimated memory: {polars_df.estimated_size('mb'):.2f} MB")

        # Show data types comparison
        print(f"\n📋 Data Type Comparison:")
        print("  PyArrow -> Polars")
        for field in arrow_table.schema:
            if field.name in polars_df.columns:
                polars_dtype = str(polars_df[field.name].dtype)
                print(f"  {field.name}: {field.type} -> {polars_dtype}")

        # Demonstrate Polars-specific optimizations
        print(f"\n⚡ Polars Optimizations:")

        # Convert to categorical
        categorical_cols = ["category", "brand"]
        for col in categorical_cols:
            if col in polars_df.columns:
                polars_df = polars_df.with_columns(pl.col(col).cast(pl.Categorical))
                print(f"  {col}: converted to Categorical")

        # Convert to optimal numeric types
        polars_df = polars_df.with_columns(
            [
                pl.col("price").cast(pl.Float32),
                pl.col("quantity").cast(pl.Int16),
                pl.col("rating").cast(pl.Float32),
            ]
        )
        print("  Numeric columns: converted to optimal types")

        # Compare memory usage
        arrow_memory = arrow_table.nbytes / 1024 / 1024
        polars_memory = polars_df.estimated_size("mb")

        print(f"\n💾 Memory Comparison:")
        print(f"  PyArrow: {arrow_memory:.2f} MB")
        print(f"  Polars:   {polars_memory:.2f} MB")
        print(f"  Ratio:    {arrow_memory / polars_memory:.2f}x")

        return polars_df

    except Exception as e:
        print(f"❌ Conversion failed: {e}")
        return None


def demonstrate_cross_format_operations():
    """Demonstrate operations across different formats."""

    print("\n🔄 Cross-Format Operations")

    # Create sample data
    arrow_table = create_sample_data()

    # Save to different formats
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Parquet (Arrow-native)
        parquet_path = temp_dir / "data.parquet"
        pq.write_table(arrow_table, parquet_path)

        # CSV (universal format)
        csv_path = temp_dir / "data.csv"
        arrow_table.to_pandas().to_csv(csv_path, index=False)

        # Read back and compare
        print("Reading back from different formats...")

        # From Parquet
        arrow_from_parquet = pq.read_table(parquet_path)
        print(f"✅ Parquet read: {len(arrow_from_parquet)} rows")

        # From CSV (requires pandas)
        if PANDAS_AVAILABLE:
            pandas_from_csv = pd.read_csv(csv_path)
            arrow_from_csv = pa.Table.from_pandas(pandas_from_csv)
            print(f"✅ CSV read: {len(arrow_from_csv)} rows")

            # Compare results
            print(f"\n📊 Format Comparison:")
            print(f"  Original Arrow: {len(arrow_table)} rows")
            print(f"  From Parquet:   {len(arrow_from_parquet)} rows")
            print(f"  From CSV:       {len(arrow_from_csv)} rows")

            # Check data integrity
            price_diff = pc.subtract(
                arrow_table.column("price"), arrow_from_parquet.column("price")
            )
            max_price_diff = pc.max(pc.abs(price_diff)).as_py()

            print(f"  Price accuracy (Parquet): {max_price_diff}")

            # File size comparison
            parquet_size = parquet_path.stat().st_size
            csv_size = csv_path.stat().st_size

            print(f"\n💾 File Size Comparison:")
            print(f"  Parquet: {parquet_size / 1024:.1f} KB")
            print(f"  CSV:     {csv_size / 1024:.1f} KB")
            print(f"  Ratio:   {csv_size / parquet_size:.1f}x")

    except Exception as e:
        print(f"❌ Cross-format operation failed: {e}")

    finally:
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir)


def demonstrate_type_safe_conversions():
    """Demonstrate type-safe conversion patterns."""

    print("\n🛡️  Type-Safe Conversion Patterns")

    # Create data with potential type issues
    problematic_data = pa.table(
        {
            "mixed_numbers": pa.array(["1", "2.5", "3", "invalid", "5"]),
            "dates": pa.array(
                ["2024-01-01", "2024-02-30", "2024-03-15", None, "2024-04-01"]
            ),
            "booleans": pa.array(["true", "false", "1", "0", "maybe"]),
            "ids": pa.array([1, 2, None, 4, 5]),  # Fixed: removed invalid string
        }
    )

    print("Problematic data:")
    for field in problematic_data.schema:
        column = problematic_data.column(field.name)
        print(f"  {field.name}: {column.type} - {column.to_pylist()}")

    # Safe conversion functions
    def safe_convert_to_int(column: pa.Array, default_value=0) -> pa.Array:
        """Safely convert array to integers."""
        try:
            # Try direct conversion first
            return column.cast(pa.int64())
        except:
            # Fall back to string parsing
            results = []
            for val in column.to_pylist():
                try:
                    if val is None:
                        results.append(None)
                    elif isinstance(val, str):
                        results.append(int(val) if val.isdigit() else default_value)
                    else:
                        results.append(int(val))
                except:
                    results.append(default_value)
            return pa.array(results, type=pa.int64())

    def safe_convert_to_float(column: pa.Array, default_value=0.0) -> pa.Array:
        """Safely convert array to floats."""
        try:
            return column.cast(pa.float64())
        except:
            results = []
            for val in column.to_pylist():
                try:
                    if val is None:
                        results.append(None)
                    elif isinstance(val, str):
                        results.append(
                            float(val)
                            if val.replace(".", "").isdigit()
                            else default_value
                        )
                    else:
                        results.append(float(val))
                except:
                    results.append(default_value)
            return pa.array(results, type=pa.float64())

    def safe_convert_to_bool(column: pa.Array, default_value=False) -> pa.Array:
        """Safely convert array to booleans."""
        try:
            return column.cast(pa.bool_())
        except:
            results = []
            for val in column.to_pylist():
                if val is None:
                    results.append(None)
                elif isinstance(val, str):
                    val_lower = val.lower()
                    if val_lower in ["true", "1", "yes", "on"]:
                        results.append(True)
                    elif val_lower in ["false", "0", "no", "off"]:
                        results.append(False)
                    else:
                        results.append(default_value)
                else:
                    results.append(bool(val) if val is not None else default_value)
            return pa.array(results, type=pa.bool_())

    # Apply safe conversions
    print(f"\n🔄 Safe Conversions:")

    try:
        # Convert mixed numbers
        mixed_col = problematic_data.column("mixed_numbers")
        safe_mixed = safe_convert_to_float(mixed_col)
        print(f"  mixed_numbers: {mixed_col.type} -> {safe_mixed.type}")
        print(f"    Original: {mixed_col.to_pylist()}")
        print(f"    Converted: {safe_mixed.to_pylist()}")

        # Convert IDs
        ids_col = problematic_data.column("ids")
        safe_ids = safe_convert_to_int(ids_col)
        print(f"  ids: {ids_col.type} -> {safe_ids.type}")
        print(f"    Original: {ids_col.to_pylist()}")
        print(f"    Converted: {safe_ids.to_pylist()}")

        # Convert booleans
        bool_col = problematic_data.column("booleans")
        safe_bool = safe_convert_to_bool(bool_col)
        print(f"  booleans: {bool_col.type} -> {safe_bool.type}")
        print(f"    Original: {bool_col.to_pylist()}")
        print(f"    Converted: {safe_bool.to_pylist()}")

    except Exception as e:
        print(f"❌ Safe conversion failed: {e}")


def demonstrate_memory_efficient_conversions():
    """Demonstrate memory-efficient conversion strategies."""

    print("\n💾 Memory-Efficient Conversions")

    # Create large dataset to show memory considerations
    print("Creating large dataset...")
    large_data = pa.table(
        {
            "id": pa.array(range(50000)),
            "value": pa.array([x * 1.1 for x in range(50000)]),
            "category": pa.array([f"Category_{x % 100}" for x in range(50000)]),
            "description": pa.array(
                [f"Description for item {x}" for x in range(50000)]
            ),
        }
    )

    original_memory = large_data.nbytes / 1024 / 1024
    print(f"Large dataset: {len(large_data):,} rows, {original_memory:.2f} MB")

    # Strategy 1: Column projection
    print(f"\n1. Column Projection:")
    projected = large_data.select(["id", "value"])
    projected_memory = projected.nbytes / 1024 / 1024
    reduction = (1 - projected_memory / original_memory) * 100
    print(
        f"   Selected 2/4 columns: {projected_memory:.2f} MB ({reduction:.1f}% reduction)"
    )

    # Strategy 2: Row filtering before conversion
    print(f"\n2. Early Filtering:")
    filtered = large_data.filter(pc.greater(large_data.column("value"), 25000))
    filtered_memory = filtered.nbytes / 1024 / 1024
    reduction = (1 - filtered_memory / original_memory) * 100
    print(
        f"   Filtered >25000: {len(filtered):,} rows, {filtered_memory:.2f} MB ({reduction:.1f}% reduction)"
    )

    # Strategy 3: Type optimization before pandas conversion
    if PANDAS_AVAILABLE:
        print(f"\n3. Optimized Pandas Conversion:")

        # Convert with PyArrow optimizations first
        optimized_arrow = large_data.cast(
            pa.schema(
                [
                    pa.field("id", pa.int32()),
                    pa.field("value", pa.float32()),
                    pa.field("category", pa.dictionary(pa.int16(), pa.string())),
                    pa.field("description", pa.string()),
                ]
            )
        )

        # Convert to pandas
        pandas_direct = large_data.to_pandas()
        pandas_optimized = optimized_arrow.to_pandas()

        direct_memory = pandas_direct.memory_usage(deep=True).sum() / 1024 / 1024
        optimized_memory = pandas_optimized.memory_usage(deep=True).sum() / 1024 / 1024
        savings = (1 - optimized_memory / direct_memory) * 100

        print(f"   Direct conversion: {direct_memory:.2f} MB")
        print(
            f"   Optimized conversion: {optimized_memory:.2f} MB ({savings:.1f}% savings)"
        )

    # Strategy 4: Chunked processing
    print(f"\n4. Chunked Processing:")
    chunk_size = 10000
    processed_chunks = []

    for i in range(0, len(large_data), chunk_size):
        chunk = large_data.slice(i, min(chunk_size, len(large_data) - i))
        # Process chunk here (e.g., convert, filter, transform)
        processed_chunks.append(chunk)

    recombined = pa.concat_tables(processed_chunks)
    print(f"   Processed in {len(processed_chunks)} chunks of {chunk_size:,} rows")
    print(
        f"   Result: {len(recombined):,} rows, {recombined.nbytes / 1024 / 1024:.2f} MB"
    )


def main():
    """Run all type conversion examples."""

    print("🔄 Type Conversion Example")
    print("=" * 50)

    try:
        # Run all conversion demonstrations
        optimized_arrow = demonstrate_basic_pyarrow_conversions()
        pandas_df = demonstrate_pandas_conversion()
        polars_df = demonstrate_polars_conversion()
        demonstrate_cross_format_operations()
        demonstrate_type_safe_conversions()
        demonstrate_memory_efficient_conversions()

        print("\n" + "=" * 50)
        print("✅ All type conversion examples completed!")

        print("\n💡 Key Takeaways:")
        print("• Choose the right format for your use case")
        print("• Optimize data types to reduce memory usage")
        print("• Use safe conversion patterns for unreliable data")
        print("• Apply filters and projections early to save memory")
        print("• Consider chunked processing for large datasets")
        print("• Test performance with your actual data patterns")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()
