"""
DuckDB Upsert Operations - Getting Started

This example demonstrates how to perform UPSERT (insert or update) operations
using DuckDB with PyArrow tables.

The example covers:
1. Basic UPSERT concepts with DuckDB
2. Creating tables from PyArrow data
3. INSERT ... ON CONFLICT DO UPDATE syntax
4. Batched upserts for large datasets
5. Comparison with PyArrow merge-aware writes
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency 'pyarrow'. Install with: pip install -e \".[datasets]\" "
        "(or run `uv sync` then `uv run python ...`)."
    ) from exc

try:
    from fsspeckit.datasets import DuckDBParquetHandler
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing fsspeckit dataset dependencies. Install with: pip install -e \".[datasets]\" "
        "(or run `uv sync` then `uv run python ...`)."
    ) from exc


def create_sample_data():
    """Create sample data for upsert demonstrations."""

    # Initial customer data
    initial_customers = pa.Table.from_pydict(
        {
            "customer_id": [1, 2, 3, 4, 5],
            "name": [
                "Alice Johnson",
                "Bob Smith",
                "Carol Davis",
                "David Lee",
                "Eve Wilson",
            ],
            "email": [
                "alice@example.com",
                "bob@example.com",
                "carol@example.com",
                "david@example.com",
                "eve@example.com",
            ],
            "segment": ["premium", "standard", "premium", "standard", "premium"],
            "total_spend": [1500.00, 250.00, 3200.00, 180.00, 950.00],
            "last_purchase": ["2024-01-15", "2024-01-10", "2024-01-18", "2024-01-05", "2024-01-12"],
            "updated_at": ["2024-01-01"] * 5,
        }
    )

    # Customer updates (some existing, some new)
    customer_updates = pa.Table.from_pydict(
        {
            "customer_id": [2, 3, 6, 7],  # Bob & Carol exist, Diana & Frank are new
            "name": [
                "Robert Smith",  # Bob updated name
                "Carol Williams",  # Carol updated name
                "Diana Prince",  # New customer
                "Frank Miller",  # New customer
            ],
            "email": [
                "robert@example.com",
                "carol.w@example.com",
                "diana@example.com",
                "frank@example.com",
            ],
            "segment": ["premium", "premium", "standard", "standard"],
            "total_spend": [450.00, 3800.00, 0.00, 0.00],  # New customers start at 0
            "last_purchase": ["2024-01-20", "2024-01-22", "2024-01-20", "2024-01-21"],
            "updated_at": ["2024-01-20"] * 4,
        }
    )

    # Product catalog
    initial_products = pa.Table.from_pydict(
        {
            "product_id": [101, 102, 103, 104, 105],
            "name": ["Laptop Pro", "Wireless Mouse", "Mechanical Keyboard", "USB-C Hub", "Webcam HD"],
            "category": ["Electronics"] * 5,
            "price": [1299.99, 29.99, 149.99, 49.99, 89.99],
            "stock": [50, 200, 75, 150, 100],
            "updated_at": ["2024-01-01"] * 5,
        }
    )

    # Price/stock updates
    product_updates = pa.Table.from_pydict(
        {
            "product_id": [101, 102, 106, 107],  # Laptop & Mouse exist, new products
            "name": ["Laptop Pro Max", "Ultra Mouse", "Monitor 4K", "Headphones Pro"],
            "category": ["Electronics", "Electronics", "Electronics", "Electronics"],
            "price": [1599.99, 49.99, 499.99, 299.99],
            "stock": [30, 180, 25, 60],
            "updated_at": ["2024-01-20"] * 4,
        }
    )

    return {
        "initial_customers": initial_customers,
        "customer_updates": customer_updates,
        "initial_products": initial_products,
        "product_updates": product_updates,
    }


def demonstrate_basic_upsert():
    """Demonstrate basic UPSERT functionality with DuckDB."""
    print("\n🔄 Basic DuckDB UPSERT")
    print("=" * 50)

    temp_dir = Path(tempfile.mkdtemp())
    data = create_sample_data()

    try:
        with DuckDBParquetHandler() as handler:
            # Step 1: Create initial table from PyArrow table using DuckDB's native registration
            print("\n📥 Step 1: Creating initial customer table...")
            conn = handler._connection.connection
            conn.register("initial_customers", data["initial_customers"])
            # Create table with PRIMARY KEY constraint for ON CONFLICT to work
            conn.execute("""
                CREATE OR REPLACE TABLE customers (
                    customer_id INTEGER PRIMARY KEY,
                    name VARCHAR,
                    email VARCHAR,
                    segment VARCHAR,
                    total_spend DECIMAL(10, 2),
                    last_purchase VARCHAR,
                    updated_at VARCHAR
                )
            """)
            conn.execute("INSERT INTO customers SELECT * FROM initial_customers")

            # Verify initial data
            result = handler.execute_sql("SELECT * FROM customers ORDER BY customer_id").fetchdf()
            print(f"   Initial customers: {len(result)}")
            print(result.to_string(index=False))

            # Step 2: Perform UPSERT
            print("\n📝 Step 2: Performing UPSERT with customer updates...")
            print("   Customer 2 (Bob) → Update (new name, segment, spend)")
            print("   Customer 3 (Carol) → Update (new name, email)")
            print("   Customers 6,7 (Diana, Frank) → Insert")

            # DuckDB UPSERT syntax: INSERT ... ON CONFLICT DO UPDATE
            # Use VALUES clause with registered table for batch insert
            conn.register("customer_updates", data["customer_updates"])
            conn.execute("""
                INSERT INTO customers
                    (customer_id, name, email, segment, total_spend, last_purchase, updated_at)
                SELECT customer_id, name, email, segment, total_spend, last_purchase, updated_at
                FROM customer_updates
                ON CONFLICT (customer_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    segment = EXCLUDED.segment,
                    total_spend = EXCLUDED.total_spend,
                    last_purchase = EXCLUDED.last_purchase,
                    updated_at = EXCLUDED.updated_at
            """)

            # Step 3: Verify results
            print("\n🔍 Step 3: Verifying UPSERT results...")
            result = handler.execute_sql("SELECT * FROM customers ORDER BY customer_id").fetchdf()
            print(f"   Final customers: {len(result)}")
            print(result.to_string(index=False))

            # Show changes
            print("\n📊 Changes Summary:")
            updated = result[result["customer_id"].isin([2, 3])]
            new = result[result["customer_id"].isin([6, 7])]
            print(f"   Updated records: {len(updated)}")
            print(f"   New records inserted: {len(new)}")

    finally:
        import shutil
        shutil.rmtree(temp_dir)


def demonstrate_upsert_with_parquet():
    """Demonstrate UPSERT using Parquet files as source."""
    print("\n📁 UPSERT with Parquet File Sources")
    print("=" * 50)

    temp_dir = Path(tempfile.mkdtemp())
    data = create_sample_data()

    try:
        # Save data to Parquet files
        customers_file = temp_dir / "customers.parquet"
        updates_file = temp_dir / "customer_updates.parquet"
        pq.write_table(data["initial_customers"], customers_file)
        pq.write_table(data["customer_updates"], updates_file)

        with DuckDBParquetHandler() as handler:
            # Step 1: Create table from Parquet with PRIMARY KEY
            print("\n📥 Step 1: Creating table from Parquet file...")
            conn = handler._connection.connection
            conn.execute(f"""
                CREATE OR REPLACE TABLE customers (
                    customer_id INTEGER PRIMARY KEY,
                    name VARCHAR,
                    email VARCHAR,
                    segment VARCHAR,
                    total_spend DECIMAL(10, 2),
                    last_purchase VARCHAR,
                    updated_at VARCHAR
                )
            """)
            conn.execute(f"""
                INSERT INTO customers
                SELECT * FROM read_parquet('{customers_file}')
            """)

            # Step 2: UPSERT using Parquet file directly
            print("\n📝 Step 2: Performing UPSERT directly from Parquet...")
            print("   Using INSERT ... ON CONFLICT with read_parquet()")

            handler.execute_sql(f"""
                INSERT INTO customers
                    (customer_id, name, email, segment, total_spend, last_purchase, updated_at)
                SELECT customer_id, name, email, segment, total_spend, last_purchase, updated_at
                FROM read_parquet('{updates_file}')
                ON CONFLICT (customer_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    segment = EXCLUDED.segment,
                    total_spend = EXCLUDED.total_spend,
                    last_purchase = EXCLUDED.last_purchase,
                    updated_at = EXCLUDED.updated_at
            """)

            # Step 3: Verify results
            print("\n🔍 Step 3: Verifying results...")
            result = handler.execute_sql(
                "SELECT * FROM customers ORDER BY customer_id"
            ).fetchdf()
            print(f"   Total customers: {len(result)}")
            print(result.to_string(index=False))

    finally:
        import shutil
        shutil.rmtree(temp_dir)


def demonstrate_batched_upsert():
    """Demonstrate batched upserts for large datasets."""
    print("\n📦 Batched UPSERT for Large Datasets")
    print("=" * 50)

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create larger dataset
        import random

        print("\n🔧 Generating test dataset (1000 records)...")
        initial_records = []
        update_records = []

        for i in range(500):
            initial_records.append({
                "id": i + 1,
                "name": f"Customer {i + 1}",
                "value": random.randint(1, 1000),
                "category": random.choice(["A", "B", "C"]),
            })

        for i in range(600):  # 400 updates + 200 new
            update_records.append({
                "id": i + 1 if i < 400 else 1001 + (i - 400),
                "name": f"Updated Customer {i + 1}" if i < 400 else f"New Customer {i + 501}",
                "value": random.randint(1, 1000),
                "category": random.choice(["A", "B", "C"]),
            })

        initial_table = pa.Table.from_pylist(initial_records)
        update_table = pa.Table.from_pylist(update_records)

        with DuckDBParquetHandler() as handler:
            # Create initial table
            conn = handler._connection.connection
            conn.register("initial_data", initial_table)
            conn.execute("""
                CREATE OR REPLACE TABLE test_data (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR,
                    value INTEGER,
                    category VARCHAR
                )
            """)
            conn.execute("INSERT INTO test_data SELECT * FROM initial_data")
            print(f"   Initial records: {len(initial_table)}")

            # Perform batched upsert
            print("\n📝 Performing batched upsert (batch size: 100)...")
            batch_size = 100
            total_batches = (len(update_table) + batch_size - 1) // batch_size

            import time
            start_time = time.time()

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(update_table))
                batch = update_table.slice(start_idx, end_idx - start_idx)

                # Use registered table for batch upsert
                batch_name = f"batch_{batch_num}"
                conn.register(batch_name, batch)
                conn.execute(f"""
                    INSERT INTO test_data (id, name, value, category)
                    SELECT id, name, value, category FROM {batch_name}
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        value = EXCLUDED.value,
                        category = EXCLUDED.category
                """)

                print(f"   Batch {batch_num + 1}/{total_batches}: {len(batch)} records")

            elapsed = time.time() - start_time

            # Verify results
            result = handler.execute_sql("SELECT COUNT(*) as cnt FROM test_data").fetchdf()
            print(f"\n✅ Batched upsert completed in {elapsed:.2f}s")
            print(f"   Final record count: {result['cnt'].iloc[0]}")

    finally:
        import shutil
        shutil.rmtree(temp_dir)


def demonstrate_partial_upsert():
    """Demonstrate upsert with partial column updates."""
    print("\n✏️  Partial Column Updates")
    print("=" * 50)

    temp_dir = Path(tempfile.mkdtemp())
    data = create_sample_data()

    try:
        with DuckDBParquetHandler() as handler:
            # Create initial products table
            print("\n📥 Creating initial product catalog...")
            conn = handler._connection.connection
            conn.register("initial_products", data["initial_products"])
            # Create table with PRIMARY KEY for ON CONFLICT
            conn.execute("""
                CREATE OR REPLACE TABLE products (
                    product_id INTEGER PRIMARY KEY,
                    name VARCHAR,
                    category VARCHAR,
                    price DECIMAL(10, 2),
                    stock INTEGER,
                    updated_at VARCHAR
                )
            """)
            conn.execute("INSERT INTO products SELECT * FROM initial_products")
            result = handler.execute_sql("SELECT * FROM products ORDER BY product_id").fetchdf()
            print(result.to_string(index=False))

            # Only update price and stock (keep other columns)
            print("\n📝 Performing partial update (price + stock only)...")
            handler.execute_sql("""
                INSERT INTO products
                    (product_id, name, category, price, stock, updated_at)
                SELECT product_id, name, category, price, stock, updated_at
                FROM (
                    VALUES
                        (101, 'Laptop Pro', 'Electronics', 1599.99, 30, '2024-01-20'),
                        (102, 'Wireless Mouse', 'Electronics', 49.99, 180, '2024-01-20'),
                        (106, 'Monitor 4K', 'Electronics', 499.99, 25, '2024-01-20'),
                        (107, 'Headphones Pro', 'Electronics', 299.99, 60, '2024-01-20')
                ) AS new_products(product_id, name, category, price, stock, updated_at)
                ON CONFLICT (product_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    stock = EXCLUDED.stock,
                    updated_at = EXCLUDED.updated_at
            """)

            print("\n🔍 Results after partial update...")
            result = handler.execute_sql("SELECT * FROM products ORDER BY product_id").fetchdf()
            print(result.to_string(index=False))

            print("\n💡 Key Insight: Only specified columns are updated")
            print("   - Product names for new items (106, 107) are inserted")
            print("   - Existing products (101, 102) only have price/stock updated")

    finally:
        import shutil
        shutil.rmtree(temp_dir)


def demonstrate_conflict_resolution():
    """Demonstrate different conflict resolution strategies."""
    print("\n⚔️  Conflict Resolution Strategies")
    print("=" * 50)

    temp_dir = Path(tempfile.mkdtemp())

    try:
        with DuckDBParquetHandler() as handler:
            # Create sample data with potential conflicts
            conn = handler._connection.connection
            conn.execute("""
                CREATE TABLE inventory (
                    sku VARCHAR PRIMARY KEY,
                    product_name VARCHAR,
                    quantity INT,
                    price DECIMAL(10, 2),
                    last_updated DATE
                )
            """)

            # Insert initial data
            handler.execute_sql("""
                INSERT INTO inventory VALUES
                    ('SKU001', 'Widget A', 100, 19.99, '2024-01-15'),
                    ('SKU002', 'Widget B', 50, 29.99, '2024-01-15'),
                    ('SKU003', 'Widget C', 75, 39.99, '2024-01-15')
            """)

            print("📋 Initial inventory:")
            result = handler.execute_sql("SELECT * FROM inventory ORDER BY sku").fetchdf()
            print(result.to_string(index=False))

            # Simulate multiple updates coming in
            print("\n📝 Scenario: Two update batches arrive with conflicting SKUs")

            # Update batch 1: Adds stock
            handler.execute_sql("""
                INSERT INTO inventory (sku, product_name, quantity, price, last_updated)
                VALUES
                    ('SKU001', 'Widget A', 25, 19.99, '2024-01-20'),
                    ('SKU004', 'Widget D', 30, 49.99, '2024-01-20')
                ON CONFLICT (sku) DO UPDATE SET
                    quantity = inventory.quantity + EXCLUDED.quantity,
                    last_updated = EXCLUDED.last_updated
            """)

            print("\n✅ After Batch 1 (add 25 to SKU001):")
            result = handler.execute_sql("SELECT * FROM inventory ORDER BY sku").fetchdf()
            print(result.to_string(index=False))

            # Update batch 2: Sets exact quantity (replacement)
            handler.execute_sql("""
                INSERT INTO inventory (sku, product_name, quantity, price, last_updated)
                VALUES
                    ('SKU001', 'Widget A', 200, 24.99, '2024-01-21'),
                    ('SKU005', 'Widget E', 40, 59.99, '2024-01-21')
                ON CONFLICT (sku) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    price = EXCLUDED.price,
                    last_updated = EXCLUDED.last_updated
            """)

            print("\n✅ After Batch 2 (replace SKU001 quantity):")
            result = handler.execute_sql("SELECT * FROM inventory ORDER BY sku").fetchdf()
            print(result.to_string(index=False))

            print("\n📊 Summary of Strategies:")
            print("   • ADD: inventory.quantity + EXCLUDED.quantity (cumulative)")
            print("   • REPLACE: EXCLUDED.* (full replacement)")
            print("   • MAX/MIN: GREATEST/LEAST (conditional updates)")

    finally:
        import shutil
        shutil.rmtree(temp_dir)


def demonstrate_upsert_vs_pyarrow_merge():
    """Compare DuckDB UPSERT with PyArrow merge-aware writes."""
    print("\n⚖️  DuckDB UPSERT vs PyArrow Merge-Aware Writes")
    print("=" * 50)

    print("\n📋 Feature Comparison:")
    print("""
┌─────────────────────┬─────────────────────┬─────────────────────────┐
│ Feature             │ DuckDB UPSERT       │ PyArrow Merge-Aware     │
├─────────────────────┼─────────────────────┼─────────────────────────┤
│ SQL Syntax          │ INSERT ... ON       │ strategy='upsert'       │
│                     │ CONFLICT DO UPDATE  │                         │
├─────────────────────┼─────────────────────┼─────────────────────────┤
│ Complex Logic       │ ✅ Full SQL power   │ Limited to basic ops    │
├─────────────────────┼─────────────────────┼─────────────────────────┤
│ Large Datasets      │ ✅ Excellent        │ ✅ Good                 │
├─────────────────────┼─────────────────────┼─────────────────────────┤
│ In-Memory Data      │ ✅ Native           │ ✅ Native               │
├─────────────────────┼─────────────────────┼─────────────────────────┤
│ Parquet Integration │ ✅ Direct read/     │ ✅ Native parquet       │
│                     │ write               │ support                 │
├─────────────────────┼─────────────────────┼─────────────────────────┤
│ Memory Efficiency   │ ✅ Configurable     │ Streaming available     │
├─────────────────────┼─────────────────────┼─────────────────────────┤
│ Setup Required      │ DuckDB connection   │ fsspec filesystem       │
└─────────────────────┴─────────────────────┴─────────────────────────┘
    """)

    print("\n💡 When to Use Each:")
    print("   DuckDB UPSERT:")
    print("   • Complex conflict resolution logic")
    print("   • Multiple data sources (join before upsert)")
    print("   • Aggregating values during upsert")
    print("   • Already using DuckDB for other operations")

    print("\n   PyArrow Merge-Aware:")
    print("   • Simple insert/update scenarios")
    print("   • Integrating with fsspec ecosystem")
    print("   • When you want a simpler API")
    print("   • Streaming data processing")


def main():
    """Run all DuckDB UPSERT examples."""
    print("🚀 DuckDB UPSERT Operations - Getting Started")
    print("=" * 60)
    print("This example demonstrates UPSERT (insert or update) operations")
    print("using DuckDB with PyArrow tables.")

    try:
        demonstrate_basic_upsert()
        demonstrate_upsert_with_parquet()
        demonstrate_batched_upsert()
        demonstrate_partial_upsert()
        demonstrate_conflict_resolution()
        demonstrate_upsert_vs_pyarrow_merge()

        print("\n" + "=" * 60)
        print("✅ DuckDB UPSERT examples completed successfully!")

        print("\n🎯 Key Takeaways:")
        print("• Use INSERT ... ON CONFLICT DO UPDATE for UPSERT operations")
        print("• Specify conflict columns (usually primary key) in ON CONFLICT")
        print("• Use EXCLUDED table reference to access proposed values")
        print("• Batch large operations for better performance")
        print("• DuckDB offers more flexibility than PyArrow merge-aware writes")

        print("\n🔗 Related Examples:")
        print("• 03_simple_merges.py: Basic merge operations")
        print("• 04_pyarrow_merges.py: PyArrow merge-aware writes")
        print("• DuckDB basics: 01_duckdb_basics.py")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()
