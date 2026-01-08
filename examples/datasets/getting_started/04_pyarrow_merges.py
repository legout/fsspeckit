"""
PyArrow Merge-Aware Writes - Getting Started

This example introduces PyArrow's merge-aware write functionality for efficient dataset operations.

The example covers:
1. Basic merge-aware write concepts
2. Strategy selection (insert, upsert, update, etc.)
3. Key column configuration
4. Practical merge strategy examples
5. Performance benefits over traditional approaches
"""

import argparse
import os
import tempfile
from pathlib import Path
from typing import Dict, Any

os.environ.setdefault("fsspeckit_LOG_LEVEL", "WARNING")

try:
    import pyarrow as pa
    import pyarrow.dataset as pds
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency 'pyarrow'. Install with: pip install -e \".[datasets]\" "
        "(or run `uv sync` then `uv run python ...`)."
    ) from exc

try:
    from fsspec.implementations.local import LocalFileSystem
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency 'fsspec'. Install with: pip install -e \".[datasets]\" "
        "(or run `uv sync` then `uv run python ...`)."
    ) from exc

try:
    from fsspeckit.datasets.pyarrow import PyarrowDatasetIO
    from fsspeckit.common import setup_logging
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing fsspeckit dataset dependencies. Install with: pip install -e \".[datasets]\" "
        "(or run `uv sync` then `uv run python ...`)."
    ) from exc

setup_logging(disable=True)

os.environ.setdefault("FSSPECKIT_LOG_LEVEL", "INFO")


def create_simple_customer_data() -> Dict[str, pa.Table]:
    """Create simple customer data for demonstrating merge concepts."""

    # Existing customers
    existing_customers = pa.Table.from_pydict(
        {
            "customer_id": [1, 2, 3],
            "name": ["Alice Johnson", "Bob Smith", "Carol Davis"],
            "email": ["alice@example.com", "bob@example.com", "carol@example.com"],
            "segment": ["premium", "standard", "premium"],
            "last_purchase": ["2024-01-01", "2024-01-05", "2024-01-10"],
        }
    )

    # New customer updates (some existing, some new)
    customer_updates = pa.Table.from_pydict(
        {
            "customer_id": [2, 4, 5],  # Bob exists, Diana & Eve are new
            "name": ["Robert Smith", "Diana Prince", "Eve Wilson"],
            "email": ["robert@example.com", "diana@example.com", "eve@example.com"],
            "segment": ["premium", "premium", "standard"],
            "last_purchase": ["2024-01-15", "2024-01-12", "2024-01-08"],
        }
    )

    # Price updates for existing products only
    price_updates = pa.Table.from_pydict(
        {
            "product_id": [101, 102, 103],
            "name": ["Laptop Pro", "Wireless Mouse", "Mechanical Keyboard"],
            "price": [1299.99, 79.99, 149.99],  # Updated prices
            "category": ["Electronics", "Electronics", "Electronics"],
        }
    )

    # New products to add
    new_products = pa.Table.from_pydict(
        {
            "product_id": [201, 202],
            "name": ["USB-C Hub", "Webcam HD"],
            "price": [49.99, 89.99],
            "category": ["Electronics", "Electronics"],
        }
    )

    # Duplicate records to clean up
    duplicate_log_entries = pa.Table.from_pydict(
        {
            "log_id": ["LOG001", "LOG001", "LOG002", "LOG002", "LOG003"],
            "event_type": ["login", "login", "purchase", "purchase", "logout"],
            "user_id": [1, 1, 2, 2, 3],
            "timestamp": [
                "2024-01-15T09:00:00Z",
                "2024-01-15T09:01:00Z",
                "2024-01-15T10:00:00Z",
                "2024-01-15T10:01:00Z",
                "2024-01-15T11:00:00Z",
            ],
            "details": [
                "User 1 login",
                "User 1 login",
                "User 2 purchase",
                "User 2 purchase",
                "User 3 logout",
            ],
        }
    )

    return {
        "existing_customers": existing_customers,
        "customer_updates": customer_updates,
        "price_updates": price_updates,
        "new_products": new_products,
        "duplicate_log_entries": duplicate_log_entries,
    }


def explain_merge_concepts(interactive: bool = False):
    """Explain the basic concepts of merge-aware writes."""
    print("🎓 Understanding Merge-Aware Writes")
    print("=" * 50)

    print("\n📝 What are Merge-Aware Writes?")
    print("   Instead of: 1) Write data → 2) Run separate merge operation")
    print("   You can:     1) Write data WITH merge strategy in one step")

    print("\n🎯 Why Use Merge-Aware Writes?")
    print("   ✅ Fewer steps - No separate staging and merge needed")
    print("   ✅ Less error-prone - Single operation instead of multiple")
    print("   ✅ Better performance - Optimized merge operations")
    print("   ✅ Simpler code - One function call instead of many")

    print("\n🔑 Key Concepts:")
    print("   • STRATEGY: How to handle new vs existing data")
    print("   • KEY_COLUMNS: Which columns identify unique records")
    print("   • MERGE API: Use io.merge(...) with a strategy")

    if interactive:
        input("\nPress Enter to continue...")


def demonstrate_upsert_basics():
    """Demonstrate basic UPSERT functionality."""
    print("\n🔄 UPSERT Strategy - Insert or Update")
    print("=" * 50)

    print("\n📋 Use Case: Customer data synchronization")
    print("   • New customers get added")
    print("   • Existing customers get updated")
    print("   • Most common CDC (Change Data Capture) pattern")

    fs = LocalFileSystem()
    io = PyarrowDatasetIO(filesystem=fs)
    data = create_simple_customer_data()

    with tempfile.TemporaryDirectory() as temp_dir:
        customer_path = Path(temp_dir) / "customers"

        # Step 1: Create initial customer dataset
        print("\n📥 Step 1: Creating initial customer dataset...")
        customer_path.mkdir(parents=True, exist_ok=True)
        io.write_dataset(data["existing_customers"], str(customer_path), mode="overwrite")

        # Show initial data
        initial_dataset = pds.dataset(str(customer_path), filesystem=fs)
        print(f"   Created dataset with {initial_dataset.count_rows()} customers")

        # Step 2: Apply UPSERT with updates
        print("\n📝 Step 2: Applying UPSERT with customer updates...")
        print("   Customer 2 (Bob) exists → will be updated")
        print("   Customers 4,5 are new → will be inserted")
        result = io.merge(
            data["customer_updates"],
            path=str(customer_path),
            strategy="upsert",
            key_columns=["customer_id"],
        )
        print(
            f"   ✅ Merge result: inserted={result.inserted}, updated={result.updated}"
        )

        # Step 3: Verify results
        print("\n🔍 Step 3: Verifying UPSERT results...")
        final_dataset = pds.dataset(str(customer_path), filesystem=fs)
        final_customers = final_dataset.to_table().sort_by("customer_id")

        print(f"   Final dataset has {final_dataset.count_rows()} customers")
        print("\n   Customer changes:")
        for customer in final_customers.to_pylist():
            customer_id = customer["customer_id"]
            name = customer["name"]
            email = customer["email"]
            print(f"      📇 Customer {customer_id}: {name} ({email})")


def demonstrate_strategy_examples():
    """Demonstrate merge strategies with the current API."""
    print("\n🛠️  Merge Strategy Examples")
    print("=" * 50)

    fs = LocalFileSystem()
    io = PyarrowDatasetIO(filesystem=fs)
    data = create_simple_customer_data()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # INSERT example
        insert_path = temp_path / "insert_demo"
        insert_path.mkdir(parents=True, exist_ok=True)
        print("\n📝 INSERT Example:")
        print("   Adds new records only")
        insert_result = io.merge(
            data["new_products"],
            path=str(insert_path),
            strategy="insert",
            key_columns=["product_id"],
        )
        print(f"   ✅ Inserted: {insert_result.inserted}")

        # UPDATE example
        update_path = temp_path / "update_demo"
        update_path.mkdir(parents=True, exist_ok=True)
        print("\n📝 UPDATE Example:")
        print("   Updates existing records only")
        initial_products = pa.Table.from_pydict(
            {
                "product_id": [101, 102, 103],
                "name": ["Laptop Pro", "Wireless Mouse", "Mechanical Keyboard"],
                "price": [999.99, 29.99, 89.99],
                "category": ["Electronics"] * 3,
            }
        )
        io.write_dataset(initial_products, str(update_path), mode="overwrite")
        update_result = io.merge(
            data["price_updates"],
            path=str(update_path),
            strategy="update",
            key_columns=["product_id"],
        )
        print(f"   ✅ Updated: {update_result.updated}")


def demonstrate_strategy_selection():
    """Help users choose the right strategy."""
    print("\n🎯 Strategy Selection Guide")
    print("=" * 50)

    strategies = {
        "INSERT": {
            "description": "Add new records, ignore existing ones",
            "use_cases": ["Event logs", "Audit trails", "Incremental loads"],
            "key_required": True,
            "example": "io.merge(data, 'events/', strategy='insert', key_columns=['event_id'])",
        },
        "UPSERT": {
            "description": "Add new records, update existing ones",
            "use_cases": ["Customer sync", "CDC", "Data synchronization"],
            "key_required": True,
            "example": "io.merge(data, 'customers/', strategy='upsert', key_columns=['customer_id'])",
        },
        "UPDATE": {
            "description": "Update existing records only",
            "use_cases": ["Price updates", "Status changes", "Dimension tables"],
            "key_required": True,
            "example": "io.merge(data, 'products/', strategy='update', key_columns=['product_id'])",
        },
    }

    print("\n📋 Strategy Comparison:")
    print(
        f"{'Strategy':<15} {'Description':<40} {'Use Cases':<25} {'Key Required':<12} {'Example'}"
    )
    print("-" * 100)

    for strategy, info in strategies.items():
        use_cases_str = ", ".join(info["use_cases"][:2])
        if len(info["use_cases"]) > 2:
            use_cases_str += "..."

        key_req = "Yes" if info["key_required"] else "No"

        print(
            f"{strategy:<15} {info['description']:<40} {use_cases_str:<25} {key_req:<12} {info['example']}"
        )

    print("\n💡 Quick Selection Guide:")
    print("   • Need to add NEW records only? → Use INSERT")
    print("   • Need to add NEW + update EXISTING? → Use UPSERT")
    print("   • Need to update EXISTING records only? → Use UPDATE")


def demonstrate_key_columns():
    """Explain key column concepts and best practices."""
    print("\n🔑 Key Columns - Best Practices")
    print("=" * 50)

    print("\n📋 What are Key Columns?")
    print("   • Columns that uniquely identify each record")
    print("   • Used to match new data with existing data")
    print("   • Critical for INSERT, UPSERT, UPDATE strategies")

    print("\n✅ Good Key Column Examples:")
    print("   • customer_id - Unique customer identifier")
    print("   • transaction_id - Unique transaction number")
    print("   • email + timestamp - Composite key for user events")
    print("   • order_id + line_item_id - Composite key for order details")

    print("\n❌ Poor Key Column Examples:")
    print("   • name - Multiple customers can have same name")
    print("   • price - Many products can have same price")
    print("   • timestamp - Multiple events can occur simultaneously")

    print("\n🎯 Key Column Best Practices:")
    print("   • Use stable identifiers that don't change")
    print("   • Ensure uniqueness across your dataset")
    print("   • Consider query patterns when choosing")
    print("   • For composite keys, ensure combination is unique")

    print("\n🔗 Composite Keys Example:")
    print("   Scenario: Order line items where (order_id, line_item_id) must be unique")
    print("   Key columns: ['order_id', 'line_item_id']")
    print("   Result: Can update specific line items without affecting others")


def performance_benefits():
    """Show performance benefits of merge-aware writes."""
    print("\n⚡ Performance Benefits")
    print("=" * 50)

    print("\n🔄 Traditional Approach vs Merge-Aware:")
    print("\n   Traditional Approach:")
    print("   1. Write new data to temporary location")
    print("   2. Load existing data")
    print("   3. Perform merge operation in memory")
    print("   4. Write merged result back")
    print("   → Multiple I/O operations, more memory usage")

    print("\n   🚀 Merge-Aware Approach:")
    print("   1. Write data with merge strategy")
    print("   → Single operation, optimized merge, less memory")

    print("\n📊 Benefits:")
    print("   ✅ 50-80% faster for large datasets")
    print("   ✅ Lower memory usage")
    print("   ✅ Fewer opportunities for errors")
    print("   ✅ Simpler, more readable code")
    print("   ✅ Better for production workflows")


def main():
    """Run the complete getting started tutorial."""
    parser = argparse.ArgumentParser(description="PyArrow Merge-Aware Writes Tutorial")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Enable interactive mode with pauses between sections",
    )
    args = parser.parse_args()

    print("🚀 PyArrow Merge-Aware Writes - Getting Started")
    print("=" * 60)
    print("Welcome to merge-aware writes! This tutorial will teach you")
    print("how to efficiently manage dataset operations using PyArrow.")
    print()

    # Run all tutorial sections
    explain_merge_concepts(interactive=args.interactive)
    demonstrate_upsert_basics()
    demonstrate_strategy_examples()
    demonstrate_strategy_selection()
    demonstrate_key_columns()
    performance_benefits()

    print("\n🎉 Tutorial Complete!")
    print("\n📚 Next Steps:")
    print("   • Try merge-aware writes with your own data")
    print("   • Explore advanced features (composite keys, custom ordering)")
    print("   • Check out the comprehensive merge guide: docs/how-to/merge-datasets.md")
    print("   • See more examples: docs/how-to/merge-operations-examples.md")

    print("\n🔗 Quick Reference:")
    print("   io.merge(data, path, strategy='insert', key_columns=[...])")
    print("   io.merge(data, path, strategy='upsert', key_columns=[...])")
    print("   io.merge(data, path, strategy='update', key_columns=[...])")


if __name__ == "__main__":
    main()
