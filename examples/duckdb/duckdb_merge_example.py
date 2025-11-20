"""
DuckDB Parquet Dataset Merge Examples

This script demonstrates all merge strategies and common use cases for the
DuckDBParquetHandler.merge_parquet_dataset() method.

Examples covered:
1. UPSERT - Change Data Capture (CDC) workflow
2. INSERT - Incremental data loads
3. UPDATE - Dimension table updates
4. FULL_MERGE - Full synchronization
5. DEDUPLICATE - Deduplication before merge
6. Composite keys
7. Error handling and validation
"""

import tempfile
from pathlib import Path

import pyarrow as pa

from fsspeckit.datasets import DuckDBParquetHandler


def example_1_upsert_cdc_workflow():
    """
    Example 1: UPSERT Strategy - Change Data Capture (CDC) Workflow
    
    Use case: Synchronize a target dataset with incoming changes that may
    contain both new records and updates to existing records.
    """
    print("\n" + "=" * 70)
    print("Example 1: UPSERT Strategy - Change Data Capture (CDC)")
    print("=" * 70)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "customers"
        
        # Initial customer data
        initial_data = pa.table({
            "customer_id": [1, 2, 3, 4],
            "name": ["Alice Johnson", "Bob Smith", "Charlie Brown", "Diana Ross"],
            "email": ["alice@email.com", "bob@email.com", "charlie@email.com", "diana@email.com"],
            "total_purchases": [1500, 2300, 890, 4200],
            "last_purchase_date": ["2024-01-15", "2024-02-20", "2024-01-30", "2024-03-10"]
        })
        
        # CDC changes: Update customers 2 and 3, add new customer 5
        cdc_changes = pa.table({
            "customer_id": [2, 3, 5],
            "name": ["Bob Smith", "Charlie Brown", "Eve White"],
            "email": ["bob.smith@newemail.com", "charlie@email.com", "eve@email.com"],
            "total_purchases": [2800, 1200, 500],  # Updated values
            "last_purchase_date": ["2024-03-25", "2024-03-22", "2024-03-20"]
        })
        
        with DuckDBParquetHandler() as handler:
            # Write initial data
            handler.write_parquet_dataset(initial_data, str(dataset_path))
            print(f"\n✓ Initial dataset written: {initial_data.num_rows} customers")
            
            # Apply CDC changes using UPSERT
            stats = handler.merge_parquet_dataset(
                source=cdc_changes,
                target_path=str(dataset_path),
                key_columns="customer_id",
                strategy="upsert"
            )
            
            print(f"\n✓ CDC Merge Statistics:")
            print(f"  - Inserted: {stats['inserted']} (new customer)")
            print(f"  - Updated: {stats['updated']} (existing customers)")
            print(f"  - Total customers: {stats['total']}")
            
            # Verify the result
            result = handler.read_parquet(str(dataset_path))
            print(f"\n✓ Final dataset: {result.num_rows} customers")
            
            # Show updated customer
            result_dict = result.to_pydict()
            bob_idx = result_dict["customer_id"].index(2)
            print(f"\n✓ Customer 2 (Bob) updated:")
            print(f"  - Email: {result_dict['email'][bob_idx]}")
            print(f"  - Purchases: ${result_dict['total_purchases'][bob_idx]}")


def example_2_insert_incremental_loads():
    """
    Example 2: INSERT Strategy - Incremental Data Loads
    
    Use case: Append only new records without modifying existing ones.
    Useful for event logs, audit trails, or append-only systems.
    """
    print("\n" + "=" * 70)
    print("Example 2: INSERT Strategy - Incremental Data Loads")
    print("=" * 70)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "events"
        
        # Existing event log
        existing_events = pa.table({
            "event_id": [1001, 1002, 1003],
            "event_type": ["login", "purchase", "logout"],
            "user_id": [101, 102, 101],
            "timestamp": ["2024-03-01 10:00:00", "2024-03-01 10:15:00", "2024-03-01 10:30:00"],
            "metadata": ["success", "amount:50", "success"]
        })
        
        # New events (some event_ids might overlap - we only want new ones)
        new_events = pa.table({
            "event_id": [1003, 1004, 1005],  # 1003 already exists
            "event_type": ["view", "purchase", "login"],
            "user_id": [103, 102, 104],
            "timestamp": ["2024-03-01 10:45:00", "2024-03-01 11:00:00", "2024-03-01 11:15:00"],
            "metadata": ["page:home", "amount:120", "success"]
        })
        
        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(existing_events, str(dataset_path))
            print(f"\n✓ Existing events: {existing_events.num_rows}")
            
            # Insert only new events
            stats = handler.merge_parquet_dataset(
                source=new_events,
                target_path=str(dataset_path),
                key_columns="event_id",
                strategy="insert"
            )
            
            print(f"\n✓ Insert Statistics:")
            print(f"  - Inserted: {stats['inserted']} (new events only)")
            print(f"  - Skipped: {new_events.num_rows - stats['inserted']} (duplicates)")
            print(f"  - Total events: {stats['total']}")
            
            # Verify event 1003 was not modified
            result = handler.read_parquet(str(dataset_path))
            result_dict = result.to_pydict()
            event_1003_idx = result_dict["event_id"].index(1003)
            print(f"\n✓ Event 1003 unchanged:")
            print(f"  - Type: {result_dict['event_type'][event_1003_idx]}")
            print(f"  - Original: 'logout', Not updated to 'view'")


def example_3_update_dimension_tables():
    """
    Example 3: UPDATE Strategy - Dimension Table Updates
    
    Use case: Update existing records only without inserting new ones.
    Common in data warehousing for slowly changing dimensions (SCD Type 1).
    """
    print("\n" + "=" * 70)
    print("Example 3: UPDATE Strategy - Dimension Table Updates")
    print("=" * 70)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "products"
        
        # Product dimension table
        products = pa.table({
            "product_id": [101, 102, 103],
            "name": ["Laptop", "Mouse", "Keyboard"],
            "category": ["Electronics", "Accessories", "Accessories"],
            "price": [999.99, 29.99, 79.99],
            "status": ["active", "active", "active"]
        })
        
        # Updates (includes product 104 which doesn't exist)
        updates = pa.table({
            "product_id": [102, 103, 104],  # 104 doesn't exist
            "name": ["Wireless Mouse", "Mechanical Keyboard", "Monitor"],
            "category": ["Accessories", "Accessories", "Electronics"],
            "price": [39.99, 129.99, 299.99],
            "status": ["active", "active", "active"]
        })
        
        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(products, str(dataset_path))
            print(f"\n✓ Product catalog: {products.num_rows} products")
            
            # Update existing products only
            stats = handler.merge_parquet_dataset(
                source=updates,
                target_path=str(dataset_path),
                key_columns="product_id",
                strategy="update"
            )
            
            print(f"\n✓ Update Statistics:")
            print(f"  - Updated: {stats['updated']} (existing products)")
            print(f"  - Ignored: {updates.num_rows - stats['updated']} (non-existent product)")
            print(f"  - Total products: {stats['total']} (no new products added)")
            
            # Verify updates
            result = handler.read_parquet(str(dataset_path))
            result_dict = result.to_pydict()
            mouse_idx = result_dict["product_id"].index(102)
            print(f"\n✓ Product 102 updated:")
            print(f"  - Name: {result_dict['name'][mouse_idx]}")
            print(f"  - Price: ${result_dict['price'][mouse_idx]}")
            
            # Verify product 104 was NOT added
            if 104 not in result_dict["product_id"]:
                print(f"\n✓ Product 104 (Monitor) was not added (UPDATE only mode)")


def example_4_full_merge_synchronization():
    """
    Example 4: FULL_MERGE Strategy - Complete Synchronization
    
    Use case: Make target dataset identical to source by adding, updating,
    and deleting records. Used for complete syncs or snapshot replacements.
    """
    print("\n" + "=" * 70)
    print("Example 4: FULL_MERGE Strategy - Full Synchronization")
    print("=" * 70)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "inventory"
        
        # Current inventory
        current_inventory = pa.table({
            "sku": ["SKU001", "SKU002", "SKU003", "SKU004"],
            "product": ["Widget A", "Widget B", "Widget C", "Widget D"],
            "quantity": [100, 50, 75, 25],
            "warehouse": ["WH1", "WH1", "WH2", "WH2"]
        })
        
        # Fresh inventory snapshot (SKU002 and SKU004 removed, SKU005 added, others updated)
        fresh_snapshot = pa.table({
            "sku": ["SKU001", "SKU003", "SKU005"],
            "product": ["Widget A", "Widget C", "Widget E"],
            "quantity": [120, 60, 200],  # Updated quantities
            "warehouse": ["WH1", "WH2", "WH3"]
        })
        
        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(current_inventory, str(dataset_path))
            print(f"\n✓ Current inventory: {current_inventory.num_rows} SKUs")
            
            # Full synchronization
            stats = handler.merge_parquet_dataset(
                source=fresh_snapshot,
                target_path=str(dataset_path),
                key_columns="sku",
                strategy="full_merge"
            )
            
            print(f"\n✓ Full Merge Statistics:")
            print(f"  - Inserted: {stats['inserted']} (new SKUs)")
            print(f"  - Deleted: {stats['deleted']} (removed SKUs)")
            print(f"  - Total SKUs after sync: {stats['total']}")
            
            # Verify result matches source exactly
            result = handler.read_parquet(str(dataset_path))
            result_dict = result.to_pydict()
            print(f"\n✓ Target synchronized with source:")
            print(f"  - SKUs present: {sorted(result_dict['sku'])}")
            print(f"  - SKU002 and SKU004 deleted")
            print(f"  - SKU005 added")


def example_5_deduplicate_strategy():
    """
    Example 5: DEDUPLICATE Strategy - Removing Duplicates
    
    Use case: Merge datasets while removing duplicate records based on key
    columns, keeping the most recent or relevant version.
    """
    print("\n" + "=" * 70)
    print("Example 5: DEDUPLICATE Strategy - Duplicate Removal")
    print("=" * 70)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "transactions"
        
        # Existing transactions
        existing_txns = pa.table({
            "transaction_id": [1, 2, 3],
            "amount": [100.0, 200.0, 150.0],
            "timestamp": [1000, 2000, 3000],
            "status": ["completed", "completed", "completed"]
        })
        
        # New transactions with duplicates (transaction_id 2 appears twice with different timestamps)
        new_txns = pa.table({
            "transaction_id": [2, 2, 4, 4],  # Duplicates!
            "amount": [200.0, 200.0, 300.0, 300.0],
            "timestamp": [2500, 3500, 4000, 4500],  # Different timestamps
            "status": ["pending", "completed", "pending", "completed"]
        })
        
        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(existing_txns, str(dataset_path))
            print(f"\n✓ Existing transactions: {existing_txns.num_rows}")
            print(f"✓ New transactions (with duplicates): {new_txns.num_rows}")
            
            # Deduplicate: Keep records with highest timestamp
            stats = handler.merge_parquet_dataset(
                source=new_txns,
                target_path=str(dataset_path),
                key_columns="transaction_id",
                strategy="deduplicate",
                dedup_order_by=["timestamp"]  # Keep highest timestamp
            )
            
            print(f"\n✓ Deduplicate Statistics:")
            print(f"  - Total unique transactions: {stats['total']}")
            print(f"  - Duplicates removed: {existing_txns.num_rows + new_txns.num_rows - stats['total']}")
            
            # Verify deduplication
            result = handler.read_parquet(str(dataset_path))
            result_dict = result.to_pydict()
            
            # Check transaction 2 (should have highest timestamp)
            txn2_indices = [i for i, tid in enumerate(result_dict["transaction_id"]) if tid == 2]
            print(f"\n✓ Transaction 2 deduplicated:")
            print(f"  - Count: {len(txn2_indices)} (was 2 in new data)")
            if len(txn2_indices) == 1:
                idx = txn2_indices[0]
                print(f"  - Kept timestamp: {result_dict['timestamp'][idx]}")
                print(f"  - Status: {result_dict['status'][idx]}")
            
            # Check transaction 4
            txn4_indices = [i for i, tid in enumerate(result_dict["transaction_id"]) if tid == 4]
            print(f"\n✓ Transaction 4 deduplicated:")
            print(f"  - Count: {len(txn4_indices)} (was 2 in new data)")


def example_6_composite_keys():
    """
    Example 6: Composite Keys - Multi-Column Keys
    
    Use case: Merge on multiple columns when uniqueness is defined by
    a combination of fields (e.g., user + date, product + location).
    """
    print("\n" + "=" * 70)
    print("Example 6: Composite Keys - Multi-Column Merge")
    print("=" * 70)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "daily_metrics"
        
        # Daily user metrics
        existing_metrics = pa.table({
            "user_id": [101, 101, 102, 102],
            "date": ["2024-03-01", "2024-03-02", "2024-03-01", "2024-03-02"],
            "page_views": [10, 15, 8, 12],
            "session_duration": [300, 450, 200, 350]
        })
        
        # New/updated metrics (update 101/2024-03-02, add 101/2024-03-03)
        new_metrics = pa.table({
            "user_id": [101, 101, 103],
            "date": ["2024-03-02", "2024-03-03", "2024-03-01"],
            "page_views": [20, 18, 5],  # Updated for existing combo
            "session_duration": [600, 500, 150]
        })
        
        with DuckDBParquetHandler() as handler:
            handler.write_parquet_dataset(existing_metrics, str(dataset_path))
            print(f"\n✓ Existing metrics: {existing_metrics.num_rows} records")
            
            # Merge on composite key: (user_id, date)
            stats = handler.merge_parquet_dataset(
                source=new_metrics,
                target_path=str(dataset_path),
                key_columns=["user_id", "date"],
                strategy="upsert"
            )
            
            print(f"\n✓ Composite Key Merge Statistics:")
            print(f"  - Key columns: ['user_id', 'date']")
            print(f"  - Inserted: {stats['inserted']} (new combinations)")
            print(f"  - Updated: {stats['updated']} (existing combinations)")
            print(f"  - Total records: {stats['total']}")
            
            # Verify the update
            result = handler.read_parquet(str(dataset_path))
            result_dict = result.to_pydict()
            
            # Find user 101 on 2024-03-02
            for i in range(len(result_dict["user_id"])):
                if result_dict["user_id"][i] == 101 and result_dict["date"][i] == "2024-03-02":
                    print(f"\n✓ User 101 on 2024-03-02 updated:")
                    print(f"  - Page views: {result_dict['page_views'][i]}")
                    print(f"  - Session duration: {result_dict['session_duration'][i]}s")
                    break


def example_7_error_handling():
    """
    Example 7: Error Handling and Validation
    
    Demonstrates various validation errors and how to handle them properly.
    """
    print("\n" + "=" * 70)
    print("Example 7: Error Handling and Validation")
    print("=" * 70)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "test"
        
        with DuckDBParquetHandler() as handler:
            # Error 1: Invalid strategy
            print("\n1. Testing invalid strategy...")
            try:
                handler.merge_parquet_dataset(
                    source=pa.table({"id": [1]}),
                    target_path=str(dataset_path),
                    key_columns="id",
                    strategy="invalid_strategy"  # type: ignore
                )
            except ValueError as e:
                print(f"   ✓ Caught error: {str(e)[:60]}...")
            
            # Error 2: Missing key column
            print("\n2. Testing missing key column...")
            try:
                handler.merge_parquet_dataset(
                    source=pa.table({"id": [1], "name": ["Alice"]}),
                    target_path=str(dataset_path),
                    key_columns="missing_column",
                    strategy="upsert"
                )
            except ValueError as e:
                print(f"   ✓ Caught error: {str(e)[:60]}...")
            
            # Error 3: NULL values in key column
            print("\n3. Testing NULL in key column...")
            try:
                handler.merge_parquet_dataset(
                    source=pa.table({
                        "id": [1, None, 3],
                        "name": ["Alice", "Bob", "Charlie"]
                    }),
                    target_path=str(dataset_path),
                    key_columns="id",
                    strategy="upsert"
                )
            except ValueError as e:
                print(f"   ✓ Caught error: {str(e)[:60]}...")
            
            # Error 4: Schema mismatch
            print("\n4. Testing schema mismatch...")
            try:
                # Create target with specific schema
                target_data = pa.table({
                    "id": [1, 2],
                    "value": [100, 200]
                })
                handler.write_parquet_dataset(target_data, str(dataset_path))
                
                # Try to merge with different schema
                source_data = pa.table({
                    "id": [3],
                    "amount": [300]  # Different column name
                })
                handler.merge_parquet_dataset(
                    source=source_data,
                    target_path=str(dataset_path),
                    key_columns="id",
                    strategy="upsert"
                )
            except ValueError as e:
                print(f"   ✓ Caught error: {str(e)[:60]}...")
            
            print("\n✓ All validation errors handled correctly!")


def example_8_compression_options():
    """
    Example 8: Using Compression Options
    
    Demonstrates how to use compression when writing merged datasets.
    """
    print("\n" + "=" * 70)
    print("Example 8: Compression Options")
    print("=" * 70)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "compressed_data"
        
        # Large dataset
        large_data = pa.table({
            "id": list(range(1, 10001)),
            "value": [f"value_{i}" for i in range(1, 10001)],
            "amount": [i * 1.5 for i in range(1, 10001)]
        })
        
        # Incremental data
        incremental_data = pa.table({
            "id": list(range(9900, 10101)),  # 100 updates, 100 new
            "value": [f"updated_value_{i}" for i in range(9900, 10101)],
            "amount": [i * 2.0 for i in range(9900, 10101)]
        })
        
        with DuckDBParquetHandler() as handler:
            # Write with snappy compression (default)
            handler.write_parquet_dataset(large_data, str(dataset_path), compression="snappy")
            print(f"\n✓ Initial dataset: {large_data.num_rows} records (snappy compression)")
            
            # Merge with zstd compression for better compression ratio
            stats = handler.merge_parquet_dataset(
                source=incremental_data,
                target_path=str(dataset_path),
                key_columns="id",
                strategy="upsert",
                compression="zstd"
            )
            
            print(f"\n✓ Merge with ZSTD compression:")
            print(f"  - Inserted: {stats['inserted']}")
            print(f"  - Updated: {stats['updated']}")
            print(f"  - Total: {stats['total']}")
            
            # Check file sizes
            import os
            total_size = sum(
                f.stat().st_size 
                for f in Path(dataset_path).glob("*.parquet")
            )
            print(f"\n✓ Dataset size: {total_size:,} bytes")


def main():
    """Run all examples."""
    print("\n" + "=" * 70)
    print("DuckDB Parquet Dataset Merge Examples")
    print("=" * 70)
    print("\nThis script demonstrates all merge strategies and common use cases.")
    print("Each example is self-contained and uses temporary directories.")
    
    # Run all examples
    example_1_upsert_cdc_workflow()
    example_2_insert_incremental_loads()
    example_3_update_dimension_tables()
    example_4_full_merge_synchronization()
    example_5_deduplicate_strategy()
    example_6_composite_keys()
    example_7_error_handling()
    example_8_compression_options()
    
    print("\n" + "=" * 70)
    print("All examples completed successfully!")
    print("=" * 70)
    print("\nKey Takeaways:")
    print("  • UPSERT: Best for CDC and general updates")
    print("  • INSERT: Use for append-only scenarios")
    print("  • UPDATE: Update existing records without adding new ones")
    print("  • FULL_MERGE: Complete synchronization with deletes")
    print("  • DEDUPLICATE: Remove duplicates based on key + order")
    print("  • Composite keys: Handle multi-column uniqueness")
    print("  • Validation: Comprehensive error checking built-in")
    print("  • Compression: Support for snappy, gzip, zstd, and more")
    print()


if __name__ == "__main__":
    main()
