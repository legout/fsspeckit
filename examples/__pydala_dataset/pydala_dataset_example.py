"""
Example: Working with Delta Tables using fsspeckit

This example demonstrates how to work with Delta tables using fsspeckit and StorageOptions.

The example shows:
1. Using StorageOptions with DeltaTable from delta-rs
2. Creating and writing data to Delta tables
3. Reading data back from Delta tables
4. Performing update and append operations on Delta tables
"""

import tempfile
import shutil
from pathlib import Path

# Import required libraries
import polars as pl
from fsspeckit import filesystem
from fsspeckit.storage_options import LocalStorageOptions


def main():
    """Demonstrate using StorageOptions with DeltaTable using delta-rs and fsspeckit."""

    # Create a temporary directory for our DeltaTable
    temp_dir = tempfile.mkdtemp()
    print(f"Created temporary directory: {temp_dir}")

    try:
        # 1. Create LocalStorageOptions for the temporary directory
        local_options = LocalStorageOptions(auto_mkdir=True)
        print(f"Created LocalStorageOptions with auto_mkdir={local_options.auto_mkdir}")

        # 2. Obtain an fsspec filesystem instance from LocalStorageOptions
        fs = filesystem("file", storage_options=local_options)
        print(f"Created fsspec filesystem: {type(fs).__name__}")

        # 3. Use fs.pydala_dataset() to create a Pydala dataset instance
        ds = fs.pydala_dataset(temp_dir)
        print(f"Created Pydala dataset at: {temp_dir}")

        # 4. Create some dummy data using Polars DataFrame
        original_data = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "age": [25, 30, 35, 40, 45],
                "city": ["New York", "London", "Paris", "Tokyo", "Sydney"],
            }
        )
        print("Created original dummy data:")
        print(original_data)

        # 5. Write the dummy data to the DeltaTable using ds.write_to_dataset() with mode="delta"
        print("\nWriting data to DeltaTable...")
        ds.write_to_dataset(
            data=original_data,
            mode="delta",
            delta_subset=["id"],  # Use 'id' column for delta operations
        )
        print("Data written successfully to DeltaTable")

        # 6. Verify the DeltaTable write by reading the data back using ds.to_polars()
        print("\nReading data back from DeltaTable:")
        read_data = ds.t.to_polars()
        print(read_data)

        # Verify the data matches
        if read_data.equals(original_data):
            print("✓ Data verification successful: Read data matches original data")
        else:
            print("✗ Data verification failed: Read data does not match original data")

        # 7. Demonstrate a simple update operation to the DeltaTable
        print("\nDemonstrating update operation...")

        # Create updated data (modify some records)
        updated_data = pl.DataFrame(
            {
                "id": [1, 3, 5],  # Update records with IDs 1, 3, and 5
                "name": ["Alice Updated", "Charlie Updated", "Eve Updated"],
                "age": [26, 36, 46],  # Increment ages by 1
                "city": ["Boston", "Lyon", "Melbourne"],  # Change cities
            }
        )
        print("Updated data:")
        print(updated_data)

        # Write the updated data with delta mode
        ds.write_to_dataset(data=updated_data, mode="delta", delta_subset=["id"])
        print("Update operation completed")

        # Read the data back to verify updates
        print("\nReading data after update:")
        updated_read_data = ds.to_polars()
        print(updated_read_data)

        # 8. Demonstrate an append operation to the DeltaTable
        print("\nDemonstrating append operation...")

        # Create new data to append
        new_data = pl.DataFrame(
            {
                "id": [6, 7],
                "name": ["Frank", "Grace"],
                "age": [50, 55],
                "city": ["Berlin", "Toronto"],
            }
        )
        print("New data to append:")
        print(new_data)

        # Append the new data
        ds.write_to_dataset(data=new_data, mode="append")
        print("Append operation completed")

        # Read the final data
        print("\nReading final data after append:")
        final_data = ds.to_polars()
        print(final_data)

        # Verify the final record count
        expected_count = 7  # 5 original + 2 appended
        actual_count = len(final_data)
        if actual_count == expected_count:
            print(
                f"✓ Final record count verification successful: {actual_count} records"
            )
        else:
            print(
                f"✗ Final record count verification failed: Expected {expected_count}, got {actual_count}"
            )

        print("\nDeltaTable operations demonstration completed successfully!")

    except Exception as e:
        print(f"Error during demonstration: {e}")
        raise

    finally:
        # 9. Clean up the temporary directory
        print(f"\nCleaning up temporary directory: {temp_dir}")
        shutil.rmtree(temp_dir)
        print("Temporary directory removed successfully")


if __name__ == "__main__":
    main()
