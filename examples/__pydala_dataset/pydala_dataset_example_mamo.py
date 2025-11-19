import marimo

__generated_with = "0.9.27"
app = marimo.App(title="Working with Delta Tables using fsspeckit")


@app.cell
def __():
    # Working with Delta Tables using fsspeckit (marimo notebook version)
    import marimo as mo
    import tempfile
    import shutil
    from pathlib import Path
    import polars as pl
    from fsspeckit import filesystem
    from fsspeckit.storage_options import LocalStorageOptions

    return LocalStorageOptions, Path, filesystem, mo, pl, shutil, tempfile


@app.cell
def __(mo, tempfile):
    # Create a temporary directory for our DeltaTable
    temp_dir = tempfile.mkdtemp()
    mo.md(f"Created temporary directory: {temp_dir}")
    return (temp_dir,)


@app.cell
def __(LocalStorageOptions, mo):
    # Create LocalStorageOptions for the temporary directory
    local_options = LocalStorageOptions(auto_mkdir=True)
    mo.md(f"Created LocalStorageOptions with auto_mkdir={local_options.auto_mkdir}")
    return (local_options,)


@app.cell
def __(filesystem, local_options, mo):
    # Obtain an fsspec filesystem instance from LocalStorageOptions
    fs = filesystem("file", storage_options=local_options)
    mo.md(f"Created fsspec filesystem: {type(fs).__name__}")
    return (fs,)


@app.cell
def __(fs, mo, temp_dir):
    # Use fs.pydala_dataset() to create a Pydala dataset instance
    ds = fs.pydala_dataset(temp_dir)
    mo.md(f"Created Pydala dataset at: {temp_dir}")
    return (ds,)


@app.cell
def __(mo, pl):
    # Create some dummy data using Polars DataFrame
    original_data = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "city": ["New York", "London", "Paris", "Tokyo", "Sydney"],
        }
    )
    mo.md("Created original dummy data:")
    return (original_data,)


@app.cell
def __(original_data):
    original_data


@app.cell
def __(ds, mo, original_data):
    # Write the dummy data to the DeltaTable using ds.write_to_dataset() with mode="delta"
    mo.md("Writing data to DeltaTable...")
    ds.write_to_dataset(
        data=original_data,
        mode="delta",
        delta_subset=["id"],  # Use 'id' column for delta operations
    )
    mo.md("Data written successfully to DeltaTable")


@app.cell
def __(ds, mo):
    # Verify the DeltaTable write by reading the data back using ds.to_polars()
    mo.md("Reading data back from DeltaTable:")
    read_data = ds.to_polars()
    return (read_data,)


@app.cell
def __(read_data):
    read_data


@app.cell
def __(mo, original_data, read_data):
    # Verify the data matches
    if read_data.equals(original_data):
        mo.md("✓ Data verification successful: Read data matches original data")
    else:
        mo.md("✗ Data verification failed: Read data does not match original data")


@app.cell
def __(mo, pl):
    # Demonstrate a simple update operation to the DeltaTable
    mo.md("Demonstrating update operation...")

    # Create updated data (modify some records)
    updated_data = pl.DataFrame(
        {
            "id": [1, 3, 5],  # Update records with IDs 1, 3, and 5
            "name": ["Alice Updated", "Charlie Updated", "Eve Updated"],
            "age": [26, 36, 46],  # Increment ages by 1
            "city": ["Boston", "Lyon", "Melbourne"],  # Change cities
        }
    )
    mo.md("Updated data:")
    return (updated_data,)


@app.cell
def __(updated_data):
    updated_data


@app.cell
def __(ds, mo, updated_data):
    # Write the updated data with delta mode
    ds.write_to_dataset(data=updated_data, mode="delta", delta_subset=["id"])
    mo.md("Update operation completed")


@app.cell
def __(ds, mo):
    # Read the data back to verify updates
    mo.md("Reading data after update:")
    updated_read_data = ds.to_polars()
    return (updated_read_data,)


@app.cell
def __(updated_read_data):
    updated_read_data


@app.cell
def __(mo, pl):
    # Demonstrate an append operation to the DeltaTable
    mo.md("Demonstrating append operation...")

    # Create new data to append
    new_data = pl.DataFrame(
        {
            "id": [6, 7],
            "name": ["Frank", "Grace"],
            "age": [50, 55],
            "city": ["Berlin", "Toronto"],
        }
    )
    mo.md("New data to append:")
    return (new_data,)


@app.cell
def __(new_data):
    new_data


@app.cell
def __(ds, mo, new_data):
    # Append the new data
    ds.write_to_dataset(data=new_data, mode="append")
    mo.md("Append operation completed")


@app.cell
def __(ds, mo):
    # Read the final data
    mo.md("Reading final data after append:")
    final_data = ds.to_polars()
    return (final_data,)


@app.cell
def __(final_data):
    final_data


@app.cell
def __(final_data, mo):
    # Verify the final record count
    expected_count = 7  # 5 original + 2 appended
    actual_count = len(final_data)
    if actual_count == expected_count:
        mo.md(f"✓ Final record count verification successful: {actual_count} records")
    else:
        mo.md(
            f"✗ Final record count verification failed: Expected {expected_count}, got {actual_count}"
        )

    mo.md("DeltaTable operations demonstration completed successfully!")


@app.cell
def __(mo, shutil, temp_dir):
    # Clean up the temporary directory
    mo.md(f"Cleaning up temporary directory: {temp_dir}")
    shutil.rmtree(temp_dir)
    mo.md("Temporary directory removed successfully")


if __name__ == "__main__":
    app.run()
