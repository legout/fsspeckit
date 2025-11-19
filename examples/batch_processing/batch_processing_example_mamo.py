"""Learn more about Marimo: https://marimo.io"""

import marimo

__generated_with = "0.2.2"
app = marimo.App()


@app.cell
def __():
    import tempfile
    import shutil
    import os
    from pathlib import Path
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Import fsspeckit
    from fsspeckit import filesystem

    return filesystem, os, pa, Path, pl, pq, shutil, tempfile


@app.cell
def __(os, pl):
    def create_sample_data(temp_dir):
        """Create sample Parquet, CSV, and JSON files for demonstration."""
        print(f"Creating sample data in {temp_dir}")

        # Create sample data
        sample_data = [
            {"id": 1, "name": "Alice", "age": 25, "city": "New York"},
            {"id": 2, "name": "Bob", "age": 30, "city": "London"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "Paris"},
            {"id": 4, "name": "Diana", "age": 28, "city": "Tokyo"},
            {"id": 5, "name": "Eve", "age": 32, "city": "Berlin"},
            {"id": 6, "name": "Frank", "age": 27, "city": "Sydney"},
            {"id": 7, "name": "Grace", "age": 29, "city": "Toronto"},
            {"id": 8, "name": "Henry", "age": 31, "city": "Moscow"},
        ]

        # Create Parquet files
        for i in range(3):
            file_path = os.path.join(temp_dir, f"data_{i + 1}.parquet")
            df = pl.DataFrame(sample_data[i * 2 : (i + 1) * 2])
            df.write_parquet(file_path)
            print(f"Created Parquet file: {file_path}")

        # Create CSV files
        for i in range(3):
            file_path = os.path.join(temp_dir, f"data_{i + 1}.csv")
            df = pl.DataFrame(sample_data[i * 2 : (i + 1) * 2])
            df.write_csv(file_path)
            print(f"Created CSV file: {file_path}")

        # Create JSON files
        for i in range(3):
            file_path = os.path.join(temp_dir, f"data_{i + 1}.json")
            with open(file_path, "w") as f:
                import json

                json.dump(sample_data[i * 2 : (i + 1) * 2], f)
            print(f"Created JSON file: {file_path}")

    return (create_sample_data,)


@app.cell
def __(filesystem, os):
    def demonstrate_parquet_batch_reading(temp_dir):
        """Demonstrate batch reading of Parquet files."""
        print("\n=== Parquet Batch Reading ===")

        # Get filesystem
        fs = filesystem("file")

        # Define the path pattern for Parquet files
        parquet_path = os.path.join(temp_dir, "*.parquet")

        # Example 1: Read Parquet files in batches
        print("\n1. Reading Parquet files in batches (batch_size=2):")
        for i, batch in enumerate(fs.read_parquet(parquet_path, batch_size=2)):
            print(f"   Batch {i + 1}:")
            print(f"   - Type: {type(batch)}")
            print(f"   - Number of rows: {batch.num_rows}")
            print(f"   - Columns: {batch.column_names}")
            print(f"   - Data preview: {batch.to_pandas().head(1).to_dict('records')}")

        # Example 2: Read Parquet files with include_file_path=True
        print("\n2. Reading Parquet files with include_file_path=True:")
        for i, batch in enumerate(
            fs.read_parquet(parquet_path, batch_size=2, include_file_path=True)
        ):
            print(f"   Batch {i + 1}:")
            print(f"   - Type: {type(batch)}")
            print(f"   - Number of rows: {batch.num_rows}")
            print(f"   - Columns: {batch.column_names}")
            if "file_path" in batch.column_names:
                file_paths = batch.column("file_path").to_pylist()
                print(f"   - File paths: {set(file_paths)}")

    return (demonstrate_parquet_batch_reading,)


@app.cell
def __(filesystem, os):
    def demonstrate_csv_batch_reading(temp_dir):
        """Demonstrate batch reading of CSV files."""
        print("\n=== CSV Batch Reading ===")

        # Get filesystem
        fs = filesystem("file")

        # Define the path pattern for CSV files
        csv_path = os.path.join(temp_dir, "*.csv")

        # Example 1: Read CSV files in batches
        print("\n1. Reading CSV files in batches (batch_size=2):")
        for i, batch in enumerate(fs.read_csv(csv_path, batch_size=2)):
            print(f"   Batch {i + 1}:")
            print(f"   - Type: {type(batch)}")
            print(f"   - Shape: {batch.shape}")
            print(f"   - Columns: {batch.columns}")
            print(f"   - Data preview: {batch.head(1).to_dicts()}")

        # Example 2: Read CSV files with include_file_path=True
        print("\n2. Reading CSV files with include_file_path=True:")
        for i, batch in enumerate(
            fs.read_csv(csv_path, batch_size=2, include_file_path=True)
        ):
            print(f"   Batch {i + 1}:")
            print(f"   - Type: {type(batch)}")
            print(f"   - Shape: {batch.shape}")
            print(f"   - Columns: {batch.columns}")
            if "file_path" in batch.columns:
                file_paths = batch["file_path"].unique().to_list()
                print(f"   - File paths: {file_paths}")

    return (demonstrate_csv_batch_reading,)


@app.cell
def __(filesystem, os):
    def demonstrate_json_batch_reading(temp_dir):
        """Demonstrate batch reading of JSON files."""
        print("\n=== JSON Batch Reading ===")

        # Get filesystem
        fs = filesystem("file")

        # Define the path pattern for JSON files
        json_path = os.path.join(temp_dir, "*.json")

        # Example 1: Read JSON files in batches
        print("\n1. Reading JSON files in batches (batch_size=2):")
        for i, batch in enumerate(fs.read_json(json_path, batch_size=2)):
            print(f"   Batch {i + 1}:")
            print(f"   - Type: {type(batch)}")
            print(f"   - Shape: {batch.shape}")
            print(f"   - Columns: {batch.columns}")
            print(f"   - Data preview: {batch.head(1).to_dicts()}")

        # Example 2: Read JSON files with include_file_path=True
        print("\n2. Reading JSON files with include_file_path=True:")
        for i, batch in enumerate(
            fs.read_json(json_path, batch_size=2, include_file_path=True)
        ):
            print(f"   Batch {i + 1}:")
            print(f"   - Type: {type(batch)}")
            print(f"   - Shape: {batch.shape}")
            print(f"   - Columns: {batch.columns}")
            if "file_path" in batch.columns:
                file_paths = batch["file_path"].unique().to_list()
                print(f"   - File paths: {file_paths}")

    return (demonstrate_json_batch_reading,)


@app.cell
def __(tempfile):
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    print(f"Created temporary directory: {temp_dir}")
    return (temp_dir,)


@app.cell
def __(create_sample_data, temp_dir):
    # Create sample data
    create_sample_data(temp_dir)
    return


@app.cell
def __(demonstrate_parquet_batch_reading, temp_dir):
    # Demonstrate batch reading for each format
    demonstrate_parquet_batch_reading(temp_dir)
    return


@app.cell
def __(demonstrate_csv_batch_reading, temp_dir):
    demonstrate_csv_batch_reading(temp_dir)
    return


@app.cell
def __(demonstrate_json_batch_reading, temp_dir):
    demonstrate_json_batch_reading(temp_dir)
    return


@app.cell
def __(shutil, temp_dir):
    # Clean up temporary directory
    shutil.rmtree(temp_dir)
    print(f"\nCleaned up temporary directory: {temp_dir}")
    return


if __name__ == "__main__":
    app.run()
