"""
Example: Using Caching with fsspeckit

This example demonstrates how to use the caching functionality in fsspeckit
to improve performance for repeated file operations.

The example shows:
1. Creating a filesystem with caching enabled
2. Performing file operations that populate the cache
3. Demonstrating improved performance on subsequent reads
4. Showing how cached files can be accessed even when the original source is unavailable
"""

import tempfile
import time
import os
import json

# Import fsspeckit filesystem function
from fsspeckit import filesystem


def main():
    # Create a temporary directory and file for our demonstration
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a sample JSON file (use relative path from temp dir)
        sample_file = "sample_data.json"
        sample_data = {
            "name": "fsspeckit caching example",
            "timestamp": time.time(),
            "items": [
                {"id": i, "value": f"item_{i}"} for i in range(1000)
            ],  # Larger dataset for better demo
        }

        # Write the sample data to our file using absolute path for initial write
        abs_sample_file = os.path.join(tmpdir, sample_file)
        with open(abs_sample_file, "w") as f:
            json.dump(sample_data, f)

        print(f"Created sample file: {abs_sample_file}")

        # Create a cache directory
        cache_dir = "cache"

        # Create a filesystem rooted at the temp directory with caching enabled
        print("\n=== Creating filesystem with caching ===")
        fs = filesystem(
            protocol_or_path=tmpdir,  # Root filesystem at temp directory
            cached=True,
            cache_storage=cache_dir,
            verbose=True,  # Enable verbose logging to see cache operations
        )

        # First read - this should populate the cache
        print("\n=== First read (populating cache) ===")
        start_time = time.time()
        data1 = fs.read_json(sample_file, as_dataframe=False)  # Returns list of records
        first_read_time = time.time() - start_time
        print(f"First read completed in {first_read_time:.4f} seconds")
        print(f"Data type: {type(data1)}")
        if isinstance(data1, list) and len(data1) > 0:
            print(f"Data keys: {list(data1[0].keys())}")
        else:
            print(f"Data: {data1}")

        # Check if cache files were created
        cache_files = []
        if os.path.exists(cache_dir):
            for root, dirs, files in os.walk(cache_dir):
                for file in files:
                    cache_files.append(os.path.join(root, file))
        print(f"Cache files created: {len(cache_files)} files")
        for file in cache_files[:5]:  # Show first 5 cache files
            print(f"  - {file}")
        if len(cache_files) > 5:
            print(f"  ... and {len(cache_files) - 5} more files")

        # Second read - this should use the cache
        print("\n=== Second read (using cache) ===")
        start_time = time.time()
        data2 = fs.read_json(sample_file, as_dataframe=False)
        second_read_time = time.time() - start_time
        print(f"Second read completed in {second_read_time:.4f} seconds")
        print(f"Data type: {type(data2)}")
        if isinstance(data2, list) and len(data2) > 0:
            print(f"Data keys: {list(data2[0].keys())}")
        else:
            print(f"Data: {data2}")

        # Verify data is the same
        assert data1 == data2, "Data from first and second reads should be identical"
        print("✓ Data from both reads is identical")

        # Demonstrate cache effectiveness by removing original file
        print("\n=== Demonstrating cache effectiveness ===")
        print("Removing original file...")
        os.remove(abs_sample_file)  # Remove the absolute path file
        print(f"Original file exists: {os.path.exists(abs_sample_file)}")

        # Third read - this should still work from cache
        print("\n=== Third read (from cache only) ===")
        try:
            start_time = time.time()
            data3 = fs.read_json(sample_file, as_dataframe=False)
            third_read_time = time.time() - start_time
            print(f"Third read completed in {third_read_time:.4f} seconds")
            print(f"Data type: {type(data3)}")
            if isinstance(data3, list) and len(data3) > 0:
                print(f"Data keys: {list(data3[0].keys())}")
            else:
                print(f"Data: {data3}")

            # Verify data is still the same
            assert data1 == data3, "Data from cache should be identical to original"
            print("✓ Successfully read from cache even after original file was removed")

        except Exception as e:
            print(f"Error reading from cache: {e}")

        # Performance comparison
        print("\n=== Performance Comparison ===")
        print(f"First read (from disk): {first_read_time:.4f} seconds")
        print(f"Second read (from cache): {second_read_time:.4f} seconds")
        print(f"Third read (from cache): {third_read_time:.4f} seconds")

        if second_read_time < first_read_time:
            improvement = ((first_read_time - second_read_time) / first_read_time) * 100
            print(f"Cache improvement: {improvement:.1f}% faster")
        else:
            print(
                "Note: Cache read wasn't faster in this small example, but would be with larger files or remote storage"
            )


if __name__ == "__main__":
    main()
