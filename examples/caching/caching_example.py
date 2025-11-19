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
        # Create a sample JSON file
        sample_file = os.path.join(tmpdir, "sample_data.json")
        sample_data = {
            "name": "fsspeckit caching example",
            "timestamp": time.time(),
            "items": [
                {"id": i, "value": f"item_{i}"} for i in range(1000)
            ],  # Larger dataset for better demo
        }

        # Write the sample data to our file
        with open(sample_file, "w") as f:
            json.dump(sample_data, f)

        print(f"Created sample file: {sample_file}")

        # Create a cache directory
        cache_dir = os.path.join(tmpdir, "cache")

        # Create a filesystem with caching enabled
        print("\n=== Creating filesystem with caching ===")
        fs = filesystem(
            protocol_or_path="file",
            cached=True,
            cache_storage=cache_dir,
            verbose=True,  # Enable verbose logging to see cache operations
        )

        # First read - this should populate the cache
        print("\n=== First read (populating cache) ===")
        start_time = time.time()
        data1 = fs.read_json(sample_file)
        first_read_time = time.time() - start_time
        print(f"First read completed in {first_read_time:.4f} seconds")
        print(f"Data keys: {list(data1.keys())}")

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
        data2 = fs.read_json(sample_file)
        second_read_time = time.time() - start_time
        print(f"Second read completed in {second_read_time:.4f} seconds")
        print(f"Data keys: {list(data2.keys())}")

        # Verify data is the same
        assert data1 == data2, "Data from first and second reads should be identical"
        print("✓ Data from both reads is identical")

        # Demonstrate cache effectiveness by removing original file
        print("\n=== Demonstrating cache effectiveness ===")
        print("Removing original file...")
        os.remove(sample_file)
        print(f"Original file exists: {os.path.exists(sample_file)}")

        # Third read - this should still work from cache
        print("\n=== Third read (from cache only) ===")
        try:
            start_time = time.time()
            data3 = fs.read_json(sample_file)
            third_read_time = time.time() - start_time
            print(f"Third read completed in {third_read_time:.4f} seconds")
            print(f"Data keys: {list(data3.keys())}")

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
