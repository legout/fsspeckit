"""
Parallel Processing Example

This example demonstrates how to use fsspeckit's parallel processing utilities
to speed up data operations by leveraging multiple CPU cores.

The example covers:
1. Basic parallel execution with run_parallel
2. Processing multiple files in parallel
3. Parallel data transformation operations
4. Error handling in parallel workflows
5. Performance monitoring and optimization
6. Resource management and memory considerations

This example is designed for users who need to process large datasets
or multiple files efficiently using parallel processing.
"""

from __future__ import annotations

import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from fsspeckit.common.misc import run_parallel


def create_sample_datasets(num_datasets: int = 10) -> list[Path]:
    """Create multiple sample datasets for parallel processing."""

    print(f"Creating {num_datasets} sample datasets...")

    temp_dir = Path(tempfile.mkdtemp())
    dataset_paths = []

    for i in range(num_datasets):
        # Create sample data
        data = pa.table(
            {
                "id": pa.array(range(i * 100, (i + 1) * 100)),
                "value": pa.array([x * 1.5 for x in range(100)]),
                "category": pa.array([f"Cat_{x % 5}" for x in range(100)]),
                "timestamp": pa.array(
                    [f"2024-01-{(x % 28) + 1:02d}" for x in range(100)]
                ),
                "active": pa.array([x % 2 == 0 for x in range(100)]),
            }
        )

        # Write to file
        dataset_path = temp_dir / f"dataset_{i:03d}.parquet"
        pq.write_table(data, dataset_path)
        dataset_paths.append(dataset_path)

    print(f"Created datasets in: {temp_dir}")
    return dataset_paths


def demonstrate_basic_parallel_processing():
    """Demonstrate basic parallel processing with simple tasks."""

    print("\n🚀 Basic Parallel Processing")

    # Define simple tasks to run in parallel
    def simple_task(task_id: int) -> dict:
        """Simple task that simulates work."""
        time.sleep(0.1)  # Simulate work
        return {
            "task_id": task_id,
            "result": f"Task {task_id} completed",
            "processing_time": 0.1,
            "thread_id": threading.get_ident(),
        }

    # Run tasks sequentially first
    print("Running tasks sequentially...")
    start_time = time.time()
    sequential_results = []
    for i in range(8):
        result = simple_task(i)
        sequential_results.append(result)
    sequential_time = time.time() - start_time

    print(f"Sequential execution: {sequential_time:.2f} seconds")

    # Run tasks in parallel
    print("Running tasks in parallel...")
    start_time = time.time()
    parallel_results = run_parallel(simple_task, range(8), n_jobs=4)
    parallel_time = time.time() - start_time

    print(f"Parallel execution: {parallel_time:.2f} seconds")
    print(f"Speedup: {sequential_time / parallel_time:.2f}x")

    # Display results
    print(f"\nResults (first 3):")
    for i, result in enumerate(parallel_results[:3]):
        print(f"  Task {result['task_id']}: {result['result']}")


def demonstrate_parallel_file_processing():
    """Demonstrate parallel processing of multiple files."""

    print("\n📁 Parallel File Processing")

    # Create sample datasets
    dataset_paths = create_sample_datasets(8)

    def process_file(file_path: Path) -> dict:
        """Process a single parquet file."""
        start_time = time.time()

        try:
            # Read the file
            table = pq.read_table(file_path)

            # Perform some transformations
            processed_table = table.filter(pc.greater(table.column("value"), 50))

            # Calculate some statistics
            total_rows = len(table)
            filtered_rows = len(processed_table)
            avg_value = (
                pc.mean(processed_table.column("value")).as_py()
                if filtered_rows > 0
                else 0
            )

            processing_time = time.time() - start_time

            return {
                "file_name": file_path.name,
                "total_rows": total_rows,
                "filtered_rows": filtered_rows,
                "avg_filtered_value": avg_value,
                "processing_time": processing_time,
                "success": True,
            }

        except Exception as e:
            return {
                "file_name": file_path.name,
                "error": str(e),
                "processing_time": time.time() - start_time,
                "success": False,
            }

    # Process files in parallel
    print("Processing files in parallel...")
    start_time = time.time()
    results = run_parallel(process_file, dataset_paths, n_jobs=4)
    total_time = time.time() - start_time

    # Display results
    successful_results = [r for r in results if r.get("success", False)]
    failed_results = [r for r in results if not r.get("success", False)]

    print(f"\n📊 Processing Results:")
    print(f"  Total files: {len(dataset_paths)}")
    print(f"  Successful: {len(successful_results)}")
    print(f"  Failed: {len(failed_results)}")
    print(f"  Total time: {total_time:.2f} seconds")
    print(f"  Avg time per file: {total_time / len(dataset_paths):.2f} seconds")

    if successful_results:
        total_rows = sum(r["total_rows"] for r in successful_results)
        total_filtered = sum(r["filtered_rows"] for r in successful_results)
        avg_filter_rate = (total_filtered / total_rows) * 100

        print(f"  Total rows processed: {total_rows:,}")
        print(f"  Total rows after filter: {total_filtered:,}")
        print(f"  Average filter rate: {avg_filter_rate:.1f}%")

    if failed_results:
        print(f"\n❌ Failed files:")
        for result in failed_results:
            print(f"  {result['file_name']}: {result.get('error', 'Unknown error')}")

    # Cleanup
    import shutil

    shutil.rmtree(dataset_paths[0].parent)


def demonstrate_parallel_data_transformation():
    """Demonstrate parallel data transformation operations."""

    print("\n🔄 Parallel Data Transformation")

    # Create a large dataset to transform
    print("Creating large dataset...")
    large_data = pa.table(
        {
            "id": pa.array(range(10000)),
            "value": pa.array([x * 1.5 + 0.1 for x in range(10000)]),
            "category": pa.array([f"Category_{x % 20}" for x in range(10000)]),
            "score": pa.array([x * 0.1 for x in range(10000)]),
        }
    )

    # Define transformation function
    def transform_chunk(chunk_data: pa.Table, chunk_id: int) -> dict:
        """Transform a chunk of data."""
        start_time = time.time()

        # Apply transformations
        # 1. Filter by value
        filtered_chunk = chunk_data.filter(
            pc.greater_equal(chunk_data.column("value"), 100)
        )

        # 2. Add computed columns
        if len(filtered_chunk) > 0:
            computed_scores = pc.multiply(filtered_chunk.column("score"), 1.1)
            categories_upper = pc.utf8_upper(filtered_chunk.column("category"))

            transformed_chunk = filtered_chunk.set_column(
                filtered_chunk.column_names.index("score"), "score", computed_scores
            ).set_column(
                len(filtered_chunk.column_names), "category_upper", categories_upper
            )
        else:
            transformed_chunk = filtered_chunk

        # 3. Aggregate statistics
        stats = {}
        if len(transformed_chunk) > 0:
            stats = {
                "chunk_id": chunk_id,
                "input_rows": len(chunk_data),
                "output_rows": len(transformed_chunk),
                "avg_value": pc.mean(transformed_chunk.column("value")).as_py(),
                "max_score": pc.max(transformed_chunk.column("score")).as_py()
                if "score" in transformed_chunk.column_names
                else 0,
                "unique_categories": len(
                    pc.unique(transformed_chunk.column("category"))
                )
                if "category" in transformed_chunk.column_names
                else 0,
            }
        else:
            stats = {
                "chunk_id": chunk_id,
                "input_rows": len(chunk_data),
                "output_rows": 0,
                "avg_value": 0,
                "max_score": 0,
                "unique_categories": 0,
            }

        stats["processing_time"] = time.time() - start_time
        return stats

    # Split data into chunks
    chunk_size = 2000
    chunks = []
    for i in range(0, len(large_data), chunk_size):
        chunk = large_data.slice(i, min(chunk_size, len(large_data) - i))
        chunks.append((chunk, i // chunk_size))

    print(f"Created {len(chunks)} chunks of ~{chunk_size} rows each")

    # Process chunks in parallel
    print("Processing chunks in parallel...")
    start_time = time.time()
    chunk_results = run_parallel(
        lambda chunk_data: transform_chunk(chunk_data[0], chunk_data[1]),
        chunks,
        n_jobs=4,
    )
    parallel_time = time.time() - start_time

    # Process sequentially for comparison
    print("Processing chunks sequentially...")
    start_time = time.time()
    sequential_results = []
    for chunk, chunk_id in chunks:
        result = transform_chunk(chunk, chunk_id)
        sequential_results.append(result)
    sequential_time = time.time() - start_time

    # Compare performance
    print(f"\n⚡ Performance Comparison:")
    print(f"  Parallel time:   {parallel_time:.2f} seconds")
    print(f"  Sequential time: {sequential_time:.2f} seconds")
    print(f"  Speedup:          {sequential_time / parallel_time:.2f}x")

    # Aggregate results
    total_input = sum(r["input_rows"] for r in chunk_results)
    total_output = sum(r["output_rows"] for r in chunk_results)
    avg_processing_time = sum(r["processing_time"] for r in chunk_results) / len(
        chunk_results
    )

    print(f"\n📊 Transformation Results:")
    print(f"  Total input rows:  {total_input:,}")
    print(f"  Total output rows: {total_output:,}")
    print(
        f"  Filter rate:       {((total_input - total_output) / total_input * 100):.1f}%"
    )
    print(f"  Avg chunk time:    {avg_processing_time:.3f} seconds")


def demonstrate_error_handling_in_parallel():
    """Demonstrate error handling in parallel processing."""

    print("\n⚠️  Error Handling in Parallel Processing")

    def unreliable_task(task_id: int, should_fail: bool = False) -> dict:
        """Task that might fail."""
        time.sleep(0.05)  # Simulate work

        if should_fail or (task_id % 7 == 0):  # Fail for specific IDs
            raise ValueError(f"Simulated error in task {task_id}")

        return {
            "task_id": task_id,
            "status": "success",
            "result": f"Task {task_id} completed successfully",
        }

    # Mix of successful and failing tasks
    tasks = [(i, i % 5 == 0) for i in range(20)]  # Fail every 5th task

    print(f"Running {len(tasks)} tasks with expected failures...")

    # Run with error handling
    results = []
    failed_tasks = []

    def safe_task_wrapper(task_args):
        """Wrapper that catches exceptions."""
        task_id, should_fail = task_args
        try:
            return unreliable_task(task_id, should_fail)
        except Exception as e:
            return {
                "task_id": task_id,
                "status": "failed",
                "error": str(e),
                "error_type": type(e).__name__,
            }

    # Process all tasks
    all_results = run_parallel(safe_task_wrapper, tasks, n_jobs=4)

    # Separate successful and failed results
    successful_results = [r for r in all_results if r.get("status") == "success"]
    failed_results = [r for r in all_results if r.get("status") == "failed"]

    print(f"\n📊 Results Summary:")
    print(f"  Total tasks:      {len(all_results)}")
    print(f"  Successful:       {len(successful_results)}")
    print(f"  Failed:           {len(failed_results)}")
    print(
        f"  Success rate:     {(len(successful_results) / len(all_results) * 100):.1f}%"
    )

    if failed_results:
        print(f"\n❌ Failed Tasks:")
        for result in failed_results[:5]:  # Show first 5 failures
            print(
                f"  Task {result['task_id']}: {result['error']} ({result['error_type']})"
            )
        if len(failed_results) > 5:
            print(f"  ... and {len(failed_results) - 5} more failures")

    if successful_results:
        print(f"\n✅ Successful Tasks:")
        for result in successful_results[:3]:  # Show first 3 successes
            print(f"  Task {result['task_id']}: {result['result']}")
        if len(successful_results) > 3:
            print(f"  ... and {len(successful_results) - 3} more successes")


def demonstrate_resource_management():
    """Demonstrate resource management in parallel processing."""

    print("\n💾 Resource Management")

    # Task with memory usage monitoring
    def memory_intensive_task(task_id: int, data_size: int) -> dict:
        """Task that uses significant memory."""
        import psutil
        import os

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Create data to consume memory
        large_data = pa.table({"data": pa.array([x for x in range(data_size)])})

        # Simulate processing
        time.sleep(0.1)

        # Peak memory usage
        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - initial_memory

        # Clean up
        del large_data

        return {
            "task_id": task_id,
            "data_size": data_size,
            "initial_memory_mb": initial_memory,
            "peak_memory_mb": peak_memory,
            "memory_increase_mb": memory_increase,
            "success": True,
        }

    # Test different worker counts
    data_sizes = [1000, 5000, 10000, 20000]
    worker_counts = [1, 2, 4]

    print("Testing different worker configurations...")

    for workers in worker_counts:
        print(f"\nTesting with {workers} workers:")

        start_time = time.time()
        tasks = [(i, size) for i, size in enumerate(data_sizes)]

        results = run_parallel(
            lambda task_args: memory_intensive_task(task_args[0], task_args[1]),
            tasks,
            n_jobs=workers,
        )

        execution_time = time.time() - start_time
        total_memory_increase = sum(r["memory_increase_mb"] for r in results)

        print(f"  Execution time:     {execution_time:.2f} seconds")
        print(f"  Total memory used:  {total_memory_increase:.1f} MB")
        print(f"  Avg memory per task: {total_memory_increase / len(results):.1f} MB")


def main():
    """Run all parallel processing examples."""

    print("⚡ Parallel Processing Example")
    print("=" * 50)

    try:
        # Run all parallel processing demonstrations
        demonstrate_basic_parallel_processing()
        demonstrate_parallel_file_processing()
        demonstrate_parallel_data_transformation()
        demonstrate_error_handling_in_parallel()
        demonstrate_resource_management()

        print("\n" + "=" * 50)
        print("✅ All parallel processing examples completed!")

        print("\n💡 Key Takeaways:")
        print("• Use run_parallel() for CPU-bound tasks")
        print("• Monitor memory usage with large datasets")
        print("• Handle errors gracefully in parallel workflows")
        print("• Choose appropriate worker counts based on your system")
        print("• Consider chunk size for large data processing")
        print("• Profile performance to find optimal configurations")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()
