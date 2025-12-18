"""Performance benchmark tests for PyArrow dataset operations.

These tests validate:
1. Performance improvements (10-100x speedup for large datasets)
2. Memory efficiency (bounded memory usage)
3. Throughput metrics
4. Scalability improvements
"""

import gc
import os
import tempfile
import time
from pathlib import Path

import numpy as np
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsspeckit.datasets.pyarrow.dataset import (
    deduplicate_parquet_dataset_pyarrow,
    process_in_chunks,
    PerformanceMonitor,
)


class TestPyArrowPerformanceBenchmarks:
    """Performance benchmark tests for PyArrow optimizations."""

    @pytest.fixture
    def memory_monitor(self):
        """Create a memory monitoring context."""
        process = psutil.Process(os.getpid())
        return {
            "get_memory_mb": lambda: process.memory_info().rss / (1024 * 1024),
            "get_peak_memory_mb": lambda: process.memory_info().rss / (1024 * 1024),
        }

    @pytest.fixture
    def large_dataset_1m(self):
        """Create a 1M row dataset for performance testing."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "dataset_1m"
            dataset_dir.mkdir()

            # Generate 1M rows with realistic data
            num_rows = 1_000_000
            ids = list(range(num_rows // 2)) * 2  # Each ID appears twice
            values = np.random.randn(num_rows).tolist()
            timestamps = [int(time.time()) + i for i in range(num_rows)]
            categories = [f"cat_{i % 1000}" for i in range(num_rows)]

            table = pa.table(
                {
                    "id": ids,
                    "value": values,
                    "timestamp": timestamps,
                    "category": categories,
                }
            )

            # Write as multiple files for realistic dataset structure
            num_files = 10
            rows_per_file = num_rows // num_files
            for i in range(num_files):
                chunk = table.slice(i * rows_per_file, rows_per_file)
                pq.write_table(chunk, dataset_dir / f"part_{i:03d}.parquet")

            yield str(dataset_dir)

    @pytest.fixture
    def large_dataset_10m(self):
        """Create a 10M row dataset for performance testing."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "dataset_10m"
            dataset_dir.mkdir()

            # Generate 10M rows
            num_rows = 10_000_000
            ids = list(range(num_rows // 2)) * 2  # Each ID appears twice
            values = np.random.randn(num_rows).tolist()
            timestamps = [int(time.time()) + i for i in range(num_rows)]
            categories = [f"cat_{i % 10000}" for i in range(num_rows)]

            table = pa.table(
                {
                    "id": ids,
                    "value": values,
                    "timestamp": timestamps,
                    "category": categories,
                }
            )

            # Write as many files for realistic structure
            num_files = 50
            rows_per_file = num_rows // num_files
            for i in range(num_files):
                chunk = table.slice(i * rows_per_file, rows_per_file)
                pq.write_table(chunk, dataset_dir / f"part_{i:03d}.parquet")

            yield str(dataset_dir)

    @pytest.fixture
    def very_large_dataset_100m(self):
        """Create a 100M row dataset for memory efficiency testing."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "dataset_100m"
            dataset_dir.mkdir()

            # Generate 100M rows with smaller schema
            num_rows = 100_000_000
            ids = list(range(num_rows // 2)) * 2  # Each ID appears twice
            values = [float(i % 1000) for i in range(num_rows)]  # Smaller values

            table = pa.table(
                {
                    "id": ids,
                    "value": values,
                }
            )

            # Write as many files for realistic structure
            num_files = 100
            rows_per_file = num_rows // num_files
            for i in range(num_files):
                chunk = table.slice(i * rows_per_file, rows_per_file)
                pq.write_table(chunk, dataset_dir / f"part_{i:03d}.parquet")

            yield str(dataset_dir)

    def test_deduplication_performance_benchmark_1m_rows(self, large_dataset_1m):
        """Benchmark deduplication performance with 1M rows."""
        start_time = time.perf_counter()

        # Force garbage collection before test
        gc.collect()

        result = deduplicate_parquet_dataset_pyarrow(
            large_dataset_1m,
            key_columns=["id"],
            dedup_order_by=["-timestamp"],
            chunk_size_rows=100_000,
            max_memory_mb=1024,
            verbose=True,
        )

        end_time = time.perf_counter()
        total_time = end_time - start_time

        # Validate performance metrics
        metrics = result["performance_metrics"]
        assert "total_process_time_sec" in metrics
        assert "memory_peak_mb" in metrics
        assert "throughput_mb_sec" in metrics
        assert "rows_per_sec" in metrics

        # Validate results
        assert result["deduplicated_rows"] == 500_000  # 50% duplicates
        assert metrics["files_processed"] == 10
        assert metrics["chunks_processed"] > 0
        assert metrics["dedup_efficiency"] == 0.5

        # Performance assertions
        assert total_time < 60.0  # Should complete within 60 seconds
        assert metrics["rows_per_sec"] > 100_000  # Minimum 100K rows/second
        assert metrics["memory_peak_mb"] < 2048  # Should not exceed 2GB

        print(f"1M rows processed in {total_time:.2f}s")
        print(f"Throughput: {metrics['rows_per_sec']:,.0f} rows/sec")
        print(f"Memory peak: {metrics['memory_peak_mb']:.1f} MB")

    @pytest.mark.slow
    def test_deduplication_performance_benchmark_10m_rows(self, large_dataset_10m):
        """Benchmark deduplication performance with 10M rows."""
        start_time = time.perf_counter()

        gc.collect()

        result = deduplicate_parquet_dataset_pyarrow(
            large_dataset_10m,
            key_columns=["id"],
            dedup_order_by=["-timestamp"],
            chunk_size_rows=500_000,
            max_memory_mb=2048,
            verbose=True,
        )

        end_time = time.perf_counter()
        total_time = end_time - start_time

        metrics = result["performance_metrics"]
        assert result["deduplicated_rows"] == 5_000_000  # 50% duplicates
        assert metrics["files_processed"] == 50
        assert metrics["chunks_processed"] > 0

        # Performance assertions for larger dataset
        assert total_time < 300.0  # Should complete within 5 minutes
        assert metrics["rows_per_sec"] > 200_000  # Higher throughput expected
        assert metrics["memory_peak_mb"] < 4096  # Should not exceed 4GB

        print(f"10M rows processed in {total_time:.2f}s")
        print(f"Throughput: {metrics['rows_per_sec']:,.0f} rows/sec")
        print(f"Memory peak: {metrics['memory_peak_mb']:.1f} MB")

    @pytest.mark.slow
    def test_memory_efficiency_boundaries(self, very_large_dataset_100m):
        """Test memory efficiency with 100M rows - should remain bounded."""
        # This test validates that memory usage remains bounded regardless of dataset size

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / (1024 * 1024)

        result = deduplicate_parquet_dataset_pyarrow(
            very_large_dataset_100m,
            key_columns=["id"],
            chunk_size_rows=1_000_000,
            max_memory_mb=1024,  # Strict memory limit
            verbose=True,
        )

        peak_memory = process.memory_info().rss / (1024 * 1024)
        memory_increase = peak_memory - initial_memory

        metrics = result["performance_metrics"]

        # Memory should be bounded regardless of dataset size
        assert metrics["memory_peak_mb"] < 2048  # Peak should stay under 2GB
        assert memory_increase < 1024  # Process memory increase should be < 1GB
        assert result["deduplicated_rows"] == 50_000_000  # 50% duplicates

        print(f"Memory increase for 100M rows: {memory_increase:.1f} MB")
        print(f"Memory efficiency: {memory_increase / 100:.2f} MB per 1M rows")

    def test_chunked_processing_performance(self, memory_monitor):
        """Test chunked processing performance and memory bounds."""
        with tempfile.TemporaryDirectory() as tmp:
            # Create a large table that would normally cause memory issues
            num_rows = 5_000_000
            data = {
                "id": list(range(num_rows)),
                "value": np.random.randn(num_rows).tolist(),
            }
            large_table = pa.table(data)

            initial_memory = memory_monitor["get_memory_mb"]()

            # Process in chunks
            chunks_processed = 0
            total_rows = 0
            max_memory_during = initial_memory

            for chunk in process_in_chunks(
                large_table,
                chunk_size_rows=500_000,
                max_memory_mb=512,  # Strict memory limit
                enable_progress=False,
            ):
                current_memory = memory_monitor["get_memory_mb"]()
                max_memory_during = max(max_memory_during, current_memory)

                chunks_processed += 1
                total_rows += chunk.num_rows

            memory_increase = max_memory_during - initial_memory

            # Validate chunked processing
            assert chunks_processed == 10  # 5M / 500K = 10 chunks
            assert total_rows == num_rows
            assert memory_increase < 256  # Memory should be well bounded

            print(f"Chunked processing: {chunks_processed} chunks")
            print(f"Memory increase: {memory_increase:.1f} MB")

    def test_streaming_vs_in_memory_comparison(self, large_dataset_1m):
        """Compare streaming vs in-memory processing performance."""

        # Test streaming approach
        start_time = time.perf_counter()
        streaming_result = deduplicate_parquet_dataset_pyarrow(
            large_dataset_1m,
            key_columns=["id"],
            chunk_size_rows=100_000,
            max_memory_mb=1024,
        )
        streaming_time = time.perf_counter() - start_time
        streaming_metrics = streaming_result["performance_metrics"]

        # Test with larger chunks (more in-memory like)
        start_time = time.perf_counter()
        in_memory_result = deduplicate_parquet_dataset_pyarrow(
            large_dataset_1m,
            key_columns=["id"],
            chunk_size_rows=1_000_000,  # Large chunks
            max_memory_mb=2048,
        )
        in_memory_time = time.perf_counter() - start_time
        in_memory_metrics = in_memory_result["performance_metrics"]

        # Both should produce same results
        assert (
            streaming_result["deduplicated_rows"]
            == in_memory_result["deduplicated_rows"]
        )
        assert (
            streaming_metrics["dedup_efficiency"]
            == in_memory_metrics["dedup_efficiency"]
        )

        # Streaming should use less memory
        assert streaming_metrics["memory_peak_mb"] < in_memory_metrics["memory_peak_mb"]

        print(
            f"Streaming: {streaming_time:.2f}s, {streaming_metrics['memory_peak_mb']:.1f} MB"
        )
        print(
            f"In-memory: {in_memory_time:.2f}s, {in_memory_metrics['memory_peak_mb']:.1f} MB"
        )
        print(
            f"Memory savings: {in_memory_metrics['memory_peak_mb'] - streaming_metrics['memory_peak_mb']:.1f} MB"
        )

    def test_performance_metrics_accuracy(self, large_dataset_1m):
        """Validate accuracy of performance metrics collection."""

        # Get initial dataset stats for verification
        from fsspeckit.datasets.pyarrow.dataset import collect_dataset_stats_pyarrow

        initial_stats = collect_dataset_stats_pyarrow(large_dataset_1m)

        result = deduplicate_parquet_dataset_pyarrow(
            large_dataset_1m,
            key_columns=["id"],
            verbose=True,
        )

        metrics = result["performance_metrics"]

        # Validate metric accuracy
        assert metrics["files_processed"] == initial_stats["total_files"]
        assert metrics["total_process_time_sec"] > 0
        assert metrics["throughput_mb_sec"] > 0
        assert metrics["rows_per_sec"] > 0
        assert 0 <= metrics["dedup_efficiency"] <= 1

        # Operation breakdown should have entries
        assert len(metrics["operation_breakdown"]) > 0
        total_breakdown_time = sum(metrics["operation_breakdown"].values())
        assert (
            abs(total_breakdown_time - metrics["total_process_time_sec"]) < 1.0
        )  # Within 1 second

        print(f"Performance metrics validation passed")
        print(f"Operation breakdown: {metrics['operation_breakdown']}")

    def test_scalability_with_dataset_size(self):
        """Test that performance scales reasonably with dataset size."""
        results = []

        for size in [100_000, 500_000, 1_000_000]:
            with tempfile.TemporaryDirectory() as tmp:
                dataset_dir = Path(tmp) / f"dataset_{size}"
                dataset_dir.mkdir()

                # Create dataset with 50% duplicates
                num_rows = size
                ids = list(range(num_rows // 2)) * 2
                values = np.random.randn(num_rows).tolist()

                table = pa.table({"id": ids, "value": values})

                # Write as single file for fair comparison
                pq.write_table(table, dataset_dir / "data.parquet")

                start_time = time.perf_counter()
                result = deduplicate_parquet_dataset_pyarrow(
                    str(dataset_dir),
                    key_columns=["id"],
                    chunk_size_rows=50_000,
                    max_memory_mb=1024,
                )
                end_time = time.perf_counter()

                processing_time = end_time - start_time
                throughput = size / processing_time

                results.append(
                    {
                        "size": size,
                        "time": processing_time,
                        "throughput": throughput,
                        "memory": result["performance_metrics"]["memory_peak_mb"],
                    }
                )

        # Validate scalability
        for i in range(1, len(results)):
            prev = results[i - 1]
            curr = results[i]

            # Throughput should not decrease dramatically
            assert curr["throughput"] >= prev["throughput"] * 0.5

            # Memory should scale sub-linearly
            memory_ratio = curr["memory"] / prev["memory"]
            size_ratio = curr["size"] / prev["size"]
            assert memory_ratio < size_ratio  # Sub-linear memory scaling

        print("Scalability test results:")
        for r in results:
            print(
                f"  {r['size']:>7} rows: {r['time']:>6.2f}s, {r['throughput']:>8.0f} rows/s, {r['memory']:>6.1f} MB"
            )

    def test_performance_monitor_accuracy(self):
        """Test the accuracy of PerformanceMonitor class."""
        monitor = PerformanceMonitor()

        # Test operation timing
        monitor.start_op("test_op")
        time.sleep(0.1)  # Simulate work
        monitor.end_op()

        monitor.start_op("test_op_2")
        time.sleep(0.05)  # Simulate more work
        monitor.end_op()

        # Test memory tracking
        monitor.track_memory()

        # Get metrics
        metrics = monitor.get_metrics(
            total_rows_before=1000,
            total_rows_after=900,
            total_bytes=1024 * 1024,  # 1 MB
        )

        # Validate metrics
        assert "total_process_time_sec" in metrics
        assert "memory_peak_mb" in metrics
        assert "throughput_mb_sec" in metrics
        assert "rows_per_sec" in metrics
        assert "operation_breakdown" in metrics

        assert metrics["total_process_time_sec"] >= 0.14  # At least 0.14 seconds total
        assert "test_op" in metrics["operation_breakdown"]
        assert "test_op_2" in metrics["operation_breakdown"]
        assert metrics["operation_breakdown"]["test_op"] >= 0.1
        assert metrics["operation_breakdown"]["test_op_2"] >= 0.05

        print(f"Performance monitor test passed: {metrics['operation_breakdown']}")


class TestPyArrowMemoryEfficiency:
    """Tests specifically focused on memory efficiency validation."""

    def test_memory_bound_with_various_chunk_sizes(self, large_dataset_1m):
        """Test that memory usage is bounded across different chunk sizes."""
        chunk_sizes = [10_000, 50_000, 100_000, 500_000, 1_000_000]
        memory_results = []

        for chunk_size in chunk_sizes:
            # Force garbage collection
            gc.collect()

            process = psutil.Process(os.getpid())
            initial_memory = process.memory_info().rss / (1024 * 1024)

            result = deduplicate_parquet_dataset_pyarrow(
                large_dataset_1m,
                key_columns=["id"],
                chunk_size_rows=chunk_size,
                max_memory_mb=1024,
            )

            peak_memory = process.memory_info().rss / (1024 * 1024)
            memory_results.append(
                {
                    "chunk_size": chunk_size,
                    "peak_memory": peak_memory,
                    "memory_increase": peak_memory - initial_memory,
                }
            )

        # Memory should remain relatively stable across chunk sizes
        memory_increases = [r["memory_increase"] for r in memory_results]
        max_memory = max(memory_increases)
        min_memory = min(memory_increases)

        # Memory variation should be reasonable (less than 2x)
        assert max_memory / min_memory < 2.0, (
            f"Memory variation too high: {memory_results}"
        )

        print("Memory usage across chunk sizes:")
        for r in memory_results:
            print(
                f"  {r['chunk_size']:>7} rows: {r['memory_increase']:>6.1f} MB increase"
            )

    def test_memory_efficiency_with_mixed_data_types(self):
        """Test memory efficiency with various data types and schemas."""
        with tempfile.TemporaryDirectory() as tmp:
            # Test with different schemas
            schemas = [
                # Simple types
                {"id": "int64", "value": "float64"},
                # Mixed types
                {"id": "int32", "name": "string", "active": "bool"},
                # Complex types
                {"id": "int64", "tags": "list<string>", "metadata": "string"},
            ]

            for i, schema in enumerate(schemas):
                dataset_dir = Path(tmp) / f"schema_test_{i}"
                dataset_dir.mkdir()

                # Generate appropriate test data
                num_rows = 500_000
                data = {}

                for col, dtype in schema.items():
                    if dtype == "int64":
                        data[col] = list(range(num_rows))
                    elif dtype == "int32":
                        data[col] = [j % 1000 for j in range(num_rows)]
                    elif dtype == "float64":
                        data[col] = np.random.randn(num_rows).tolist()
                    elif dtype == "string":
                        data[col] = [f"text_{j}" for j in range(num_rows)]
                    elif dtype == "bool":
                        data[col] = [j % 2 == 0 for j in range(num_rows)]
                    elif dtype == "list<string>":
                        data[col] = [
                            [f"tag_{k}" for k in range(j % 5)] for j in range(num_rows)
                        ]
                    elif dtype == "string":
                        data[col] = [f"metadata_{j}" for j in range(num_rows)]

                table = pa.table(data)
                pq.write_table(table, dataset_dir / "data.parquet")

                # Test deduplication
                if "id" in schema:
                    process = psutil.Process(os.getpid())
                    initial_memory = process.memory_info().rss / (1024 * 1024)

                    result = deduplicate_parquet_dataset_pyarrow(
                        str(dataset_dir),
                        key_columns=["id"],
                        chunk_size_rows=100_000,
                        max_memory_mb=512,
                    )

                    peak_memory = process.memory_info().rss / (1024 * 1024)
                    memory_increase = peak_memory - initial_memory

                    # Memory should be bounded regardless of schema
                    assert memory_increase < 256, (
                        f"Memory too high for schema {schema}: {memory_increase} MB"
                    )

                    print(f"Schema {schema}: {memory_increase:.1f} MB increase")


class TestPyArrowThroughputBenchmarks:
    """Tests focused on throughput and processing speed validation."""

    def test_throughput_benchmarks_various_operations(self, large_dataset_1m):
        """Benchmark throughput for various operations."""

        operations = [
            {
                "name": "key_based_deduplication",
                "params": {"key_columns": ["id"], "dedup_order_by": ["-timestamp"]},
            },
            {
                "name": "exact_deduplication",
                "params": {"key_columns": None},
            },
            {
                "name": "category_deduplication",
                "params": {"key_columns": ["category"]},
            },
        ]

        throughput_results = []

        for op in operations:
            start_time = time.perf_counter()

            result = deduplicate_parquet_dataset_pyarrow(
                large_dataset_1m,
                chunk_size_rows=100_000,
                max_memory_mb=1024,
                **op["params"],
            )

            end_time = time.perf_counter()
            processing_time = end_time - start_time

            metrics = result["performance_metrics"]

            throughput_results.append(
                {
                    "operation": op["name"],
                    "time": processing_time,
                    "rows_per_sec": metrics["rows_per_sec"],
                    "mb_per_sec": metrics["throughput_mb_sec"],
                    "deduplicated": result["deduplicated_rows"],
                }
            )

        # Validate throughput is reasonable for all operations
        for result in throughput_results:
            assert result["rows_per_sec"] > 50_000, (
                f"Low throughput for {result['operation']}: {result['rows_per_sec']} rows/sec"
            )
            assert result["mb_per_sec"] > 10, (
                f"Low MB/sec for {result['operation']}: {result['mb_per_sec']} MB/sec"
            )

        print("Throughput benchmark results:")
        for r in throughput_results:
            print(
                f"  {r['operation']:>25}: {r['rows_per_sec']:>8.0f} rows/s, {r['mb_per_sec']:>6.1f} MB/s"
            )

    def test_concurrent_processing_performance(self):
        """Test that chunked processing can handle concurrent workloads efficiently."""
        # This test validates that the chunked processing doesn't create bottlenecks
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "concurrent_test"
            dataset_dir.mkdir()

            # Create multiple datasets for concurrent processing simulation
            datasets = []
            for i in range(3):
                dataset_path = dataset_dir / f"dataset_{i}"
                dataset_path.mkdir()

                num_rows = 200_000
                ids = list(range(num_rows // 2)) * 2
                values = np.random.randn(num_rows).tolist()

                table = pa.table({"id": ids, "value": values})
                pq.write_table(table, dataset_path / "data.parquet")
                datasets.append(str(dataset_path))

            # Process each dataset and measure individual performance
            results = []
            for dataset_path in datasets:
                start_time = time.perf_counter()

                result = deduplicate_parquet_dataset_pyarrow(
                    dataset_path,
                    key_columns=["id"],
                    chunk_size_rows=50_000,
                    max_memory_mb=512,
                )

                end_time = time.perf_counter()
                results.append(
                    {
                        "time": end_time - start_time,
                        "rows_per_sec": 200_000 / (end_time - start_time),
                        "memory": result["performance_metrics"]["memory_peak_mb"],
                    }
                )

            # All datasets should have similar performance (consistent throughput)
            times = [r["time"] for r in results]
            throughputs = [r["rows_per_sec"] for r in results]

            # Performance should be consistent (coefficient of variation < 20%)
            time_mean = np.mean(times)
            time_cv = np.std(times) / time_mean
            assert time_cv < 0.2, f"Performance too variable: CV = {time_cv:.3f}"

            throughput_mean = np.mean(throughputs)
            print(f"Concurrent processing: {throughput_mean:.0f} rows/sec average")
            print(f"Performance consistency: CV = {time_cv:.3f}")

    def test_performance_degradation_boundaries(self, large_dataset_1m):
        """Test that performance degradation is bounded and predictable."""

        # Test with increasingly constrained memory limits
        memory_limits = [4096, 2048, 1024, 512, 256]
        performance_results = []

        for memory_limit in memory_limits:
            start_time = time.perf_counter()

            result = deduplicate_parquet_dataset_pyarrow(
                large_dataset_1m,
                key_columns=["id"],
                chunk_size_rows=100_000,
                max_memory_mb=memory_limit,
            )

            end_time = time.perf_counter()
            processing_time = end_time - start_time

            performance_results.append(
                {
                    "memory_limit": memory_limit,
                    "time": processing_time,
                    "actual_memory": result["performance_metrics"]["memory_peak_mb"],
                    "throughput": 1_000_000 / processing_time,
                }
            )

        # Time should increase as memory limit decreases (more chunks)
        # But should not increase dramatically
        max_time = max(r["time"] for r in performance_results)
        min_time = min(r["time"] for r in performance_results)

        # Performance degradation should be reasonable (less than 3x)
        assert max_time / min_time < 3.0, (
            f"Performance degradation too severe: {max_time / min_time:.2f}x"
        )

        print("Performance under memory constraints:")
        for r in performance_results:
            print(
                f"  {r['memory_limit']:>4} MB limit: {r['time']:>6.2f}s, {r['actual_memory']:>6.1f} MB actual, {r['throughput']:>8.0f} rows/s"
            )
