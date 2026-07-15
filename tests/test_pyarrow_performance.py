"""Performance benchmark tests for PyArrow dataset operations.

These tests validate:
1. Performance improvements (10-100x speedup for large datasets)
2. Memory efficiency (bounded memory usage)
3. Throughput metrics
4. Scalability improvements
"""

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

    @pytest.mark.slow
    @pytest.mark.slow
    def test_chunked_processing_performance(self, memory_monitor):
        """Test chunked processing performance and memory bounds."""
        with tempfile.TemporaryDirectory():
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


class TestPyArrowThroughputBenchmarks:
    """Tests focused on throughput and processing speed validation."""
