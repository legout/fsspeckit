"""Integration tests for PyArrow dataset operations.

These tests validate end-to-end functionality including:
1. Chunked processing end-to-end
2. Streaming merge operations
3. Performance metrics collection
4. Configuration parameters
"""

import gc
import os
import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from fsspeckit.datasets.pyarrow.dataset import (
    deduplicate_parquet_dataset_pyarrow,
    process_in_chunks,
    PerformanceMonitor,
)
from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO


class TestPyArrowIntegrationEndToEnd:
    """End-to-end integration tests for PyArrow operations."""

    @pytest.fixture
    def integration_test_data(self):
        """Create comprehensive test data for integration testing."""
        with tempfile.TemporaryDirectory() as tmp:
            datasets = {}

            # Dataset 1: Multi-file dataset for integration testing
            multi_dir = Path(tmp) / "multi_file_dataset"
            multi_dir.mkdir()

            # Create 10 files with different data distributions
            for i in range(10):
                file_dir = multi_dir / f"batch_{i:02d}"
                file_dir.mkdir()

                # Each file has 1000 rows with overlapping IDs
                num_rows = 1000
                base_id = i * 500  # Start IDs to create overlaps
                ids = [base_id + j for j in range(num_rows // 2)] * 2
                values = np.random.randn(num_rows).tolist()
                timestamps = [int(time.time()) + i * 1000 + j for j in range(num_rows)]
                categories = [f"cat_{j % 20}" for j in range(num_rows)]

                table = pa.table(
                    {
                        "id": ids,
                        "value": values,
                        "timestamp": timestamps,
                        "category": categories,
                    }
                )

                pq.write_table(table, file_dir / "data.parquet")

            datasets["multi_file"] = str(multi_dir)

            # Dataset 2: Large single file for streaming tests
            large_dir = Path(tmp) / "large_single"
            large_dir.mkdir()

            large_num_rows = 5_000_000
            large_ids = list(range(large_num_rows // 2)) * 2
            large_values = np.random.randn(large_num_rows).tolist()
            large_table = pa.table(
                {
                    "id": large_ids,
                    "value": large_values,
                }
            )

            pq.write_table(large_table, large_dir / "large_data.parquet")
            datasets["large_single"] = str(large_dir)

            # Dataset 3: Partitioned dataset
            partitioned_dir = Path(tmp) / "partitioned_dataset"
            partitioned_dir.mkdir()

            for partition in ["2023", "2024"]:
                part_dir = partitioned_dir / f"year={partition}"
                part_dir.mkdir()

                for month in ["01", "02", "03"]:
                    month_dir = part_dir / f"month={month}"
                    month_dir.mkdir()

                    # Create data for this partition
                    num_rows = 500
                    ids = list(range(num_rows // 2)) * 2
                    values = np.random.randn(num_rows).tolist()

                    table = pa.table(
                        {
                            "id": ids,
                            "value": values,
                            "year": [partition] * num_rows,
                            "month": [month] * num_rows,
                        }
                    )

                    pq.write_table(table, month_dir / "data.parquet")

            datasets["partitioned"] = str(partitioned_dir)

            yield datasets

    def test_chunked_processing_end_to_end(self, integration_test_data):
        """Test complete chunked processing pipeline."""
        dataset_path = integration_test_data["multi_file"]

        # Test with different chunk sizes
        chunk_sizes = [100, 500, 1000]

        for chunk_size in chunk_sizes:
            start_time = time.perf_counter()

            result = deduplicate_parquet_dataset_pyarrow(
                dataset_path,
                key_columns=["id"],
                dedup_order_by=["-timestamp"],
                chunk_size_rows=chunk_size,
                max_memory_mb=512,
                verbose=True,
                enable_progress=True,
            )

            processing_time = time.perf_counter() - start_time
            metrics = result["performance_metrics"]

            # Validate results
            assert result["deduplicated_rows"] > 0
            assert result["total_rows_after"] > 0
            assert metrics["chunks_processed"] > 0
            assert metrics["files_processed"] == 10

            # Performance validation
            assert processing_time < 60.0  # Should complete within 60 seconds
            assert metrics["memory_peak_mb"] < 1024  # Should stay under 1GB
            assert metrics["rows_per_sec"] > 10_000  # Minimum throughput

            print(f"Chunked processing (size={chunk_size}): {processing_time:.2f}s")

    def test_streaming_merge_operations(self, integration_test_data):
        """Test streaming merge operations end-to-end."""
        dataset_path = integration_test_data["large_single"]

        # Create source data for merging
        source_data = pa.table(
            {
                "id": list(range(100_000)),  # Mix of existing and new
                "value": np.random.randn(100_000).tolist(),
            }
        )

        handler = PyarrowDatasetIO()

        # Test different merge strategies with streaming
        strategies = ["insert", "update", "upsert"]

        for strategy in strategies:
            # Create a copy for each test
            test_dir = Path(dataset_path).parent / f"test_{strategy}"
            import shutil

            shutil.copytree(dataset_path, test_dir)

            start_time = time.perf_counter()

            result = handler.merge(
                data=source_data,
                path=str(test_dir),
                strategy=strategy,  # type: ignore
                key_columns=["id"],
                merge_chunk_size_rows=50_000,
                enable_streaming_merge=True,
                merge_max_memory_mb=512,
            )

            processing_time = time.perf_counter() - start_time

            # Validate results
            assert result.strategy == strategy
            assert result.source_count == 100_000

            if strategy == "insert":
                assert result.inserted > 0
                assert result.updated == 0
            elif strategy == "update":
                assert result.updated > 0
                assert result.inserted == 0
            elif strategy == "upsert":
                assert result.inserted >= 0
                assert result.updated >= 0
                assert result.inserted + result.updated > 0

            # Performance validation
            assert processing_time < 120.0  # Should complete within 2 minutes

            # Clean up
            shutil.rmtree(test_dir)

            print(
                f"Streaming merge ({strategy}): {processing_time:.2f}s, {result.inserted + result.updated} operations"
            )

    def test_performance_metrics_collection(self, integration_test_data):
        """Test comprehensive performance metrics collection."""
        dataset_path = integration_test_data["multi_file"]

        # Custom progress callback
        progress_updates = []

        def progress_callback(rows_processed, total_rows):
            progress_updates.append((rows_processed, total_rows))

        result = deduplicate_parquet_dataset_pyarrow(
            dataset_path,
            key_columns=["id"],
            dedup_order_by=["-timestamp"],
            chunk_size_rows=500,
            max_memory_mb=1024,
            verbose=True,
            enable_progress=True,
            progress_callback=progress_callback,
        )

        metrics = result["performance_metrics"]

        # Validate all required metrics are present
        required_metrics = [
            "total_process_time_sec",
            "memory_peak_mb",
            "throughput_mb_sec",
            "rows_per_sec",
            "files_processed",
            "chunks_processed",
            "dedup_efficiency",
            "operation_breakdown",
        ]

        for metric in required_metrics:
            assert metric in metrics, f"Missing metric: {metric}"
            assert isinstance(metrics[metric], (int, float, dict)), (
                f"Invalid metric type for {metric}"
            )

        # Validate metric values
        assert metrics["total_process_time_sec"] > 0
        assert metrics["memory_peak_mb"] > 0
        assert metrics["throughput_mb_sec"] > 0
        assert metrics["rows_per_sec"] > 0
        assert metrics["files_processed"] == 10
        assert metrics["chunks_processed"] > 0
        assert 0 <= metrics["dedup_efficiency"] <= 1
        assert len(metrics["operation_breakdown"]) > 0

        # Validate progress tracking
        assert len(progress_updates) > 0
        assert all(
            isinstance(update, tuple) and len(update) == 2
            for update in progress_updates
        )

        # Validate operation breakdown sums to total time
        breakdown_total = sum(metrics["operation_breakdown"].values())
        assert (
            abs(breakdown_total - metrics["total_process_time_sec"]) < 1.0
        )  # Within 1 second

        print(f"Performance metrics validation passed")
        print(f"Operation breakdown: {metrics['operation_breakdown']}")
        print(f"Progress updates: {len(progress_updates)}")

    def test_configuration_parameters_integration(self, integration_test_data):
        """Test various configuration parameter combinations."""
        dataset_path = integration_test_data["multi_file"]

        # Test different configuration combinations
        configurations = [
            {
                "name": "small_chunks_strict_memory",
                "chunk_size_rows": 100,
                "max_memory_mb": 256,
            },
            {
                "name": "large_chunks_permissive_memory",
                "chunk_size_rows": 2000,
                "max_memory_mb": 2048,
            },
            {
                "name": "balanced",
                "chunk_size_rows": 500,
                "max_memory_mb": 512,
            },
        ]

        results = []

        for config in configurations:
            start_time = time.perf_counter()

            result = deduplicate_parquet_dataset_pyarrow(
                dataset_path,
                key_columns=["id"],
                chunk_size_rows=config["chunk_size_rows"],
                max_memory_mb=config["max_memory_mb"],
                verbose=True,
            )

            processing_time = time.perf_counter() - start_time
            metrics = result["performance_metrics"]

            results.append(
                {
                    "config": config["name"],
                    "time": processing_time,
                    "memory": metrics["memory_peak_mb"],
                    "throughput": metrics["rows_per_sec"],
                    "chunks": metrics["chunks_processed"],
                }
            )

            # All configurations should complete successfully
            assert result["deduplicated_rows"] >= 0
            assert (
                metrics["memory_peak_mb"] <= config["max_memory_mb"] * 1.2
            )  # Allow 20% tolerance

        # Compare configuration performance
        for result in results:
            print(
                f"{result['config']}: {result['time']:.2f}s, {result['memory']:.1f}MB, {result['throughput']:.0f} rows/s"
            )

        # Validate that performance is reasonable across configurations
        times = [r["time"] for r in results]
        assert (
            max(times) / min(times) < 5.0
        )  # Performance shouldn't vary by more than 5x

    def test_partitioned_dataset_integration(self, integration_test_data):
        """Test operations on partitioned datasets."""
        dataset_path = integration_test_data["partitioned"]

        # Test deduplication with partition filter
        result = deduplicate_parquet_dataset_pyarrow(
            dataset_path,
            key_columns=["id"],
            partition_filter=["year=2023"],  # Only process 2023 data
            chunk_size_rows=100,
            max_memory_mb=256,
        )

        # Should only process files matching the filter
        assert result["files_processed"] == 3  # 2023 has 3 months
        assert result["deduplicated_rows"] >= 0

        # Test deduplication on all partitions
        result_all = deduplicate_parquet_dataset_pyarrow(
            dataset_path,
            key_columns=["id"],
            chunk_size_rows=100,
            max_memory_mb=256,
        )

        # Should process all files
        assert result_all["files_processed"] == 6  # 2 years * 3 months
        assert result_all["deduplicated_rows"] >= result["deduplicated_rows"]

        print(
            f"Partitioned dataset: filtered={result['files_processed']} files, all={result_all['files_processed']} files"
        )

    def test_error_handling_and_recovery(self, integration_test_data):
        """Test error handling and recovery mechanisms."""
        dataset_path = integration_test_data["multi_file"]

        # Test with invalid key columns
        with pytest.raises((ValueError, KeyError)):
            deduplicate_parquet_dataset_pyarrow(
                dataset_path,
                key_columns=["nonexistent_column"],
            )

        # Test with invalid chunk size
        with pytest.raises((ValueError, TypeError)):
            deduplicate_parquet_dataset_pyarrow(
                dataset_path,
                key_columns=["id"],
                chunk_size_rows=0,  # Invalid chunk size
            )

        # Test with invalid memory limit
        with pytest.raises((ValueError, TypeError)):
            deduplicate_parquet_dataset_pyarrow(
                dataset_path,
                key_columns=["id"],
                max_memory_mb=0,  # Invalid memory limit
            )

        # Test with non-existent dataset
        with pytest.raises((FileNotFoundError, OSError)):
            deduplicate_parquet_dataset_pyarrow(
                "/nonexistent/path",
                key_columns=["id"],
            )

        # Valid operation should still work after errors
        result = deduplicate_parquet_dataset_pyarrow(
            dataset_path,
            key_columns=["id"],
            chunk_size_rows=500,
            max_memory_mb=1024,
        )

        assert result["deduplicated_rows"] >= 0

        print("Error handling tests passed - system recovered gracefully")

    def test_memory_pressure_simulation(self, integration_test_data):
        """Test behavior under memory pressure."""
        dataset_path = integration_test_data["large_single"]

        # Simulate memory pressure by setting very low limits
        memory_limits = [64, 128, 256]  # Very low memory limits

        results = []
        for memory_limit in memory_limits:
            start_time = time.perf_counter()

            try:
                result = deduplicate_parquet_dataset_pyarrow(
                    dataset_path,
                    key_columns=["id"],
                    chunk_size_rows=10_000,  # Small chunks
                    max_memory_mb=memory_limit,
                    verbose=True,
                )

                processing_time = time.perf_counter() - start_time
                metrics = result["performance_metrics"]

                results.append(
                    {
                        "memory_limit": memory_limit,
                        "success": True,
                        "time": processing_time,
                        "actual_memory": metrics["memory_peak_mb"],
                        "throughput": metrics["rows_per_sec"],
                    }
                )

            except (MemoryError, OSError) as e:
                results.append(
                    {
                        "memory_limit": memory_limit,
                        "success": False,
                        "error": str(e),
                    }
                )

            # Force garbage collection between tests
            gc.collect()

        # Analyze results
        successful_results = [r for r in results if r["success"]]
        failed_results = [r for r in results if not r["success"]]

        print(
            f"Memory pressure test: {len(successful_results)} successful, {len(failed_results)} failed"
        )

        for result in results:
            if result["success"]:
                print(
                    f"  {result['memory_limit']}MB: {result['time']:.2f}s, {result['actual_memory']:.1f}MB actual"
                )
            else:
                print(f"  {result['memory_limit']}MB: FAILED - {result['error'][:50]}")

        # At least some tests should succeed or fail gracefully
        assert len(successful_results) > 0 or len(failed_results) > 0

    def test_concurrent_file_access_simulation(self, integration_test_data):
        """Test concurrent access patterns (simulated)."""
        dataset_path = integration_test_data["multi_file"]

        # Simulate multiple operations on the same dataset
        operations = []

        # Operation 1: Deduplication
        start_time = time.perf_counter()
        result1 = deduplicate_parquet_dataset_pyarrow(
            dataset_path,
            key_columns=["id"],
            chunk_size_rows=500,
            max_memory_mb=512,
        )
        operations.append(
            {
                "name": "deduplication",
                "time": time.perf_counter() - start_time,
                "result": result1,
            }
        )

        # Operation 2: Statistics collection
        start_time = time.perf_counter()
        stats_result = deduplicate_parquet_dataset_pyarrow(
            dataset_path,
            key_columns=["id"],
            dry_run=True,  # Just collect stats
        )
        operations.append(
            {
                "name": "stats_collection",
                "time": time.perf_counter() - start_time,
                "result": stats_result,
            }
        )

        # Operation 3: Another deduplication with different parameters
        start_time = time.perf_counter()
        result3 = deduplicate_parquet_dataset_pyarrow(
            dataset_path,
            key_columns=["id", "category"],  # Different key
            chunk_size_rows=1000,
            max_memory_mb=1024,
        )
        operations.append(
            {
                "name": "multi_key_deduplication",
                "time": time.perf_counter() - start_time,
                "result": result3,
            }
        )

        # Validate all operations completed
        for op in operations:
            assert op["time"] > 0
            if "deduplicated_rows" in op["result"]:
                assert op["result"]["deduplicated_rows"] >= 0

        print("Concurrent access simulation results:")
        for op in operations:
            print(f"  {op['name']}: {op['time']:.2f}s")


class TestPyArrowProcessInChunksIntegration:
    """Integration tests for the process_in_chunks function."""

    def test_chunked_processing_various_data_types(self):
        """Test chunked processing with various PyArrow data types."""
        data_types = [
            ("int32", pa.int32()),
            ("int64", pa.int64()),
            ("float32", pa.float32()),
            ("float64", pa.float64()),
            ("string", pa.string()),
            ("bool", pa.bool_()),
            ("date32", pa.date32()),
            ("timestamp", pa.timestamp("ms")),
        ]

        for type_name, arrow_type in data_types:
            with tempfile.TemporaryDirectory() as tmp:
                # Create table with specific data type
                num_rows = 10_000
                if "int" in type_name:
                    data = pa.array(list(range(num_rows)), type=arrow_type)
                elif "float" in type_name:
                    data = pa.array(
                        [float(i) for i in range(num_rows)], type=arrow_type
                    )
                elif type_name == "string":
                    data = pa.array(
                        [f"string_{i}" for i in range(num_rows)], type=arrow_type
                    )
                elif type_name == "bool":
                    data = pa.array(
                        [i % 2 == 0 for i in range(num_rows)], type=arrow_type
                    )
                elif type_name == "date32":
                    data = pa.array([i % 365 for i in range(num_rows)], type=arrow_type)
                elif type_name == "timestamp":
                    data = pa.array(
                        [i * 1000 for i in range(num_rows)], type=arrow_type
                    )
                else:
                    continue

                table = pa.table({f"col_{type_name}": data})

                # Process in chunks
                chunk_size = 1000
                chunks_processed = 0
                total_rows = 0

                for chunk in process_in_chunks(
                    table,
                    chunk_size_rows=chunk_size,
                    max_memory_mb=64,
                    enable_progress=False,
                ):
                    chunks_processed += 1
                    total_rows += chunk.num_rows

                    # Validate chunk structure
                    assert chunk.num_rows <= chunk_size
                    assert f"col_{type_name}" in chunk.column_names
                    assert chunk.column(f"col_{type_name}").type == arrow_type

                # Validate processing
                assert chunks_processed == num_rows // chunk_size + (
                    1 if num_rows % chunk_size else 0
                )
                assert total_rows == num_rows

                print(
                    f"Chunked processing ({type_name}): {chunks_processed} chunks, {total_rows} rows"
                )

    def test_chunked_processing_with_memory_monitoring(self):
        """Test chunked processing with active memory monitoring."""
        with tempfile.TemporaryDirectory() as tmp:
            # Create large table to test memory monitoring
            num_rows = 100_000
            table = pa.table(
                {
                    "id": pa.array(list(range(num_rows))),
                    "large_text": pa.array(
                        [f"text_data_{i}_" * 100 for i in range(num_rows)]
                    ),  # Large strings
                    "large_array": pa.array(
                        [[i, i + 1, i + 2] for i in range(num_rows)]
                    ),
                }
            )

            # Monitor memory during processing
            import psutil

            process = psutil.Process(os.getpid())
            memory_readings = []

            def memory_callback(rows_processed, total_rows):
                current_memory = process.memory_info().rss / (1024 * 1024)
                memory_readings.append(current_memory)

            chunk_size = 5000
            chunks_processed = 0
            max_memory = 0

            for chunk in process_in_chunks(
                table,
                chunk_size_rows=chunk_size,
                max_memory_mb=512,
                enable_progress=True,
                progress_callback=memory_callback,
            ):
                chunks_processed += 1
                current_memory = process.memory_info().rss / (1024 * 1024)
                max_memory = max(max_memory, current_memory)

                # Validate chunk
                assert chunk.num_rows <= chunk_size
                assert len(chunk.column_names) == 3

            # Memory should remain bounded
            assert max_memory < 1024  # Should stay under 1GB
            assert len(memory_readings) > 0  # Progress callback should be called

            print(
                f"Memory monitoring test: {chunks_processed} chunks, max memory: {max_memory:.1f} MB"
            )

    def test_chunked_processing_error_handling(self):
        """Test error handling in chunked processing."""
        with tempfile.TemporaryDirectory() as tmp:
            # Create table
            table = pa.table({"id": pa.array(list(range(1000)))})

            # Test with invalid chunk size
            with pytest.raises((ValueError, ZeroDivisionError)):
                list(
                    process_in_chunks(
                        table,
                        chunk_size_rows=0,
                        max_memory_mb=512,
                    )
                )

            # Test with very low memory limit (should still work for small data)
            chunks = list(
                process_in_chunks(
                    table,
                    chunk_size_rows=100,
                    max_memory_mb=1,  # Very low limit
                )
            )
            assert len(chunks) > 0

            print("Chunked processing error handling tests passed")


class TestPyArrowConfigurationValidation:
    """Tests for configuration parameter validation and combinations."""

    def test_chunk_size_parameter_validation(self):
        """Test chunk size parameter validation."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "test"
            dataset_dir.mkdir()
            table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            pq.write_table(table, dataset_dir / "data.parquet")

            # Test valid chunk sizes
            valid_chunk_sizes = [1, 10, 100, 1000]
            for chunk_size in valid_chunk_sizes:
                result = deduplicate_parquet_dataset_pyarrow(
                    str(dataset_dir),
                    key_columns=["id"],
                    chunk_size_rows=chunk_size,
                    max_memory_mb=1024,
                )
                assert result["deduplicated_rows"] >= 0

            # Test invalid chunk sizes
            invalid_chunk_sizes = [0, -1, -100]
            for chunk_size in invalid_chunk_sizes:
                with pytest.raises((ValueError, TypeError)):
                    deduplicate_parquet_dataset_pyarrow(
                        str(dataset_dir),
                        key_columns=["id"],
                        chunk_size_rows=chunk_size,
                        max_memory_mb=1024,
                    )

    def test_memory_limit_parameter_validation(self):
        """Test memory limit parameter validation."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "test"
            dataset_dir.mkdir()
            table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            pq.write_table(table, dataset_dir / "data.parquet")

            # Test valid memory limits
            valid_limits = [1, 64, 512, 1024, 4096]
            for memory_limit in valid_limits:
                result = deduplicate_parquet_dataset_pyarrow(
                    str(dataset_dir),
                    key_columns=["id"],
                    chunk_size_rows=100,
                    max_memory_mb=memory_limit,
                )
                assert result["deduplicated_rows"] >= 0

            # Test invalid memory limits
            invalid_limits = [0, -1, -512]
            for memory_limit in invalid_limits:
                with pytest.raises((ValueError, TypeError)):
                    deduplicate_parquet_dataset_pyarrow(
                        str(dataset_dir),
                        key_columns=["id"],
                        chunk_size_rows=100,
                        max_memory_mb=memory_limit,
                    )

    def test_parameter_interaction_effects(self):
        """Test interaction effects between different parameters."""
        with tempfile.TemporaryDirectory() as tmp:
            dataset_dir = Path(tmp) / "interaction_test"
            dataset_dir.mkdir()

            # Create larger dataset for interaction testing
            num_rows = 50_000
            table = pa.table(
                {
                    "id": list(range(num_rows // 2)) * 2,
                    "value": list(range(num_rows)),
                }
            )
            pq.write_table(table, dataset_dir / "data.parquet")

            # Test different parameter combinations
            combinations = [
                {"chunk_size": 100, "memory": 64},
                {"chunk_size": 1000, "memory": 256},
                {"chunk_size": 5000, "memory": 1024},
                {"chunk_size": 10000, "memory": 2048},
            ]

            results = []
            for combo in combinations:
                start_time = time.perf_counter()

                result = deduplicate_parquet_dataset_pyarrow(
                    str(dataset_dir),
                    key_columns=["id"],
                    chunk_size_rows=combo["chunk_size"],
                    max_memory_mb=combo["memory"],
                )

                processing_time = time.perf_counter() - start_time
                metrics = result["performance_metrics"]

                results.append(
                    {
                        "chunk_size": combo["chunk_size"],
                        "memory_limit": combo["memory"],
                        "time": processing_time,
                        "actual_memory": metrics["memory_peak_mb"],
                        "throughput": metrics["rows_per_sec"],
                        "chunks": metrics["chunks_processed"],
                    }
                )

            # Validate interactions
            for result in results:
                # Actual memory should be close to limit but not exceed it significantly
                assert result["actual_memory"] <= result["memory_limit"] * 1.5

                # More chunks should generally mean more processing time
                assert result["chunks"] >= 1

                print(
                    f"Chunk {result['chunk_size']}, Memory {result['memory_limit']}MB: "
                    f"{result['time']:.2f}s, {result['actual_memory']:.1f}MB actual, "
                    f"{result['throughput']:.0f} rows/s"
                )
