"""Integration tests for PyArrow dataset operations.

These tests validate end-to-end functionality including:
1. Chunked processing end-to-end
2. Streaming merge operations
3. Performance metrics collection
4. Configuration parameters
"""

import os
import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsspeckit.datasets.pyarrow.dataset import (
    process_in_chunks,
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
            with tempfile.TemporaryDirectory():
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
        with tempfile.TemporaryDirectory():
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
        with tempfile.TemporaryDirectory():
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
