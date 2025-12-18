"""Performance tests for PyArrow dataset operations."""

import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fsspeckit.datasets.pyarrow.dataset import deduplicate_parquet_dataset_pyarrow


@pytest.fixture
def large_sample_dataset():
    """Create a moderately large dataset with duplicates for performance testing."""
    with tempfile.TemporaryDirectory() as tmp:
        dataset_dir = Path(tmp) / "large_dataset"
        dataset_dir.mkdir()

        # 100,000 rows, some duplicates
        num_rows = 100_000
        ids = list(range(num_rows // 2)) * 2  # Each ID appears twice
        values = [float(i) for i in range(num_rows)]
        timestamps = [int(time.time()) + i for i in range(num_rows)]

        table = pa.table({"id": ids, "value": values, "timestamp": timestamps})

        # Write as multiple files
        num_files = 5
        rows_per_file = num_rows // num_files
        for i in range(num_files):
            chunk = table.slice(i * rows_per_file, rows_per_file)
            pq.write_table(chunk, dataset_dir / f"part_{i}.parquet")

        yield str(dataset_dir)


def test_deduplicate_performance_and_metrics(large_sample_dataset):
    """Test that deduplication returns expected metrics and performs well."""

    # Run deduplication with metrics collection
    result = deduplicate_parquet_dataset_pyarrow(
        large_sample_dataset,
        key_columns=["id"],
        dedup_order_by=["-timestamp"],
        verbose=True,
        chunk_size_rows=20_000,  # Force chunked processing
    )

    # Verify results
    assert "performance_metrics" in result
    metrics = result["performance_metrics"]

    # Check for all required fields
    required_fields = [
        "total_process_time_sec",
        "memory_peak_mb",
        "throughput_mb_sec",
        "rows_per_sec",
        "files_processed",
        "chunks_processed",
        "dedup_efficiency",
        "operation_breakdown",
    ]
    for field in required_fields:
        assert field in metrics, f"Missing metric field: {field}"

    # Verify basic correctness
    assert result["deduplicated_rows"] == 50_000
    assert metrics["files_processed"] == 5
    assert metrics["chunks_processed"] > 0
    assert metrics["dedup_efficiency"] == 0.5  # 50% removed
    assert isinstance(metrics["operation_breakdown"], dict)
    assert len(metrics["operation_breakdown"]) > 0


def test_deduplicate_exact_performance(large_sample_dataset):
    """Test performance of exact deduplication (no key columns)."""

    # Add some exact duplicates
    table = pq.read_table(large_sample_dataset)
    pq.write_table(table, Path(large_sample_dataset) / "exact_dup.parquet")

    result = deduplicate_parquet_dataset_pyarrow(
        large_sample_dataset, key_columns=None, verbose=True, chunk_size_rows=20_000
    )

    # We added 100,000 rows as exact duplicates, so 100,000 rows should be removed
    assert result["deduplicated_rows"] == 100_000
    assert "performance_metrics" in result
