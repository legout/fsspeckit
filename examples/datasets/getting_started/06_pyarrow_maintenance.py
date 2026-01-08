"""
PyArrow Maintenance Operations - Getting Started

This example demonstrates dataset maintenance workflows using PyArrow utilities.

The example covers:
1. Dataset stats collection
2. Compaction to reduce file counts
3. Optimization for better query performance
"""

from __future__ import annotations

import tempfile
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing dependency 'pyarrow'. Install with: pip install -e \".[datasets]\" "
        "(or run `uv sync` then `uv run python ...`)."
    ) from exc

try:
    from fsspeckit.datasets import (
        compact_parquet_dataset_pyarrow,
        optimize_parquet_dataset_pyarrow,
    )
    from fsspeckit.datasets.pyarrow import collect_dataset_stats_pyarrow
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing fsspeckit dataset dependencies. Install with: pip install -e \".[datasets]\" "
        "(or run `uv sync` then `uv run python ...`)."
    ) from exc


def create_sample_dataset() -> pa.Table:
    """Create a small table for maintenance demos."""
    data = {
        "id": [f"item_{i:03d}" for i in range(200)],
        "category": ["A" if i % 2 == 0 else "B" for i in range(200)],
        "value": [float(i % 25) for i in range(200)],
    }
    return pa.Table.from_pydict(data)


def write_fragmented_dataset(table: pa.Table, path: Path) -> None:
    """Write many small parquet files to simulate fragmentation."""
    path.mkdir(parents=True, exist_ok=True)
    chunk_size = 25
    for i in range(0, table.num_rows, chunk_size):
        chunk = table.slice(i, chunk_size)
        pq.write_table(chunk, path / f"chunk_{i // chunk_size:03d}.parquet")


def print_stats(label: str, stats: dict) -> None:
    """Print a summary of dataset stats."""
    print(f"\n{label}")
    print(f"  Files: {len(stats['files'])}")
    print(f"  Total rows: {stats['total_rows']}")
    print(f"  Total bytes: {stats['total_bytes']}")


def main() -> None:
    print("🧹 PyArrow Maintenance Operations - Getting Started")
    print("=" * 60)

    table = create_sample_dataset()

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "maintenance_demo"

        print("\n📦 Creating fragmented dataset...")
        write_fragmented_dataset(table, dataset_path)

        stats_before = collect_dataset_stats_pyarrow(str(dataset_path))
        print_stats("📊 Stats before maintenance:", stats_before)

        print("\n🧱 Running compaction...")
        compact_result = compact_parquet_dataset_pyarrow(
            str(dataset_path),
            target_rows_per_file=200,
        )
        print(
            f"  Compaction: {compact_result['before_file_count']} -> {compact_result['after_file_count']} files"
        )

        stats_after_compaction = collect_dataset_stats_pyarrow(str(dataset_path))
        print_stats("📊 Stats after compaction:", stats_after_compaction)

        print("\n⚡ Running optimization...")
        optimize_result = optimize_parquet_dataset_pyarrow(
            str(dataset_path),
            target_rows_per_file=200,
            compression="snappy",
        )
        print(
            f"  Optimization: {optimize_result['before_file_count']} -> {optimize_result['after_file_count']} files"
        )

        stats_after_opt = collect_dataset_stats_pyarrow(str(dataset_path))
        print_stats("📊 Stats after optimization:", stats_after_opt)

    print("\n✅ Maintenance operations completed successfully!")


if __name__ == "__main__":
    main()
