"""DuckDB dataset maintenance: compaction workflows.

This script demonstrates how to inspect, dry-run, and execute compaction
against a parquet dataset using DuckDBParquetHandler.compact_parquet_dataset().
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa

from fsspeckit.utils import DuckDBParquetHandler


def _seed_fragmented_dataset(handler: DuckDBParquetHandler, dataset_path: Path) -> None:
    """Create intentionally fragmented parquet files for demonstration."""
    batches = [
        pa.table({"order_id": list(range(i * 50, (i + 1) * 50)), "amount": [i] * 50})
        for i in range(8)
    ]
    for idx, batch in enumerate(batches):
        handler.write_parquet_dataset(
            batch,
            str(dataset_path),
            mode="append" if idx else "overwrite",
            max_rows_per_file=25,
            basename_template=f"orders-part-{idx}-{{}}.parquet",
        )


def run_compaction_example() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "orders"
        with DuckDBParquetHandler() as handler:
            _seed_fragmented_dataset(handler, dataset_path)

            print("Initial dataset files:")
            for path in sorted(dataset_path.glob("*.parquet")):
                print(" -", path.name)

            dry_plan = handler.compact_parquet_dataset(
                path=str(dataset_path),
                target_mb_per_file=1,
                dry_run=True,
            )
            print("\nDry-run preview:")
            print(" before", dry_plan["before"])
            print(" first plan entry", dry_plan["plan"][0])

            stats = handler.compact_parquet_dataset(
                path=str(dataset_path),
                target_rows_per_file=200,
                compression="zstd",
                max_files_per_group=4,
            )
            print("\nCompaction complete:")
            print(stats["before"], "->", stats["after"])

            recompute = handler.compact_parquet_dataset(
                path=str(dataset_path),
                target_rows_per_file=200,
                dry_run=True,
                partition_filter=None,
            )
            assert recompute["before"]["file_count"] == stats["after"]["file_count"]
            print("\nVerification dry-run confirms stabilized file count.")

if __name__ == "__main__":
    run_compaction_example()
