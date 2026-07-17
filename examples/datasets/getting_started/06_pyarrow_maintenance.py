"""
PyArrow Maintenance Operations - Getting Started

This example demonstrates the coordinator-backed dataset maintenance API
(introduced in 0.24.0 / 0.25.0) using filesystem-level facades.

The example covers:
1. Dataset stats collection
2. Planning a compaction (the typed plan replaces the old ``dry_run`` mode)
3. Executing the plan and reading the typed ``MaintenanceResult``
4. One-call convenience optimization

The maintenance facades are registered on every fsspec ``AbstractFileSystem``
when ``fsspeckit`` is imported. Two usage styles exist:

- Plan-then-execute: ``fs.plan_parquet_compaction(...)`` -> inspect the
  immutable plan -> ``fs.execute_maintenance_plan(plan)``
- One-call: ``fs.compact_parquet_dataset(...)`` / ``fs.optimize_parquet_dataset(...)``

See the migration guide ``docs/migration/maintenance-api.md`` for the mapping
from the removed dictionary-returning helpers.
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
    import fsspec

    import fsspeckit  # noqa: F401  (registers the filesystem maintenance facades)
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

    fs = fsspec.filesystem("file")
    table = create_sample_dataset()

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "maintenance_demo"

        print("\n📦 Creating fragmented dataset...")
        write_fragmented_dataset(table, dataset_path)

        stats_before = collect_dataset_stats_pyarrow(str(dataset_path))
        print_stats("📊 Stats before maintenance:", stats_before)

        # ------------------------------------------------------------------
        # 1. Plan-then-execute compaction
        #    The immutable plan replaces the removed ``dry_run=True`` mode:
        #    planning never touches the dataset.
        # ------------------------------------------------------------------
        print("\n🧱 Planning compaction (replaces dry_run)...")
        plan = fs.plan_parquet_compaction(
            str(dataset_path),
            target_rows_per_file=200,
        )
        print(f"  Plan type: {type(plan).__name__}")
        print(f"  Operation: {plan.operation.value}")
        print(f"  Guarantee level: {plan.guarantee_level.value}")
        print(f"  Source files: {len(plan.source_snapshot.files)}")
        print(f"  Compaction groups: {len(plan.compaction_groups)}")

        print("\n🚀 Executing compaction plan...")
        compact_result = fs.execute_maintenance_plan(plan)
        print(f"  Succeeded: {compact_result.succeeded}")
        if compact_result.actual_metrics is not None:
            metrics = compact_result.actual_metrics
            print(
                f"  Compaction: {len(plan.source_snapshot.files)} -> "
                f"{metrics.file_count} files ({metrics.row_count} rows)"
            )

        stats_after_compaction = collect_dataset_stats_pyarrow(str(dataset_path))
        print_stats("📊 Stats after compaction:", stats_after_compaction)

        # ------------------------------------------------------------------
        # 2. One-call convenience optimization (dedup + compaction)
        #    Re-fragment with a second copy of the table so the optimization
        #    has both duplicates to remove and files to combine.
        # ------------------------------------------------------------------
        print("\n📦 Re-fragmenting dataset with duplicate rows...")
        write_fragmented_dataset(table, dataset_path)
        stats_before_opt = collect_dataset_stats_pyarrow(str(dataset_path))
        print_stats("📊 Stats before optimization:", stats_before_opt)

        print("\n⚡ Running one-call optimization (dedup + compaction)...")
        optimize_result = fs.optimize_parquet_dataset(
            str(dataset_path),
            deduplicate_key_columns=["id"],
            target_rows_per_file=200,
            compression="snappy",
        )
        print(f"  Succeeded: {optimize_result.succeeded}")
        if optimize_result.actual_metrics is not None:
            metrics = optimize_result.actual_metrics
            print(
                f"  Optimization: {stats_before_opt['total_rows']} -> "
                f"{metrics.row_count} rows in {metrics.file_count} file(s) "
                f"({metrics.total_bytes} bytes)"
            )

        stats_after_opt = collect_dataset_stats_pyarrow(str(dataset_path))
        print_stats("📊 Stats after optimization:", stats_after_opt)

    print("\n✅ Maintenance operations completed successfully!")


if __name__ == "__main__":
    main()
