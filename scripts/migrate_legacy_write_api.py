#!/usr/bin/env python3
"""
Migration script to remove legacy write_parquet_dataset API.

This script helps identify and migrate code from the legacy API to the new API:
- write_parquet_dataset(..., strategy=...) -> merge(strategy=...)
- write_parquet_dataset(..., mode=...) -> write_dataset(mode=...)
"""

import re
import sys
from pathlib import Path


def analyze_usage(file_path: Path) -> dict:
    """Analyze usage of write_parquet_dataset in a file."""
    content = file_path.read_text()

    # Find all calls to write_parquet_dataset
    pattern = r"\.write_parquet_dataset\([^)]*\)"
    matches = re.findall(pattern, content)

    strategy_calls = []
    mode_calls = []
    simple_calls = []

    for match in matches:
        if "strategy=" in match:
            strategy_calls.append(match)
        elif "mode=" in match:
            mode_calls.append(match)
        else:
            simple_calls.append(match)

    return {
        "file": str(file_path),
        "strategy_calls": len(strategy_calls),
        "mode_calls": len(mode_calls),
        "simple_calls": len(simple_calls),
        "total": len(matches),
    }


def main():
    root = Path(__file__).parent.parent
    src_dir = root / "src"
    test_dir = root / "tests"

    print("Analyzing write_parquet_dataset usage...")
    print("=" * 60)

    all_files = list(src_dir.rglob("*.py")) + list(test_dir.rglob("*.py"))

    total_strategy = 0
    total_mode = 0
    total_simple = 0

    for file_path in all_files:
        result = analyze_usage(file_path)
        if result["total"] > 0:
            print(f"\n{result['file']}:")
            print(f"  Strategy calls: {result['strategy_calls']}")
            print(f"  Mode calls: {result['mode_calls']}")
            print(f"  Simple calls: {result['simple_calls']}")
            print(f"  Total: {result['total']}")

            total_strategy += result["strategy_calls"]
            total_mode += result["mode_calls"]
            total_simple += result["simple_calls"]

    print("\n" + "=" * 60)
    print(f"Total strategy calls: {total_strategy}")
    print(f"Total mode calls: {total_mode}")
    print(f"Total simple calls: {total_simple}")
    print(f"Grand total: {total_strategy + total_mode + total_simple}")

    print("\nMigration guide:")
    print(
        "- Strategy calls should be migrated to: handler.merge(data, path, strategy=...)"
    )
    print(
        "- Mode calls should be migrated to: handler.write_dataset(data, path, mode=...)"
    )
    print("- Simple calls should be migrated to: handler.write_dataset(data, path)")


if __name__ == "__main__":
    main()
