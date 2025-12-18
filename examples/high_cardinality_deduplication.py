"""
High-Cardinality Data Handling Examples

This script demonstrates practical examples of using AdaptiveKeyTracker
for processing high-cardinality datasets efficiently.
"""

import random
import string
import time
import os
import sys
from datetime import datetime, timedelta

# Add source path for development
sys.path.insert(0, "/Users/volker/coding/libs/fsspeckit/src")

# Initialize all dependencies
pa = None
pd = None
psutil = None
HAS_DEPS = False

# Try to import optional dependencies
try:
    import pyarrow as pa
    import pandas as pd
    import psutil

    HAS_DEPS = True
except ImportError as e:
    print(f"Warning: Some dependencies not available: {e}")
    print("Install with: pip install pyarrow pandas psutil pybloom-live")

# Try to import AdaptiveKeyTracker
HAS_TRACKER = False
AdaptiveKeyTracker = None

try:
    from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

    HAS_TRACKER = True
    print("AdaptiveKeyTracker imported successfully")
except ImportError as e:
    print(f"Warning: AdaptiveKeyTracker not available: {e}")
    print("Install with: pip install fsspeckit[pyarrow]")


def generate_clickstream_data(num_records=100_000, unique_users=50_000):
    """Generate realistic clickstream data with high cardinality."""

    if not HAS_DEPS or pd is None:
        print("Pandas not available for data generation")
        return None

    print(f"Generating {num_records:,} records with {unique_users:,} unique users...")

    # Generate user IDs
    user_ids = [f"user_{i:06d}" for i in range(unique_users)]

    data = []
    current_time = datetime.now()

    for i in range(num_records):
        user_id = random.choice(user_ids)
        session_id = f"{user_id}_session_{random.randint(1, 100)}"
        page_url = f"https://example.com/page/{random.randint(1, 5000)}"
        timestamp = current_time - timedelta(seconds=random.randint(0, 86400))

        record = {
            "user_id": user_id,
            "session_id": session_id,
            "page_url": page_url,
            "timestamp": timestamp.isoformat(),
            "event_type": random.choice(
                ["page_view", "click", "scroll", "form_submit"]
            ),
            "device_id": f"device_{random.randint(1, 50000)}",
        }

        data.append(record)

        if i % 20_000 == 0:
            print(f"Generated {i:,} records...")

    return pd.DataFrame(data)


def process_clickstream_with_deduplication(table, tracker):
    """Process clickstream data with session-level deduplication."""

    if not HAS_DEPS or not HAS_TRACKER or table is None:
        print("Dependencies not available")
        return None, {}

    print("Processing clickstream data...")
    start_time = time.time()

    # Get memory info if available
    start_memory = 0
    if psutil is not None:
        try:
            process = psutil.Process(os.getpid())
            start_memory = process.memory_info().rss
        except:
            pass

    unique_sessions = []
    total_processed = 0
    duplicates_found = 0

    # Process in batches for memory efficiency
    batch_size = 10_000
    num_batches = (len(table) + batch_size - 1) // batch_size

    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, len(table))
        batch = table.slice(start_idx, end_idx - start_idx)

        # Extract session composite keys
        user_ids = batch["user_id"].to_pylist()
        session_ids = batch["session_id"].to_pylist()
        timestamps = batch["timestamp"].to_pylist()

        for i, (user_id, session_id, timestamp) in enumerate(
            zip(user_ids, session_ids, timestamps)
        ):
            total_processed += 1

            # Create composite key for session deduplication
            # Use hourly granularity to reduce cardinality while maintaining session uniqueness
            hour_key = timestamp[:13]  # YYYY-MM-DDTHH
            composite_key = (user_id, session_id, hour_key)

            if composite_key not in tracker:
                tracker.add(composite_key)
                unique_sessions.append(batch[i])
            else:
                duplicates_found += 1

        # Report progress
        if (batch_idx + 1) % 5 == 0:
            current_time = time.time()

            metrics = tracker.get_metrics()

            print(f"Batch {batch_idx + 1}/{num_batches} complete:")
            print(f"  Processed: {total_processed:,} records")
            print(f"  Unique sessions: {len(unique_sessions):,}")
            print(f"  Duplicates: {duplicates_found:,}")
            print(f"  Current tier: {metrics['tier']}")
            print(f"  Time: {current_time - start_time:.1f}s")
            print()

    end_time = time.time()

    # Get end memory if available
    end_memory = 0
    if psutil is not None:
        try:
            process = psutil.Process(os.getpid())
            end_memory = process.memory_info().rss
        except:
            pass

    # Create result table
    result_table = pa.Table.from_arrays(unique_sessions, schema=table.schema)

    # Final metrics
    final_metrics = tracker.get_metrics()

    print("\n=== Processing Complete ===")
    print(f"Total records processed: {total_processed:,}")
    print(f"Unique sessions found: {len(unique_sessions):,}")
    print(f"Duplicates removed: {duplicates_found:,}")
    print(f"Deduplication rate: {duplicates_found / total_processed:.1%}")
    print(f"Final tracker tier: {final_metrics['tier']}")
    print(f"Tier transitions: {final_metrics['transitions']}")
    print(f"Processing time: {end_time - start_time:.2f}s")
    if end_memory > start_memory:
        print(f"Memory used: {(end_memory - start_memory) / 1024 / 1024:.1f}MB")
    print(f"Records/sec: {total_processed / (end_time - start_time):.0f}")

    return result_table, final_metrics


def compare_tracker_configurations(data_table, key_columns):
    """Compare performance across different tracker configurations."""

    if not HAS_TRACKER or data_table is None:
        print("Dependencies not available")
        return {}

    configurations = {
        "memory_constrained": {
            "max_exact_keys": 1_000,
            "max_lru_keys": 10_000,
            "false_positive_rate": 0.01,
        },
        "balanced": {
            "max_exact_keys": 10_000,
            "max_lru_keys": 100_000,
            "false_positive_rate": 0.001,
        },
        "high_accuracy": {
            "max_exact_keys": 25_000,
            "max_lru_keys": 250_000,
            "false_positive_rate": 0.0001,
        },
    }

    results = {}

    for config_name, config in configurations.items():
        print(f"\n=== Testing {config_name} configuration ===")

        tracker = AdaptiveKeyTracker(**config)
        start_time = time.time()

        unique_records = []
        duplicates_found = 0

        # Extract keys
        key_arrays = [data_table[col] for col in key_columns]

        for i in range(len(data_table)):
            # Create composite key
            key_values = tuple(arr[i].as_py() for arr in key_arrays)
            composite_key = key_values

            if composite_key not in tracker:
                tracker.add(composite_key)
                unique_records.append(i)
            else:
                duplicates_found += 1

        end_time = time.time()

        metrics = tracker.get_metrics()

        results[config_name] = {
            "config": config,
            "total_processed": len(data_table),
            "unique_found": len(unique_records),
            "duplicates": duplicates_found,
            "dedup_rate": duplicates_found / len(data_table),
            "final_tier": metrics["tier"],
            "transitions": metrics["transitions"],
            "processing_time": end_time - start_time,
            "records_per_sec": len(data_table) / (end_time - start_time),
            "metrics": metrics,
        }

        print(f"Configuration: {config}")
        print(f"  Total processed: {len(data_table):,}")
        print(f"  Unique found: {len(unique_records):,}")
        print(f"  Duplicates: {duplicates_found:,}")
        print(f"  Dedup rate: {duplicates_found / len(data_table):.1%}")
        print(f"  Final tier: {metrics['tier']}")
        print(f"  Transitions: {metrics['transitions']}")
        print(f"  Processing time: {end_time - start_time:.2f}s")
        print(f"  Records/sec: {len(data_table) / (end_time - start_time):.0f}")

    return results


def analyze_comparison_results(results):
    """Analyze and compare tracker configuration results."""

    if not results:
        print("No results to analyze")
        return

    print("\n" + "=" * 60)
    print("CONFIGURATION COMPARISON ANALYSIS")
    print("=" * 60)

    # Create comparison table
    print(
        f"{'Configuration':<20} {'Tier':<10} {'Time(s)':<8} {'Records/sec':<12} {'Dedup%':<8}"
    )
    print("-" * 70)

    for config_name, result in results.items():
        print(
            f"{config_name:<20} {result['final_tier']:<10} "
            f"{result['processing_time']:<8.2f} {result['records_per_sec']:<12.0f} "
            f"{result['dedup_rate'] * 100:<8.1f}"
        )

    print("\n" + "=" * 60)
    print("PERFORMANCE ANALYSIS")
    print("=" * 60)

    # Sort by processing speed
    sorted_by_speed = sorted(
        results.items(), key=lambda x: x[1]["records_per_sec"], reverse=True
    )

    print("Fastest Processing:")
    for i, (config_name, result) in enumerate(sorted_by_speed[:2]):
        print(f"  {i + 1}. {config_name}: {result['records_per_sec']:.0f} records/sec")


def main():
    """Main demonstration function."""

    print("High-Cardinality Data Handling with AdaptiveKeyTracker")
    print("=" * 60)

    if not HAS_TRACKER:
        print("AdaptiveKeyTracker not available. Install with:")
        print("pip install fsspeckit[pyarrow]")
        return

    # Example 1: Clickstream data processing
    print("\n=== Example 1: Clickstream Deduplication ===")

    # Configure tracker for clickstream data
    clickstream_tracker = AdaptiveKeyTracker(
        max_exact_keys=5_000,  # Small exact tier for very recent events
        max_lru_keys=50_000,  # Medium LRU for recent events
        false_positive_rate=0.001,  # Good accuracy for historical data
    )

    print("Clickstream tracker configuration:")
    print(f"  Max exact keys: {clickstream_tracker.max_exact_keys:,}")
    print(f"  Max LRU keys: {clickstream_tracker.max_lru_keys:,}")
    print(f"  False positive rate: {clickstream_tracker.false_positive_rate}")

    # Generate and process smaller dataset for demo
    if HAS_DEPS:
        clickstream_df = generate_clickstream_data(50_000, 25_000)
        if clickstream_df is not None:
            clickstream_table = pa.Table.from_pandas(clickstream_df)

            # Process with deduplication
            unique_clicks, clickstream_metrics = process_clickstream_with_deduplication(
                clickstream_table, clickstream_tracker
            )

    # Example 2: Configuration comparison
    print("\n=== Example 2: Configuration Comparison ===")

    if HAS_DEPS:
        # Use sample data for comparison
        sample_data = pa.Table.from_pydict(
            {
                "user_id": [f"user_{i}" for i in range(10000)],
                "session_id": [f"session_{i}" for i in range(10000)],
            }
        )

        comparison_results = compare_tracker_configurations(
            sample_data, ["user_id", "session_id"]
        )

        analyze_comparison_results(comparison_results)

    print("\n=== Summary ===")
    print("Key takeaways:")
    print("1. Configuration matters for different use cases")
    print("2. Monitor tier transitions to understand data patterns")
    print("3. Balance memory usage vs accuracy requirements")
    print("4. Test with representative data for optimal configuration")
    print("5. Install pybloom-live for best performance with high cardinality")


if __name__ == "__main__":
    main()
