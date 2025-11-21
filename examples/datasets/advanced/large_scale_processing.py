"""
Large-Scale Data Processing Example

This advanced-level example demonstrates enterprise-grade large-scale data processing
capabilities using fsspeckit's dataset utilities for massive datasets.

The example covers:
1. Distributed processing patterns for big data
2. Memory management for terabyte-scale datasets
3. Parallel processing optimization strategies
4. Data sharding and partitioning strategies
5. Performance monitoring and resource management
6. Fault tolerance and recovery mechanisms
7. Batch vs. streaming processing decisions

This example shows how to build production-grade large-scale data
processing pipelines using fsspeckit.
"""

from __future__ import annotations

import asyncio
import gc
import multiprocessing
import os
import tempfile
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Iterator, Union

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.dataset as ds

# Additional libraries for large-scale processing
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

from fsspeckit.datasets import DuckDBParquetHandler


@dataclass
class ProcessingConfig:
    """Configuration for large-scale processing operations."""

    # Memory management
    max_memory_mb: int = 8192  # 8GB default
    chunk_size_mb: int = 512   # 512MB chunks

    # Parallel processing
    max_workers: Optional[int] = None  # Auto-detect if None
    use_multiprocessing: bool = True

    # I/O optimization
    batch_size: int = 10000
    compression: str = "snappy"

    # Performance tuning
    enable_profiling: bool = True
    checkpoint_interval: int = 100000  # Records

    # Fault tolerance
    max_retries: int = 3
    retry_delay: float = 1.0
    enable_checkpoints: bool = True


@dataclass
class ProcessingMetrics:
    """Metrics for monitoring processing performance."""

    start_time: float
    records_processed: int = 0
    records_per_second: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    disk_io_mb: float = 0.0
    errors: int = 0
    warnings: int = 0

    def update_performance(self):
        """Update real-time performance metrics."""
        if PSUTIL_AVAILABLE:
            process = psutil.Process()
            self.memory_usage_mb = process.memory_info().rss / 1024 / 1024
            self.cpu_usage_percent = process.cpu_percent()

        elapsed = time.time() - self.start_time
        if elapsed > 0:
            self.records_per_second = self.records_processed / elapsed

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        return {
            "records_processed": self.records_processed,
            "records_per_second": self.records_per_second,
            "memory_usage_mb": self.memory_usage_mb,
            "cpu_usage_percent": self.cpu_usage_percent,
            "errors": self.errors,
            "warnings": self.warnings,
            "elapsed_time": time.time() - self.start_time
        }


class MemoryManager:
    """Advanced memory management for large-scale processing."""

    def __init__(self, config: ProcessingConfig):
        self.config = config
        self.memory_threshold = config.max_memory_mb * 0.8  # 80% threshold

    def check_memory_usage(self) -> bool:
        """Check if memory usage is within acceptable limits."""
        if not PSUTIL_AVAILABLE:
            return True

        current_memory = psutil.virtual_memory().used / 1024 / 1024
        return current_memory < self.memory_threshold

    def trigger_gc(self):
        """Trigger garbage collection and memory cleanup."""
        gc.collect()

        # Force Arrow cleanup
        pa.default_memory_pool().release_unused()

    def get_optimal_chunk_size(self, dataset_size_mb: float) -> int:
        """Calculate optimal chunk size based on dataset size and available memory."""
        available_memory = self.config.max_memory_mb * 0.5  # Use 50% of max

        # Aim for 8-16 chunks to balance parallelism and memory
        optimal_chunks = min(16, max(8, available_memory // self.config.chunk_size_mb))

        return int(dataset_size_mb / optimal_chunks)

    def monitor_and_cleanup(self):
        """Monitor memory and perform cleanup if necessary."""
        if not self.check_memory_usage():
            print("⚠️ High memory usage detected, triggering cleanup")
            self.trigger_gc()

            # If still high, recommend reducing parallelism
            if not self.check_memory_usage():
                print("🚨 Memory still high after cleanup, consider reducing worker count")
                return False

        return True


class DataPartitioner:
    """Handles data partitioning strategies for large-scale processing."""

    @staticmethod
    def partition_by_hash(data: pa.Table, hash_columns: List[str], num_partitions: int) -> List[pa.Table]:
        """Partition data using hash-based strategy."""
        if not hash_columns:
            return [data]

        # Create hash key
        hash_values = pa.compute.hash_combine(
            [pa.compute.hash(data.column(col)) for col in hash_columns]
        )

        # Partition based on hash
        partition_indices = hash_values.to_pylist()
        partitions = [[] for _ in range(num_partitions)]

        for i, partition_idx in enumerate(partition_indices):
            partitions[partition_idx % num_partitions].append(i)

        # Create partitioned tables
        partitioned_tables = []
        for partition_indices in partitions:
            if partition_indices:
                partitioned_tables.append(data.take(pa.array(partition_indices)))

        return partitioned_tables

    @staticmethod
    def partition_by_range(data: pa.Table, range_column: str, num_partitions: int) -> List[pa.Table]:
        """Partition data using range-based strategy."""
        if range_column not in data.column_names:
            return [data]

        column_data = data.column(range_column)
        min_val, max_val = pc.min(column_data).as_py(), pc.max(column_data).as_py()

        if min_val == max_val:
            return [data]

        # Calculate ranges
        range_size = (max_val - min_val) / num_partitions
        partitions = []

        for i in range(num_partitions):
            range_min = min_val + (i * range_size)
            range_max = min_val + ((i + 1) * range_size) if i < num_partitions - 1 else max_val

            mask = pc.and_(
                pc.greater_equal(column_data, range_min),
                pc.less_equal(column_data, range_max)
            )

            partition = data.filter(mask)
            if len(partition) > 0:
                partitions.append(partition)

        return partitions if partitions else [data]

    @staticmethod
    def partition_by_size(data: pa.Table, target_size_mb: int) -> List[pa.Table]:
        """Partition data by target size."""
        data_size_mb = data.nbytes / 1024 / 1024
        if data_size_mb <= target_size_mb:
            return [data]

        num_partitions = max(1, int(data_size_mb / target_size_mb))
        rows_per_partition = len(data) // num_partitions

        partitions = []
        for i in range(num_partitions):
            start_idx = i * rows_per_partition
            end_idx = start_idx + rows_per_partition if i < num_partitions - 1 else len(data)
            partitions.append(data.slice(start_idx, end_idx - start_idx))

        return partitions


class LargeScaleProcessor:
    """Main processor for large-scale data operations."""

    def __init__(self, config: ProcessingConfig):
        self.config = config
        self.memory_manager = MemoryManager(config)
        self.metrics = ProcessingMetrics(start_time=time.time())

        # Auto-detect optimal worker count
        if config.max_workers is None:
            cpu_count = multiprocessing.cpu_count()
            self.config.max_workers = min(cpu_count, 16)  # Cap at 16 for stability

        print(f"🚀 Large-Scale Processor initialized")
        print(f"   Max workers: {self.config.max_workers}")
        print(f"   Memory limit: {self.config.max_memory_mb}MB")
        print(f"   Chunk size: {self.config.chunk_size_mb}MB")

    def process_dataset_parallel(
        self,
        dataset_path: str,
        processing_func: callable,
        output_path: str,
        partition_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Process a large dataset using parallel processing."""

        print(f"📊 Starting parallel processing of {dataset_path}")

        try:
            # Load dataset metadata
            dataset = ds.dataset(dataset_path)
            total_size = self._calculate_dataset_size(dataset)

            print(f"   Dataset size: {total_size / 1024 / 1024:.1f}MB")

            # Determine partitioning strategy
            if partition_columns:
                partitions = self._create_partitions(dataset, partition_columns)
            else:
                partitions = self._create_size_based_partitions(dataset)

            print(f"   Created {len(partitions)} partitions")

            # Process partitions in parallel
            results = self._process_partitions_parallel(
                partitions, processing_func, output_path
            )

            # Combine results
            final_result = self._combine_results(results, output_path)

            self.metrics.update_performance()
            return {
                "status": "completed",
                "metrics": self.metrics.get_summary(),
                "result_path": final_result,
                "partitions_processed": len(partitions)
            }

        except Exception as e:
            self.metrics.errors += 1
            print(f"❌ Error in parallel processing: {e}")
            traceback.print_exc()
            raise

    def _calculate_dataset_size(self, dataset: ds.Dataset) -> int:
        """Calculate total dataset size in bytes."""
        total_size = 0
        for fragment in dataset.get_fragments():
            for file in fragment.files:
                if Path(file).exists():
                    total_size += Path(file).stat().st_size
        return total_size

    def _create_partitions(self, dataset: ds.Dataset, partition_columns: List[str]) -> List[pa.Table]:
        """Create partitions based on specified columns."""
        print(f"   Partitioning by columns: {partition_columns}")

        # For demonstration, load a sample to understand partitioning
        sample_table = dataset.head(1000)

        if len(partition_columns) == 1:
            return DataPartitioner.partition_by_range(
                sample_table, partition_columns[0], self.config.max_workers
            )
        else:
            return DataPartitioner.partition_by_hash(
                sample_table, partition_columns, self.config.max_workers
            )

    def _create_size_based_partitions(self, dataset: ds.Dataset) -> List[pa.Table]:
        """Create partitions based on size for optimal processing."""
        dataset_size = self._calculate_dataset_size(dataset)
        target_chunk_size = self.memory_manager.get_optimal_chunk_size(
            dataset_size / 1024 / 1024
        ) * 1024 * 1024

        print(f"   Target chunk size: {target_chunk_size / 1024 / 1024:.1f}MB")

        partitions = []
        batch_reader = dataset.to_batches(batch_size=target_chunk_size)

        for batch in batch_reader:
            table = pa.Table.from_batches([batch])
            partitions.append(table)

            # Check memory usage
            if not self.memory_manager.monitor_and_cleanup():
                break

        return partitions

    def _process_partitions_parallel(
        self,
        partitions: List[pa.Table],
        processing_func: callable,
        output_path: str
    ) -> List[str]:
        """Process partitions in parallel using the optimal execution strategy."""

        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        results = []

        # Choose execution strategy based on configuration
        if self.config.use_multiprocessing:
            results = self._process_with_multiprocessing(
                partitions, processing_func, output_dir
            )
        else:
            results = self._process_with_threading(
                partitions, processing_func, output_dir
            )

        return results

    def _process_with_multiprocessing(
        self,
        partitions: List[pa.Table],
        processing_func: callable,
        output_dir: Path
    ) -> List[str]:
        """Process partitions using multiprocessing."""

        print(f"   Using multiprocessing with {self.config.max_workers} workers")

        results = [None] * len(partitions)

        with ProcessPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(
                    self._process_single_partition,
                    partition,
                    processing_func,
                    str(output_dir),
                    i
                ): i
                for i, partition in enumerate(partitions)
            }

            # Collect results as they complete
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    result_path = future.result()
                    results[index] = result_path
                    self.metrics.records_processed += len(partitions[index])

                    if index % 10 == 0:
                        print(f"   Completed partition {index + 1}/{len(partitions)}")

                except Exception as e:
                    self.metrics.errors += 1
                    print(f"   Error processing partition {index}: {e}")
                    results[index] = None

        return [r for r in results if r is not None]

    def _process_with_threading(
        self,
        partitions: List[pa.Table],
        processing_func: callable,
        output_dir: Path
    ) -> List[str]:
        """Process partitions using threading."""

        print(f"   Using threading with {self.config.max_workers} workers")

        results = []

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all tasks
            futures = [
                executor.submit(
                    self._process_single_partition,
                    partition,
                    processing_func,
                    str(output_dir),
                    i
                )
                for i, partition in enumerate(partitions)
            ]

            # Collect results as they complete
            for i, future in enumerate(futures):
                try:
                    result_path = future.result()
                    results.append(result_path)
                    self.metrics.records_processed += len(partitions[i])

                    if i % 10 == 0:
                        print(f"   Completed partition {i + 1}/{len(partitions)}")

                except Exception as e:
                    self.metrics.errors += 1
                    print(f"   Error processing partition {i}: {e}")
                    results.append(None)

        return [r for r in results if r is not None]

    def _process_single_partition(
        self,
        partition: pa.Table,
        processing_func: callable,
        output_dir: str,
        partition_id: int
    ) -> str:
        """Process a single partition and save to disk."""

        try:
            # Apply processing function
            processed_data = processing_func(partition)

            # Save to output file
            output_file = Path(output_dir) / f"partition_{partition_id:04d}.parquet"

            with pq.ParquetWriter(output_file, processed_data.schema, compression=self.config.compression) as writer:
                writer.write_table(processed_data)

            return str(output_file)

        except Exception as e:
            print(f"   Error in partition {partition_id}: {e}")
            raise

    def _combine_results(self, result_files: List[str], output_path: str) -> str:
        """Combine processed partitions into a final dataset."""

        print(f"   Combining {len(result_files)} processed partitions")

        if not result_files:
            raise ValueError("No valid results to combine")

        # Create combined dataset
        combined_table = []

        for file_path in result_files:
            try:
                table = pq.read_table(file_path)
                combined_table.append(table)
            except Exception as e:
                print(f"   Warning: Could not read {file_path}: {e}")

        if not combined_table:
            raise ValueError("No valid data to combine")

        # Concatenate all tables
        final_table = pa.concat_tables(combined_table)

        # Save final result
        final_output = Path(output_path) / "combined_result.parquet"
        pq.write_table(final_table, final_output, compression=self.config.compression)

        # Clean up individual partition files
        for file_path in result_files:
            try:
                Path(file_path).unlink()
            except:
                pass  # Ignore cleanup errors

        print(f"   Final result saved to: {final_output}")
        return str(final_output)

    def create_checkpoint(self, data: pa.Table, checkpoint_id: int) -> str:
        """Create a checkpoint for fault tolerance."""
        if not self.config.enable_checkpoints:
            return ""

        checkpoint_dir = Path("checkpoints")
        checkpoint_dir.mkdir(exist_ok=True)

        checkpoint_file = checkpoint_dir / f"checkpoint_{checkpoint_id:06d}.parquet"
        pq.write_table(data, checkpoint_file, compression=self.config.compression)

        return str(checkpoint_file)


def create_sample_large_dataset(output_path: str, size_mb: int = 1024) -> str:
    """Create a sample large dataset for testing."""

    print(f"📝 Creating sample dataset ({size_mb}MB)...")

    target_rows = int(size_mb * 1000)  # Rough estimation
    batch_size = 50000

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Sample schema
    schema = pa.schema([
        ("id", pa.int64()),
        ("timestamp", pa.timestamp("ms")),
        ("user_id", pa.int64()),
        ("event_type", pa.string()),
        ("session_id", pa.string()),
        ("ip_address", pa.string()),
        ("user_agent", pa.string()),
        ("value", pa.float64()),
        ("currency", pa.string()),
        ("country", pa.string()),
        ("device_type", pa.string()),
        ("browser", pa.string()),
        ("os", pa.string()),
        ("referrer", pa.string()),
        ("campaign_id", pa.string()),
        ("conversion", pa.bool_()),
        ("revenue", pa.float64())
    ])

    batch_count = 0
    total_written = 0

    with pq.ParquetWriter(str(output_dir / "large_dataset.parquet"), schema) as writer:

        while total_written < target_rows:
            # Generate batch data
            batch_size_actual = min(batch_size, target_rows - total_written)

            batch_data = {
                "id": pa.array(range(total_written, total_written + batch_size_actual)),
                "timestamp": pa.array([
                    datetime.now() - timedelta(minutes=i)
                    for i in range(batch_size_actual)
                ]),
                "user_id": pa.array([hash(f"user_{i}") % 100000 for i in range(batch_size_actual)]),
                "event_type": pa.array([f"event_{i % 10}" for i in range(batch_size_actual)]),
                "session_id": pa.array([f"session_{i % 1000}" for i in range(batch_size_actual)]),
                "ip_address": pa.array([f"192.168.{i % 255}.{(i * 7) % 255}" for i in range(batch_size_actual)]),
                "user_agent": pa.array([f"Agent_{i % 50}" for i in range(batch_size_actual)]),
                "value": pa.array([i * 1.5 for i in range(batch_size_actual)]),
                "currency": pa.array(["USD" if i % 3 == 0 else "EUR" if i % 3 == 1 else "GBP" for i in range(batch_size_actual)]),
                "country": pa.array([f"Country_{i % 50}" for i in range(batch_size_actual)]),
                "device_type": pa.array(["Desktop" if i % 3 == 0 else "Mobile" if i % 3 == 1 else "Tablet" for i in range(batch_size_actual)]),
                "browser": pa.array([f"Browser_{i % 20}" for i in range(batch_size_actual)]),
                "os": pa.array([f"OS_{i % 10}" for i in range(batch_size_actual)]),
                "referrer": pa.array([f"Referrer_{i % 100}" for i in range(batch_size_actual)]),
                "campaign_id": pa.array([f"Campaign_{i % 25}" for i in range(batch_size_actual)]),
                "conversion": pa.array([i % 10 == 0 for i in range(batch_size_actual)]),
                "revenue": pa.array([i * 2.5 if i % 10 == 0 else 0.0 for i in range(batch_size_actual)])
            }

            batch_table = pa.Table.from_pydict(batch_data, schema=schema)
            writer.write_table(batch_table)

            total_written += batch_size_actual
            batch_count += 1

            if batch_count % 10 == 0:
                print(f"   Written {total_written:,} records...")

    final_path = output_dir / "large_dataset.parquet"
    actual_size = final_path.stat().st_size / 1024 / 1024

    print(f"✅ Sample dataset created: {actual_size:.1f}MB, {total_written:,} records")
    return str(final_path)


def sample_processing_function(data: pa.Table) -> pa.Table:
    """Sample processing function for demonstration."""

    # Add derived columns
    processed_data = data.append_column(
        "revenue_per_user",
        pc.divide(
            data.column("revenue"),
            pc.cast(data.column("user_id"), pa.float64())
        )
    )

    # Filter high-value events
    high_value_mask = pc.greater(data.column("revenue"), 100.0)
    filtered_data = processed_data.filter(high_value_mask)

    # Add timestamp components
    timestamp_col = processed_data.column("timestamp")
    processed_data = processed_data.append_column(
        "hour_of_day",
        pc.hour(timestamp_col)
    ).append_column(
        "day_of_week",
        pc.day_of_week(timestamp_col)
    )

    return processed_data


def demonstrate_large_scale_processing():
    """Demonstrate large-scale processing capabilities."""

    print("\n🏗️ Large-Scale Processing Demonstration")
    print("=" * 60)

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Configuration for large-scale processing
        config = ProcessingConfig(
            max_memory_mb=4096,  # 4GB
            chunk_size_mb=256,    # 256MB chunks
            max_workers=4,
            use_multiprocessing=True,
            batch_size=50000,
            enable_profiling=True,
            enable_checkpoints=True
        )

        # Create sample dataset
        dataset_path = create_sample_large_dataset(str(temp_dir / "data"), size_mb=1024)

        # Initialize processor
        processor = LargeScaleProcessor(config)

        print(f"\n📊 Processing Configuration:")
        print(f"   Memory limit: {config.max_memory_mb}MB")
        print(f"   Chunk size: {config.chunk_size_mb}MB")
        print(f"   Workers: {config.max_workers}")
        print(f"   Processing type: {'Multiprocessing' if config.use_multiprocessing else 'Threading'}")

        # Process dataset
        print(f"\n🔄 Starting large-scale processing...")
        output_path = str(temp_dir / "output")

        result = processor.process_dataset_parallel(
            dataset_path=dataset_path,
            processing_func=sample_processing_function,
            output_path=output_path,
            partition_columns=["country"]  # Partition by country for demonstration
        )

        # Display results
        print(f"\n✅ Processing completed successfully!")
        print(f"   Status: {result['status']}")
        print(f"   Result path: {result['result_path']}")
        print(f"   Partitions processed: {result['partitions_processed']}")

        metrics = result['metrics']
        print(f"\n📈 Performance Metrics:")
        print(f"   Records processed: {metrics['records_processed']:,}")
        print(f"   Processing rate: {metrics['records_per_second']:.1f} records/sec")
        print(f"   Peak memory: {metrics['memory_usage_mb']:.1f}MB")
        print(f"   CPU usage: {metrics['cpu_usage_percent']:.1f}%")
        print(f"   Errors: {metrics['errors']}")
        print(f"   Elapsed time: {metrics['elapsed_time']:.1f} seconds")

        # Verify output
        if Path(result['result_path']).exists():
            result_size = Path(result['result_path']).stat().st_size / 1024 / 1024
            result_table = pq.read_table(result['result_path'])
            print(f"\n📋 Output Summary:")
            print(f"   File size: {result_size:.1f}MB")
            print(f"   Records: {len(result_table):,}")
            print(f"   Columns: {len(result_table.schema)}")

    except Exception as e:
        print(f"❌ Large-scale processing demo failed: {e}")
        raise

    finally:
        import shutil
        shutil.rmtree(temp_dir)


def demonstrate_partitioning_strategies():
    """Demonstrate different partitioning strategies."""

    print("\n🎯 Partitioning Strategies Demonstration")

    # Create sample data
    sample_data = pa.table({
        "id": pa.array(range(100000)),
        "value": pa.array([i * 1.5 for i in range(100000)]),
        "category": pa.array([f"cat_{i % 10}" for i in range(100000)]),
        "region": pa.array([f"region_{i % 5}" for i in range(100000)]),
        "timestamp": pa.array([
            datetime.now() - timedelta(minutes=i)
            for i in range(100000)
        ])
    })

    print(f"   Sample data: {len(sample_data):,} records, {len(sample_data.schema)} columns")

    # Hash-based partitioning
    print(f"\n1. Hash-based Partitioning:")
    hash_partitions = DataPartitioner.partition_by_hash(
        sample_data, ["category", "region"], 8
    )
    print(f"   Partitions: {len(hash_partitions)}")
    for i, partition in enumerate(hash_partitions):
        print(f"     Partition {i}: {len(partition):,} records")

    # Range-based partitioning
    print(f"\n2. Range-based Partitioning:")
    range_partitions = DataPartitioner.partition_by_range(
        sample_data, "value", 5
    )
    print(f"   Partitions: {len(range_partitions)}")
    for i, partition in enumerate(range_partitions):
        min_val = pc.min(partition.column("value")).as_py()
        max_val = pc.max(partition.column("value")).as_py()
        print(f"     Partition {i}: {len(partition):,} records, range: {min_val:.1f} - {max_val:.1f}")

    # Size-based partitioning
    print(f"\n3. Size-based Partitioning:")
    size_partitions = DataPartitioner.partition_by_size(sample_data, 1024 * 1024)  # 1MB chunks
    print(f"   Partitions: {len(size_partitions)}")
    for i, partition in enumerate(size_partitions):
        size_mb = partition.nbytes / 1024 / 1024
        print(f"     Partition {i}: {len(partition):,} records, {size_mb:.2f}MB")


def demonstrate_memory_management():
    """Demonstrate advanced memory management techniques."""

    print("\n🧠 Memory Management Demonstration")

    if not PSUTIL_AVAILABLE:
        print("   ⚠️ psutil not available, memory monitoring disabled")
        return

    config = ProcessingConfig(max_memory_mb=1024)  # 1GB limit
    memory_manager = MemoryManager(config)

    print(f"   Memory threshold: {memory_manager.memory_threshold:.1f}MB")

    # Check current memory usage
    virtual_mem = psutil.virtual_memory()
    print(f"   Current memory usage: {virtual_mem.used / 1024 / 1024:.1f}MB")
    print(f"   Available memory: {virtual_mem.available / 1024 / 1024:.1f}MB")

    # Test memory management with different chunk sizes
    dataset_sizes_mb = [100, 500, 1000]

    for size_mb in dataset_sizes_mb:
        optimal_chunk = memory_manager.get_optimal_chunk_size(size_mb)
        print(f"   Dataset {size_mb}MB -> Optimal chunk: {optimal_chunk / 1024 / 1024:.1f}MB")

    # Trigger cleanup
    print(f"\n   Triggering memory cleanup...")
    memory_manager.trigger_gc()

    # Check memory after cleanup
    virtual_mem_after = psutil.virtual_memory()
    print(f"   Memory after cleanup: {virtual_mem_after.used / 1024 / 1024:.1f}MB")


def main():
    """Run all large-scale processing examples."""

    print("🏗️ Large-Scale Data Processing Example")
    print("=" * 60)
    print("This example demonstrates enterprise-grade large-scale data")
    print("processing capabilities using fsspeckit's dataset utilities.")

    try:
        # Run all demonstrations
        demonstrate_large_scale_processing()
        demonstrate_partitioning_strategies()
        demonstrate_memory_management()

        print("\n" + "=" * 60)
        print("✅ Large-scale processing examples completed successfully!")

        print(f"\n🎯 Advanced Takeaways:")
        print("• Parallel processing is essential for large datasets")
        print("• Memory management prevents system overload")
        print("• Partitioning strategy depends on data characteristics")
        print("• Fault tolerance ensures processing reliability")
        print("• Performance monitoring guides optimization")

        print(f"\n🏗️ Production Considerations:")
        print("• Monitor system resources continuously")
        print("• Implement proper error handling and retries")
        print("• Use appropriate partitioning for your data patterns")
        print("• Consider distributed processing for very large datasets")
        print("• Implement data validation at each processing stage")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()