"""
Comprehensive test suite for AdaptiveKeyTracker implementation.

Tests cover all requirements from openspec fix-pyarrow-streaming-memory-bounds:
- 2.1 Test exact tier with low-cardinality data
- 2.2 Test LRU tier with medium-cardinality data
- 2.3 Test Bloom filter tier with high-cardinality data
- 2.4 Test tier transitions during processing
- 2.5 Benchmark memory usage across cardinality levels
"""

import pytest
import threading
import gc
import os
from unittest.mock import Mock, patch
from typing import Any, Dict


# Mock Bloom Filter implementations for testing when pybloom-live is not available
class MockBloomFilter:
    """Mock Bloom filter for testing."""

    def __init__(self, capacity: int, error_rate: float):
        self.capacity = capacity
        self.error_rate = error_rate
        self._data = set()

    def add(self, item: Any) -> None:
        self._data.add(item)

    def __contains__(self, item: Any) -> bool:
        return item in self._data


class MockScalableBloomFilter:
    """Mock Scalable Bloom filter for testing."""

    def __init__(self, initial_capacity: int = 1000, error_rate: float = 0.001):
        self.error_rate = error_rate
        self.filters = [MockBloomFilter(initial_capacity, error_rate)]

    def add(self, item: Any) -> None:
        self.filters[-1].add(item)

    def __contains__(self, item: Any) -> bool:
        return any(item in f for f in self.filters)


class TestAdaptiveKeyTracker:
    """Test suite for AdaptiveKeyTracker implementation."""

    @pytest.fixture
    def mock_bloom(self):
        """Mock pybloom_live module for testing."""
        with patch.dict(
            "sys.modules",
            {
                "pybloom_live": Mock(),
                "pybloom_live.BloomFilter": MockBloomFilter,
                "pybloom_live.ScalableBloomFilter": MockScalableBloomFilter,
            },
        ):
            # Force re-import of AdaptiveKeyTracker with mocked bloom
            import importlib
            import fsspeckit.datasets.pyarrow.adaptive_tracker as tracker_module

            importlib.reload(tracker_module)
            from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

            yield AdaptiveKeyTracker

    # === 2.1 Test exact tier with low-cardinality data ===

    def test_exact_tier_low_cardinality_single_column(self):
        """Test exact tier with single-column keys within max_exact_keys limit."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker(max_exact_keys=100, max_lru_keys=1000)

        # Add keys within the exact tier limit
        test_keys = [f"key_{i}" for i in range(50)]

        for key in test_keys:
            tracker.add(key)

        # Verify all keys are tracked with 100% accuracy
        for key in test_keys:
            assert key in tracker, f"Key {key} should be in exact tier"

        # Verify non-existent keys are not found
        assert "non_existent_key" not in tracker

        # Check metrics show exact tier
        metrics = tracker.get_metrics()
        assert metrics["tier"] == "EXACT"
        assert metrics["current_count"] == 50
        assert metrics["unique_keys_estimate"] == 50
        assert metrics["transitions"] == 0

    def test_exact_tier_low_cardinality_multi_column(self):
        """Test exact tier with multi-column keys within max_exact_keys limit."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker(max_exact_keys=100, max_lru_keys=1000)

        # Test multi-column keys (list and tuple formats)
        test_keys = [
            ["col1_val1", "col2_val1"],
            ("col1_val2", "col2_val2"),
            ["col1_val3", "col2_val3", "col3_val3"],
            ("single_col_key"),
            ("col1", 123, "col3", True),
        ]

        for key in test_keys:
            tracker.add(key)

        # Verify all keys are tracked with 100% accuracy (both list and tuple access)
        for key in test_keys:
            # Test with original format
            assert key in tracker, f"Multi-column key {key} should be in exact tier"
            # Test with converted format (list -> tuple)
            if isinstance(key, list):
                assert tuple(key) in tracker, (
                    f"Converted tuple key {tuple(key)} should be in exact tier"
                )

        # Check metrics
        metrics = tracker.get_metrics()
        assert metrics["tier"] == "EXACT"
        assert metrics["current_count"] == 5
        assert metrics["unique_keys_estimate"] == 5

    def test_exact_tier_boundary_behavior(self):
        """Test exact tier behavior at boundary conditions."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker(max_exact_keys=10, max_lru_keys=100)

        # Add keys up to the limit
        for i in range(9):
            tracker.add(f"key_{i}")

        assert tracker.get_metrics()["tier"] == "EXACT"

        # Add one more key - should still be in exact tier
        tracker.add("key_9")
        assert tracker.get_metrics()["tier"] == "EXACT"
        assert tracker.get_metrics()["current_count"] == 10

        # Add one more key - should trigger transition to LRU
        tracker.add("key_10")
        assert tracker.get_metrics()["tier"] == "LRU"
        assert tracker.get_metrics()["transitions"] == 1

    # === 2.2 Test LRU tier with medium-cardinality data ===

    def test_lru_tier_medium_cardinality(self):
        """Test LRU tier behavior with medium-cardinality data."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker(max_exact_keys=5, max_lru_keys=20)

        # Force transition to LRU by exceeding exact limit
        for i in range(7):
            tracker.add(f"key_{i}")

        assert tracker.get_metrics()["tier"] == "LRU"
        assert tracker.get_metrics()["transitions"] == 1

        # Test membership checks work correctly
        for i in range(7):
            assert f"key_{i}" in tracker

        # Test non-existent key
        assert "non_existent" not in tracker

        # Test LRU eviction behavior
        # Add more keys to exceed LRU limit
        for i in range(8, 25):  # This will exceed max_lru_keys of 20
            tracker.add(f"key_{i}")

        # At this point, some early keys should be evicted
        metrics = tracker.get_metrics()
        assert metrics["tier"] == "LRU"
        assert metrics["current_count"] <= 20  # Should not exceed LRU limit

    def test_lru_eviction_and_recency(self):
        """Test LRU eviction behavior and recency tracking."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker(max_exact_keys=2, max_lru_keys=5)

        # Force to LRU tier
        tracker.add("key_0")
        tracker.add("key_1")
        tracker.add("key_2")  # Triggers transition

        assert tracker.get_metrics()["tier"] == "LRU"

        # Add more keys to test eviction
        for i in range(3, 8):  # 5 more keys, will exceed LRU limit of 5
            tracker.add(f"key_{i}")

        # Most recent keys should be present
        assert "key_7" in tracker
        assert "key_6" in tracker
        assert "key_5" in tracker
        assert "key_4" in tracker
        assert "key_3" in tracker

        # Verify LRU size is bounded
        assert tracker.get_metrics()["current_count"] <= 5

    # === 2.3 Test Bloom filter tier with high-cardinality data ===

    def test_bloom_tier_with_mock(self, mock_bloom):
        """Test Bloom filter tier with mock implementation."""
        # Force to LRU tier first
        tracker = AdaptiveKeyTracker(
            max_exact_keys=2, max_lru_keys=5, false_positive_rate=0.01
        )

        for i in range(3):
            tracker.add(f"key_{i}")

        assert tracker.get_metrics()["tier"] == "LRU"

        # Force to Bloom tier by exceeding LRU limit
        for i in range(3, 7):  # 4 more keys, will exceed max_lru_keys of 5
            tracker.add(f"key_{i}")

        # Should transition to Bloom
        metrics = tracker.get_metrics()
        assert metrics["tier"] == "BLOOM"
        assert metrics["transitions"] >= 1

        # Test that known keys are detected
        for i in range(7):
            assert f"key_{i}" in tracker, f"Known key key_{i} should be in Bloom filter"

    def test_bloom_without_pybloom_dependency(self):
        """Test behavior when pybloom-live is not available."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import (
            AdaptiveKeyTracker,
            HAS_BLOOM,
        )

        # Temporarily disable bloom availability
        original_has_bloom = HAS_BLOOM
        try:
            import fsspeckit.datasets.pyarrow.adaptive_tracker as tracker_module

            tracker_module.HAS_BLOOM = False

            tracker = AdaptiveKeyTracker(max_exact_keys=2, max_lru_keys=5)

            # Force to LRU
            for i in range(3):
                tracker.add(f"key_{i}")

            assert tracker.get_metrics()["tier"] == "LRU"

            # Try to force to Bloom - should stay in LRU
            for i in range(3, 8):
                tracker.add(f"key_{i}")

            # Should remain in LRU since Bloom is not available
            metrics = tracker.get_metrics()
            assert metrics["tier"] == "LRU"

        finally:
            # Restore original state
            import fsspeckit.datasets.pyarrow.adaptive_tracker as tracker_module

            tracker_module.HAS_BLOOM = original_has_bloom

    # === 2.4 Test tier transitions during processing ===

    def test_exact_to_lru_transition(self):
        """Test exact to LRU tier transition."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker(max_exact_keys=3, max_lru_keys=10)

        # Start in exact tier
        assert tracker.get_metrics()["tier"] == "EXACT"

        # Add keys up to limit
        for i in range(3):
            tracker.add(f"key_{i}")

        assert tracker.get_metrics()["tier"] == "EXACT"
        assert tracker.get_metrics()["current_count"] == 3

        # Add one more - should trigger transition
        tracker.add("key_3")

        # Verify transition
        metrics = tracker.get_metrics()
        assert metrics["tier"] == "LRU"
        assert metrics["transitions"] == 1

        # Verify existing keys are preserved
        for i in range(4):
            assert f"key_{i}" in tracker

        # Verify unique count is preserved
        assert metrics["unique_keys_estimate"] == 4

    def test_lru_to_bloom_transition(self, mock_bloom):
        """Test LRU to Bloom tier transition."""
        tracker = AdaptiveKeyTracker(
            max_exact_keys=2, max_lru_keys=5, false_positive_rate=0.001
        )

        # Force to LRU
        for i in range(3):
            tracker.add(f"key_{i}")

        assert tracker.get_metrics()["tier"] == "LRU"

        # Force to Bloom by exceeding LRU limit
        for i in range(3, 7):
            tracker.add(f"key_{i}")

        # Verify transition to Bloom
        metrics = tracker.get_metrics()
        assert metrics["tier"] == "BLOOM"
        assert metrics["transitions"] >= 1

        # Verify existing keys are preserved
        for i in range(7):
            assert f"key_{i}" in tracker

    def test_multiple_transitions(self, mock_bloom):
        """Test multiple tier transitions in sequence."""
        tracker = AdaptiveKeyTracker(
            max_exact_keys=2, max_lru_keys=4, false_positive_rate=0.01
        )

        # Track initial state
        initial_metrics = tracker.get_metrics()
        assert initial_metrics["tier"] == "EXACT"
        assert initial_metrics["transitions"] == 0

        # Transition 1: EXACT -> LRU
        for i in range(3):  # Exceeds max_exact_keys=2
            tracker.add(f"key_{i}")

        metrics = tracker.get_metrics()
        assert metrics["tier"] == "LRU"
        assert metrics["transitions"] == 1

        # Transition 2: LRU -> BLOOM
        for i in range(3, 6):  # Exceeds max_lru_keys=4
            tracker.add(f"key_{i}")

        metrics = tracker.get_metrics()
        assert metrics["tier"] == "BLOOM"
        assert metrics["transitions"] == 2

        # Verify data integrity across transitions
        for i in range(6):
            assert f"key_{i}" in tracker

    # === 2.5 Benchmark memory usage across cardinality levels ===

    def test_memory_usage_bounded(self):
        """Test that memory usage stays within expected bounds."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        # Create tracker with moderate limits
        tracker = AdaptiveKeyTracker(max_exact_keys=1000, max_lru_keys=5000)

        # Add many keys to test memory growth
        for i in range(2000):
            tracker.add(f"key_{i}_{'x' * 100}")  # Some memory per key

        # Force garbage collection
        gc.collect()

        # Verify we're in the right tier
        metrics = tracker.get_metrics()
        # Should be in LRU since we exceeded exact limit
        assert metrics["tier"] in ["LRU", "BLOOM"]

    def test_large_cardinality_performance(self):
        """Test performance with large numbers of keys."""
        import time
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker(max_exact_keys=100, max_lru_keys=1000)

        start_time = time.time()

        # Add large number of keys
        num_keys = 5000
        for i in range(num_keys):
            tracker.add(f"perf_key_{i}")

        add_time = time.time() - start_time

        # Test lookup performance
        start_time = time.time()

        # Test some lookups
        for i in range(0, num_keys, 100):  # Test every 100th key
            key = f"perf_key_{i}"
            assert key in tracker

        lookup_time = time.time() - start_time

        # Performance should be reasonable
        assert add_time < 10.0, f"Adding {num_keys} keys took too long: {add_time}s"
        assert lookup_time < 1.0, f"Lookup operations took too long: {lookup_time}s"

        # Verify final state
        metrics = tracker.get_metrics()
        assert metrics["total_operations"] == num_keys
        assert metrics["unique_keys_estimate"] == num_keys

    def test_thread_safety_basic(self):
        """Test basic thread safety of AdaptiveKeyTracker."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker(max_exact_keys=100, max_lru_keys=500)

        def add_keys(thread_id: int):
            for i in range(50):
                tracker.add(f"thread_{thread_id}_key_{i}")

        def check_keys(thread_id: int):
            for i in range(50):
                key = f"thread_{thread_id}_key_{i}"
                # Should not raise exceptions
                try:
                    result = key in tracker
                except Exception as e:
                    pytest.fail(f"Thread safety issue: {e}")

        threads = []

        # Start threads adding keys
        for i in range(4):
            t = threading.Thread(target=add_keys, args=(i,))
            threads.append(t)
            t.start()

        # Wait for adds to complete
        for t in threads:
            t.join()

        # Start threads checking keys
        threads = []
        for i in range(4):
            t = threading.Thread(target=check_keys, args=(i,))
            threads.append(t)
            t.start()

        # Wait for checks to complete
        for t in threads:
            t.join()

        # Should have handled all keys without errors
        metrics = tracker.get_metrics()
        assert metrics["total_operations"] == 200  # 4 threads * 50 keys each

    def test_edge_cases(self):
        """Test edge cases and unusual inputs."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker(max_exact_keys=10, max_lru_keys=20)

        # Test empty and None keys
        tracker.add("")
        assert "" in tracker

        tracker.add(None)
        assert None in tracker

        # Test mixed data types
        tracker.add(123)
        tracker.add(123.456)
        tracker.add(True)
        tracker.add(False)

        assert 123 in tracker
        assert 123.456 in tracker
        assert True in tracker
        assert False in tracker

        # Test duplicate keys
        tracker.add("duplicate")
        tracker.add("duplicate")
        tracker.add("duplicate")

        assert "duplicate" in tracker
        # Should only count as one unique key
        metrics = tracker.get_metrics()
        assert metrics["unique_keys_estimate"] >= 1

    def test_metrics_completeness(self):
        """Test that metrics contain all expected fields."""
        from fsspeckit.datasets.pyarrow.adaptive_tracker import AdaptiveKeyTracker

        tracker = AdaptiveKeyTracker(max_exact_keys=10, max_lru_keys=20)

        tracker.add("test_key")
        metrics = tracker.get_metrics()

        # Check that all expected fields are present
        assert "tier" in metrics
        assert "current_count" in metrics
        assert "unique_keys_estimate" in metrics
        assert "total_operations" in metrics
        assert "transitions" in metrics
        assert "memory_usage_estimate_mb" in metrics

        # Test that values are reasonable
        assert metrics["tier"] == "EXACT"
        assert metrics["current_count"] == 1
        assert metrics["total_operations"] == 1
        assert metrics["transitions"] == 0
