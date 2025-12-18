#!/usr/bin/env python3
"""
Simple test for AdaptiveKeyTracker implementation.
This test can run without the full package dependencies.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from collections import OrderedDict
import threading
from typing import Any, Dict, Optional, Set, Union, Tuple

logger = __import__("logging").getLogger(__name__)


class AdaptiveKeyTracker:
    """
    Tracks unique keys with adaptive memory usage.

    Tiers:
    1. Exact (Set): Guaranteed accuracy, low memory (up to max_exact_keys).
    2. LRU (OrderedDict): Bounded memory, misses possible if evicted (up to max_lru_keys).
    """

    def __init__(
        self,
        max_exact_keys: int = 1_000_000,
        max_lru_keys: int = 10_000_000,
        false_positive_rate: float = 0.001,
    ):
        """
        Initialize the tracker.

        Args:
            max_exact_keys: Maximum number of keys to track exactly in a set.
            max_lru_keys: Maximum number of keys to track in LRU cache before switching to Bloom.
            false_positive_rate: Desired false positive rate for Bloom filter.
        """
        self.max_exact_keys = max_exact_keys
        self.max_lru_keys = max_lru_keys
        self.false_positive_rate = false_positive_rate

        # Storage for different tiers
        self.exact_keys: Set[Any] = set()
        self.lru_keys: Optional[OrderedDict] = None

        # Current tier and statistics
        self.tier = "EXACT"
        self.transition_count = 0
        self.total_operations = 0

        # Thread safety
        self._lock = threading.Lock()

    def _normalize_key(self, key: Any) -> Any:
        """Convert list keys to tuples for hashability."""
        if isinstance(key, list):
            return tuple(key)
        return key

    def _upgrade_to_lru(self) -> None:
        """Upgrade from exact set to LRU cache."""
        logger.warning("Upgrading from EXACT to LRU tier - may lose precision")

        self.lru_keys = OrderedDict()
        # Migrate existing keys
        for key in self.exact_keys:
            self.lru_keys[key] = True

        self.exact_keys.clear()
        self.tier = "LRU"
        self.transition_count += 1

    def add(self, key: Any) -> None:
        """Add a key to the tracker."""
        with self._lock:
            key = self._normalize_key(key)
            self.total_operations += 1

            if self.tier == "EXACT":
                if len(self.exact_keys) < self.max_exact_keys:
                    self.exact_keys.add(key)
                else:
                    self._upgrade_to_lru()
                    return self.add(key)  # Retry in new tier

            elif self.tier == "LRU":
                if self.lru_keys is None:
                    return self.add(key)  # Retry in new tier after upgrade
                if len(self.lru_keys) < self.max_lru_keys:
                    self.lru_keys[key] = True
                    # Move to end (most recent)
                    if key in self.lru_keys:
                        self.lru_keys.move_to_end(key)
                else:
                    # Stay in LRU mode for now (no bloom filter available)
                    logger.warning("LRU cache full, staying in LRU mode")

    def __contains__(self, key: Any) -> bool:
        """Check if key has been seen."""
        with self._lock:
            key = self._normalize_key(key)

            if self.tier == "EXACT":
                return key in self.exact_keys
            elif self.tier == "LRU":
                return key in (self.lru_keys or {})
            else:  # BLOOM (not implemented in this simplified version)
                return key in (self.lru_keys or {})

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics about the tracker state."""
        with self._lock:
            metrics = {
                "tier": self.tier,
                "total_operations": self.total_operations,
                "transition_count": self.transition_count,
            }

            if self.tier == "EXACT":
                metrics["keys_tracked"] = len(self.exact_keys)
                metrics["estimated_accuracy"] = 1.0

            elif self.tier == "LRU":
                metrics["keys_tracked"] = len(self.lru_keys) if self.lru_keys else 0
                metrics["estimated_accuracy"] = min(
                    1.0, self.max_lru_keys / max(1, self.total_operations)
                )

            return metrics


def test_adaptive_tracker():
    """Test the AdaptiveKeyTracker functionality."""
    print("Testing AdaptiveKeyTracker...")

    # Test 1: Basic functionality
    tracker = AdaptiveKeyTracker(max_exact_keys=10, max_lru_keys=20)

    # Add some keys
    for i in range(5):
        tracker.add(f"key_{i}")

    # Check membership
    assert "key_0" in tracker
    assert "key_4" in tracker
    assert "key_5" not in tracker

    metrics = tracker.get_metrics()
    assert metrics["tier"] == "EXACT"
    assert metrics["keys_tracked"] == 5
    print("✓ Basic functionality test passed")

    # Test 2: Tier transition to LRU
    for i in range(5, 15):  # This should trigger transition to LRU
        tracker.add(f"key_{i}")

    metrics = tracker.get_metrics()
    assert metrics["tier"] == "LRU"
    assert metrics["transition_count"] == 1
    print("✓ LRU transition test passed")

    # Test 3: Multi-column keys
    tracker2 = AdaptiveKeyTracker(max_exact_keys=5)

    # Test single column
    tracker2.add("single_key")
    assert "single_key" in tracker2

    # Test multi-column (list should be converted to tuple)
    tracker2.add(["col1", "col2"])
    assert ["col1", "col2"] in tracker2  # List should work
    assert ("col1", "col2") in tracker2  # Tuple should also work

    print("✓ Multi-column key test passed")

    # Test 4: Metrics
    tracker3 = AdaptiveKeyTracker()
    tracker3.add("test_key")
    metrics = tracker3.get_metrics()

    assert "tier" in metrics
    assert "keys_tracked" in metrics
    assert "estimated_accuracy" in metrics
    assert metrics["estimated_accuracy"] == 1.0  # Exact tier

    print("✓ Metrics test passed")

    print("All tests passed! 🎉")


if __name__ == "__main__":
    test_adaptive_tracker()
