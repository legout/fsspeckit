#!/usr/bin/env python3
"""
Basic test for AdaptiveKeyTracker implementation.
"""

from collections import OrderedDict
import threading
from typing import Any, Dict, Optional, Set


class AdaptiveKeyTracker:
    """Simplified AdaptiveKeyTracker for testing."""

    def __init__(self, max_exact_keys: int = 10, max_lru_keys: int = 20):
        self.max_exact_keys = max_exact_keys
        self.max_lru_keys = max_lru_keys

        self.exact_keys: Set[Any] = set()
        self.lru_keys: Optional[OrderedDict] = None
        self.tier = "EXACT"
        self.transition_count = 0
        self.total_operations = 0
        self._lock = threading.Lock()

    def _normalize_key(self, key: Any) -> Any:
        if isinstance(key, list):
            return tuple(key)
        return key

    def _upgrade_to_lru(self) -> None:
        self.lru_keys = OrderedDict()
        for key in self.exact_keys:
            self.lru_keys[key] = True
        self.exact_keys.clear()
        self.tier = "LRU"
        self.transition_count += 1

    def add(self, key: Any) -> None:
        with self._lock:
            key = self._normalize_key(key)
            self.total_operations += 1

            if self.tier == "EXACT":
                if len(self.exact_keys) < self.max_exact_keys:
                    self.exact_keys.add(key)
                else:
                    self._upgrade_to_lru()
                    return self.add(key)
            else:  # LRU
                if self.lru_keys is None:
                    return self.add(key)
                if len(self.lru_keys) < self.max_lru_keys:
                    self.lru_keys[key] = True

    def __contains__(self, key: Any) -> bool:
        with self._lock:
            key = self._normalize_key(key)
            if self.tier == "EXACT":
                return key in self.exact_keys
            else:
                return key in (self.lru_keys or {})

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            metrics = {
                "tier": self.tier,
                "total_operations": self.total_operations,
                "transition_count": self.transition_count,
            }
            if self.tier == "EXACT":
                metrics["keys_tracked"] = len(self.exact_keys)
                metrics["estimated_accuracy"] = 1.0
            else:
                metrics["keys_tracked"] = len(self.lru_keys) if self.lru_keys else 0
                metrics["estimated_accuracy"] = 0.9  # Simplified for test
            return metrics


def test_basic_functionality():
    """Test basic functionality."""
    print("Testing AdaptiveKeyTracker basic functionality...")

    tracker = AdaptiveKeyTracker(max_exact_keys=3, max_lru_keys=10)

    # Test exact tier
    print("Adding keys 1, 2, 3...")
    for i in range(1, 4):
        tracker.add(f"key_{i}")

    assert "key_1" in tracker
    assert "key_3" in tracker
    assert "key_4" not in tracker

    metrics = tracker.get_metrics()
    assert metrics["tier"] == "EXACT"
    assert metrics["keys_tracked"] == 3
    print(f"✓ Exact tier: {metrics}")

    # Test transition to LRU
    print("Adding keys 4, 5 (should trigger transition to LRU)...")
    for i in range(4, 6):
        tracker.add(f"key_{i}")

    metrics = tracker.get_metrics()
    assert metrics["tier"] == "LRU"
    assert metrics["transition_count"] == 1
    print(f"✓ LRU tier: {metrics}")

    # Test multi-column keys
    print("Testing multi-column keys...")
    tracker.add(["col1", "val1"])
    tracker.add(["col2", "val2"])

    assert ["col1", "val1"] in tracker
    assert ("col1", "val1") in tracker  # Should work as tuple too

    print("✓ Multi-column keys work")

    print("All basic tests passed! 🎉")


if __name__ == "__main__":
    test_basic_functionality()
