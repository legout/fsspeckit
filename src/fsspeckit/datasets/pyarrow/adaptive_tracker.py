"""
Adaptive key tracking for streaming operations with memory bounds.
Provides tiered storage from exact sets to probabilistic Bloom filters.
"""

from fsspeckit.common.logging import get_logger
import threading
import sys
from typing import Any, Dict, Optional, Set, Union, Tuple
from collections import OrderedDict

logger = get_logger(__name__)

try:
    from pybloom_live import BloomFilter, ScalableBloomFilter

    HAS_BLOOM = True
except ImportError:
    HAS_BLOOM = False
    BloomFilter = None
    ScalableBloomFilter = None


class AdaptiveKeyTracker:
    """
    Tracks unique keys with adaptive memory usage.

    Tiers:
    1. EXACT (Set): Guaranteed accuracy, low memory (up to max_exact_keys).
    2. LRU (OrderedDict): Bounded memory, misses possible if evicted (up to max_lru_keys).
    3. BLOOM (Probabilistic): Fixed/Scalable memory, false positives possible, no false negatives.

    Automatically transitions between tiers as cardinality grows or memory pressure increases.
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

        self._lock = threading.Lock()
        self._tier = "EXACT"
        self._exact_keys: Optional[Set[Any]] = set()
        self._lru_keys: Optional[OrderedDict] = None
        self._bloom_filter: Any = None

        self._keys_added_count = 0
        self._unique_keys_seen = 0
        self._transitions = 0

        # Performance tracking
        self._peak_estimated_mem = 0

    def add(self, key: Any) -> None:
        """
        Add a key to the tracker.

        Args:
            key: The key to track. Can be a single value or a tuple for multi-column keys.
        """
        # Ensure key is hashable (lists are common in some arrow contexts)
        if isinstance(key, list):
            key = tuple(key)

        with self._lock:
            self._keys_added_count += 1

            # Periodically update memory peak (every 1000 additions to reduce overhead)
            if self._keys_added_count % 1000 == 0:
                self._update_mem_peak()

            # Use a loop to handle potential tier transitions during addition
            while True:
                if self._tier == "EXACT":
                    if self._exact_keys is None:
                        # Defensive: tier mismatch, try to recover
                        self._tier = "LRU"
                        continue
                    if key in self._exact_keys:
                        return
                    if len(self._exact_keys) < self.max_exact_keys:
                        self._exact_keys.add(key)
                        self._unique_keys_seen += 1
                        return
                    else:
                        self._transition_to_lru()
                        # Continue to next iteration to add to the new tier
                elif self._tier == "LRU":
                    if self._lru_keys is None:
                        # Defensive: tier mismatch, try to recover
                        if (
                            HAS_BLOOM
                            and ScalableBloomFilter is not None
                            and self._bloom_filter
                        ):
                            self._tier = "BLOOM"
                        else:
                            # Re-initialize LRU if lost
                            self._lru_keys = OrderedDict()
                        continue
                    if key in self._lru_keys:
                        # Refresh LRU position
                        self._lru_keys.move_to_end(key)
                        return
                    if len(self._lru_keys) < self.max_lru_keys:
                        self._add_to_lru(key)
                        return
                    else:
                        # Try transitioning to Bloom if available
                        if HAS_BLOOM and ScalableBloomFilter is not None:
                            self._transition_to_bloom()
                            # Continue to next iteration to add to the new tier
                        else:
                            # Fallback: stay in LRU and evict oldest
                            self._add_to_lru(key)
                            return
                elif self._tier == "BLOOM":
                    if self._bloom_filter is None:
                        # Defensive: try to recover by initializing Bloom
                        if HAS_BLOOM and ScalableBloomFilter is not None:
                            self._bloom_filter = ScalableBloomFilter(
                                initial_capacity=self.max_lru_keys,
                                error_rate=self.false_positive_rate,
                            )
                        else:
                            self._tier = "LRU"
                            self._lru_keys = OrderedDict()
                            continue
                    self._add_to_bloom(key)
                    return
                else:
                    # Should not be reached
                    break

    def __contains__(self, key: Any) -> bool:
        """
        Check if a key has been seen before.
        """
        if isinstance(key, list):
            key = tuple(key)

        with self._lock:
            if self._tier == "EXACT":
                return (
                    key in self._exact_keys if self._exact_keys is not None else False
                )
            elif self._tier == "LRU":
                if self._lru_keys is not None and key in self._lru_keys:
                    self._lru_keys.move_to_end(key)
                    return True
                return False
            elif self._tier == "BLOOM":
                if self._bloom_filter:
                    # Bloom filter might have false positives but no false negatives
                    return key in self._bloom_filter
                return False
            return False

    def _add_to_lru(self, key: Any) -> None:
        """Add key to LRU cache (assumes lock is held)."""
        if self._lru_keys is None:
            self._lru_keys = OrderedDict()

        if key not in self._lru_keys:
            self._unique_keys_seen += 1
        self._lru_keys[key] = True
        self._lru_keys.move_to_end(key)
        if len(self._lru_keys) > self.max_lru_keys:
            self._lru_keys.popitem(last=False)

    def _add_to_bloom(self, key: Any) -> None:
        """Add key to Bloom filter (assumes lock is held)."""
        if self._bloom_filter:
            if key not in self._bloom_filter:
                self._unique_keys_seen += 1
            self._bloom_filter.add(key)

    def _transition_to_lru(self) -> None:
        """Transition from EXACT to LRU (assumes lock is held)."""
        if self._tier != "EXACT":
            return

        assert self._exact_keys is not None
        logger.warning(
            "AdaptiveKeyTracker: transitioning from EXACT to LRU. "
            "Memory limit for exact tracking reached (%d keys).",
            self.max_exact_keys,
        )

        # Build new structure first for atomicity
        lru_keys = OrderedDict()
        for k in self._exact_keys:
            lru_keys[k] = True

        # Update state atomically under lock
        self._lru_keys = lru_keys
        self._exact_keys = None  # Free memory
        self._transitions += 1
        self._tier = "LRU"

    def _transition_to_bloom(self) -> None:
        """Transition from LRU to BLOOM (assumes lock is held)."""
        if self._tier != "LRU":
            return

        assert self._lru_keys is not None
        if not HAS_BLOOM or ScalableBloomFilter is None:
            logger.warning(
                "AdaptiveKeyTracker: Bloom filter requested but pybloom-live not installed. "
                "Staying in LRU mode. Memory usage may exceed bounds if LRU limit is large."
            )
            return

        logger.warning(
            "AdaptiveKeyTracker: transitioning from LRU to BLOOM. "
            "Cardinality limit for LRU reached (%d keys). Accuracy will be probabilistic.",
            self.max_lru_keys,
        )

        # Build new structure first for atomicity
        # Use ScalableBloomFilter to handle unknown future growth
        bloom_filter = ScalableBloomFilter(
            initial_capacity=self.max_lru_keys, error_rate=self.false_positive_rate
        )

        for k in self._lru_keys:
            bloom_filter.add(k)

        # Update state atomically under lock
        self._bloom_filter = bloom_filter
        self._lru_keys = None  # Free memory
        self._transitions += 1
        self._tier = "BLOOM"

    def _update_mem_peak(self) -> None:
        """Update peak memory estimate (internal)."""
        current = self.get_estimated_memory_usage()
        if current > self._peak_estimated_mem:
            self._peak_estimated_mem = current

    def get_estimated_memory_usage(self) -> int:
        """
        Estimate the current memory usage of the tracker in bytes.

        Returns:
            Estimated memory usage in bytes.
        """
        # Note: sys.getsizeof is shallow, so we add estimates for container contents
        if self._tier == "EXACT":
            assert self._exact_keys is not None
            # Approx 24 bytes per entry in set + set overhead
            return sys.getsizeof(self._exact_keys) + (len(self._exact_keys) * 64)
        elif self._tier == "LRU":
            assert self._lru_keys is not None
            # Approx 100 bytes per entry in OrderedDict + overhead
            return sys.getsizeof(self._lru_keys) + (len(self._lru_keys) * 128)
        elif self._tier == "BLOOM":
            if self._bloom_filter:
                # Bloom filters are usually bitarrays, we try to get size if possible
                if hasattr(self._bloom_filter, "bitarray"):
                    return sys.getsizeof(self._bloom_filter.bitarray)
                # Fallback to a rough estimate based on capacity and error rate
                # m = - (n * ln(p)) / (ln(2)^2)
                if hasattr(self._bloom_filter, "capacity"):
                    import math

                    n = self._bloom_filter.capacity
                    p = self.false_positive_rate
                    m = -(n * math.log(p)) / (math.log(2) ** 2)
                    return int(m / 8)  # bits to bytes
            return 0
        return 0

    def get_metrics(self) -> Dict[str, Any]:
        """
        Return quality and performance metrics.
        """
        with self._lock:
            metrics = {
                "tier": self._tier,
                "total_add_calls": self._keys_added_count,
                "unique_keys_estimate": self._unique_keys_seen,
                "transitions": self._transitions,
                "has_bloom_dependency": HAS_BLOOM,
                "estimated_memory_mb": self.get_estimated_memory_usage()
                / (1024 * 1024),
                "peak_estimated_memory_mb": self._peak_estimated_mem / (1024 * 1024),
            }

            if self._tier == "EXACT":
                assert self._exact_keys is not None
                metrics["current_count"] = len(self._exact_keys)
                metrics["accuracy_type"] = "exact"
            elif self._tier == "LRU":
                assert self._lru_keys is not None
                metrics["current_count"] = len(self._lru_keys)
                metrics["accuracy_type"] = "bounded_lru"
            elif self._tier == "BLOOM":
                metrics["accuracy_type"] = "probabilistic"
                metrics["false_positive_rate_target"] = self.false_positive_rate
                if self._bloom_filter and hasattr(self._bloom_filter, "capacity"):
                    metrics["bloom_capacity"] = self._bloom_filter.capacity

            return metrics
