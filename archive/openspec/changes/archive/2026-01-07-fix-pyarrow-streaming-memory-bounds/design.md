# Design: Add Memory Bounds to PyArrow Streaming Deduplication

## Context
The streaming deduplication uses a Python `set` to track which keys have been seen across chunks. For high-cardinality data, this set can grow unbounded, consuming all available memory.

**Current Implementation** (dataset.py:650-656):
```python
seen_keys = set()
for k in keys:
    if k not in seen_keys:
        mask.append(True)
        seen_keys.add(k)
    else:
        mask.append(False)
```

**Problem**: For a dataset with 100M unique keys, this set could consume 8+ GB of memory just for key tracking, even if the data itself is processed in chunks.

## Goals / Non-Goals

### Goals
- Bound memory usage for key tracking regardless of key cardinality
- Maintain deduplication correctness for bounded key sets
- Provide configurable trade-offs between memory and accuracy
- Graceful degradation for very high cardinality data

### Non-Goals
- Perfect deduplication for unbounded cardinality (impossible with bounded memory)
- Implementing distributed deduplication
- Supporting all data types as keys

## Decisions

### Decision 1: Tiered Memory Management Strategy
**Problem**: Need to balance memory, accuracy, and performance
**Solution**: Implement three tiers based on estimated key cardinality

**Tier 1: Exact (default for low cardinality)**
- Use standard Python set
- Up to `max_exact_keys` (default: 1M keys)
- ~100MB memory for 1M integer keys

**Tier 2: LRU Cache (medium cardinality)**
- Use bounded LRU cache with eviction
- Tracks `max_lru_keys` most recent keys (default: 10M keys)
- May produce false negatives (duplicates not caught)
- Warning logged when eviction occurs

**Tier 3: Bloom Filter (high cardinality)**
- Use probabilistic Bloom filter
- Fixed memory regardless of cardinality
- May produce false positives (unique rows marked as duplicates)
- Configurable false positive rate (default: 0.1%)

### Decision 2: Automatic Tier Selection
**Problem**: Users shouldn't need to know key cardinality in advance
**Solution**: Estimate cardinality dynamically and switch tiers as needed
**Rationale**: Provides optimal behavior without user configuration

**Implementation**:
```python
class AdaptiveKeyTracker:
    def __init__(self, max_exact_keys=1_000_000, max_lru_keys=10_000_000):
        self.exact_keys = set()
        self.lru_cache = None
        self.bloom_filter = None
        self.tier = "exact"
        
    def add(self, key):
        if self.tier == "exact":
            if len(self.exact_keys) < self.max_exact_keys:
                self.exact_keys.add(key)
            else:
                self._upgrade_to_lru()
                return self.add(key)
        elif self.tier == "lru":
            # LRU cache logic
        else:
            # Bloom filter logic
```

### Decision 3: Use pybloom-live for Bloom Filter
**Problem**: Need efficient probabilistic data structure
**Solution**: Use `pybloom-live` library for Bloom filter implementation
**Rationale**:
- Well-tested, production-ready library
- Supports scalable Bloom filters
- Pure Python, no native dependencies

**Alternative**: `bloom-filter2` or custom implementation if dependency is undesirable

### Decision 4: Deduplication Quality Metrics
**Problem**: Users need to understand deduplication accuracy
**Solution**: Track and report deduplication quality metrics
**Rationale**: Enables informed decisions about memory/accuracy trade-offs

**Metrics to Track**:
- `key_tracker_tier`: Current tier being used
- `keys_tracked`: Number of keys in tracker
- `evictions`: Number of keys evicted (LRU tier)
- `estimated_false_positive_rate`: Bloom filter FP rate
- `dedup_quality`: "exact", "approximate", or "probabilistic"

## Risks / Trade-offs

### Risk 1: False Negatives in LRU Tier
**Risk**: Recently evicted keys may reappear as "new"
**Mitigation**:
- Large default LRU size (10M keys)
- Log warning when eviction occurs
- Provide option to fail instead of evict

### Risk 2: False Positives in Bloom Filter Tier
**Risk**: Unique rows may be incorrectly marked as duplicates
**Mitigation**:
- Very low default false positive rate (0.1%)
- Configurable FP rate for different use cases
- Clear documentation of probabilistic behavior

### Risk 3: Dependency on External Library
**Risk**: Adding pybloom-live as dependency
**Mitigation**:
- Make it an optional dependency
- Fallback to LRU-only if not available
- Consider vendoring a simple Bloom filter implementation

## Migration Plan

### Phase 1: Implement AdaptiveKeyTracker
1. Create `AdaptiveKeyTracker` class with exact tier
2. Add LRU tier with configurable size
3. Add tier switching logic
4. Update streaming deduplication to use tracker

### Phase 2: Add Bloom Filter Tier
1. Add pybloom-live as optional dependency
2. Implement Bloom filter tier
3. Add automatic tier selection
4. Add quality metrics reporting

### Phase 3: Integration and Testing
1. Update merge operations to use tracker
2. Comprehensive testing with high-cardinality data
3. Performance benchmarking
4. Documentation update

### Rollback Strategy
- Feature flag to disable adaptive tracking
- Fallback to unbounded set if tracker fails
- Clear error messages for tier transitions

## Open Questions

### Question 1: LRU Eviction Policy
**Question**: What should happen when LRU eviction occurs?
**Options**:
- Continue silently (current decision)
- Log warning and continue
- Fail with error
- Switch to Bloom filter

**Current Decision**: Log warning and continue, with option to fail

### Question 2: Bloom Filter Sizing
**Question**: How to size Bloom filter for unknown cardinality?
**Options**:
- Fixed size (e.g., 100MB)
- Scalable Bloom filter that grows
- Estimate from first chunk

**Current Decision**: Use scalable Bloom filter with initial estimate from first chunk
