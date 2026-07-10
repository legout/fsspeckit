# Adaptive Key Tracking

`AdaptiveKeyTracker` provides tiered, memory-bounded key tracking for streaming
deduplication and merge operations. It automatically transitions between three
tiers as cardinality grows, trading accuracy for memory.

This guide covers practical configuration. For the full tier model, metrics
reference, and constructor parameters, see the
[Adaptive Key Tracking reference](../reference/adaptive-key-tracking.md).

`AdaptiveKeyTracker` requires the `datasets` extra. See the
[extras matrix](../installation.md#optional-extras).

## Import and basic use

```python
from fsspeckit.datasets.pyarrow import AdaptiveKeyTracker

tracker = AdaptiveKeyTracker()

for record in stream:
    key = (record["user_id"], record["hour"])
    if key not in tracker:
        tracker.add(key)
        process(record)
    # else: duplicate, skip
```

The tracker supports `in` checks and `add()`. It escalates automatically; you do
not need to manage tiers yourself.

## How tiers escalate

```
EXACT (set)  ->  LRU (OrderedDict)  ->  BLOOM (probabilistic)
100% accurate     bounded memory        fixed memory, false positives possible
```

| Tier | Accuracy | Memory |
|------|----------|--------|
| EXACT | 100% true positives and negatives | ~72 bytes per key; linear |
| LRU | 100% for cached keys; false negatives for evicted keys | bounded at `max_lru_keys` |
| BLOOM | 100% true positives; false positives at `false_positive_rate` | fixed, ~1.25 to 2.5 bytes per key |

The BLOOM tier requires `pybloom-live`. Without it, the tracker stays in the
LRU tier rather than escalating to Bloom.

## Choosing a configuration

The constructor takes three knobs:

- `max_exact_keys` (default `1_000_000`): keys tracked exactly before LRU.
- `max_lru_keys` (default `10_000_000`): keys tracked in LRU before Bloom.
- `false_positive_rate` (default `0.001`): target false positive rate for Bloom.

| Workload | `max_exact_keys` | `max_lru_keys` | `false_positive_rate` |
|----------|-------------------|-----------------|------------------------|
| Small, exact required | `5_000_000` | `10_000_000` | `0.001` |
| Medium, temporal locality | `100_000` | `2_000_000` | `0.001` |
| Large, high accuracy | `10_000` | `100_000` | `0.0001` |
| Memory constrained | `10_000` | `100_000` | `0.01` |

```python
# Large, high-cardinality workload: reach Bloom with a tight error rate
tracker = AdaptiveKeyTracker(
    max_exact_keys=10_000,
    max_lru_keys=100_000,
    false_positive_rate=0.0001,
)
```

## Reading metrics

`get_metrics()` returns the current state. Use `tier` and `accuracy_type` to
decide whether the accuracy profile fits your requirements:

```python
metrics = tracker.get_metrics()
print(metrics["tier"])                # "EXACT", "LRU", or "BLOOM"
print(metrics["accuracy_type"])       # "exact", "bounded_lru", or "probabilistic"
print(metrics["unique_keys_estimate"])
print(metrics["estimated_memory_mb"])
print(metrics["peak_estimated_memory_mb"])
```

For the full metrics dictionary, see the
[Adaptive Key Tracking reference](../reference/adaptive-key-tracking.md).

## Use in merges

`PyarrowDatasetIO.merge()` uses an `AdaptiveKeyTracker` internally to track
source keys during incremental rewrites. When you call `merge()` with
`key_columns`, the tracker escalates automatically as the number of distinct
keys grows. You do not need to instantiate a tracker yourself for handler
merges.

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()
result = io.merge(
    source,
    "dataset/",
    strategy="upsert",
    key_columns=["tenant_id", "user_id"],
)
```

For manual deduplication of an existing dataset (rather than an incremental
merge), use `deduplicate_parquet_dataset_pyarrow()`. See
[Multi-Key API](../reference/multi-key-api.md).

## Related documentation

- [Adaptive Key Tracking reference](../reference/adaptive-key-tracking.md) -
  tiers, metrics, and constructor reference.
- [Memory-Constrained Environments](memory-constrained-environments.md) -
  practical memory tuning.
- [Multi-Key Examples](multi-key-examples.md) - composite-key deduplication.
