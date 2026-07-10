# Adaptive Key Tracking

`AdaptiveKeyTracker` provides tiered, memory-bounded key tracking for streaming
deduplication and merge operations. It automatically transitions between three
tiers as cardinality grows, trading accuracy for memory.

This page explains how to choose a configuration and interpret behavior. For the
exact API, see [fsspeckit.datasets.pyarrow](../api/fsspeckit.datasets.pyarrow.md).

`AdaptiveKeyTracker` requires the `datasets` extra. The memory-aware examples
also use `psutil` (included in `datasets`; available separately via the
`monitoring` extra). See the [extras matrix](../installation.md#optional-extras).

## Import

```python
from fsspeckit.datasets.pyarrow import AdaptiveKeyTracker
```

## How tiers work

The tracker starts in the EXACT tier and escalates as the number of unique keys
grows:

```
EXACT (set)  ->  LRU (OrderedDict)  ->  BLOOM (probabilistic)
100% accurate     bounded memory        fixed memory, false positives possible
```

| Tier | Implementation | Accuracy | Memory |
|------|----------------|----------|--------|
| EXACT | Python `set` | 100% true positives, 0% false positives/negatives | ~72 bytes per key; linear growth |
| LRU | `OrderedDict` with eviction | 100% for cached keys; false negatives for evicted keys | ~72 bytes per key; bounded at `max_lru_keys` |
| BLOOM | `ScalableBloomFilter` (`pybloom-live`) | 100% true positives; false positives at `false_positive_rate`; 0% false negatives | ~1.25 to 2.5 bytes per key; fixed |

The BLOOM tier requires `pybloom-live`. If it is not installed, the tracker stays
in the LRU tier (no automatic Bloom transition).

## Configuration

The constructor takes three parameters:

- `max_exact_keys` (default `1_000_000`): keys tracked exactly before escalating
  to LRU.
- `max_lru_keys` (default `10_000_000`): keys tracked in LRU before escalating to
  BLOOM.
- `false_positive_rate` (default `0.001`): target false positive rate for the
  BLOOM tier.

### Choosing a configuration

| Workload | `max_exact_keys` | `max_lru_keys` | `false_positive_rate` | Why |
|----------|-------------------|-----------------|------------------------|-----|
| Small, exact required | `5_000_000` | `10_000_000` | `0.001` | Stays in EXACT; 100% accuracy |
| Medium, temporal locality | `100_000` | `2_000_000` | `0.001` | LRU exploits recent-access patterns |
| Large, high accuracy | `10_000` | `100_000` | `0.0001` | BLOOM with tight error rate |
| Memory constrained | `10_000` | `100_000` | `0.01` | BLOOM with relaxed error rate |

## Interpreting accuracy

- **EXACT tier**: no false positives or false negatives. Every seen key is
  recognized; every unseen key is rejected.
- **LRU tier**: no false positives. False negatives occur for evicted keys. The
  miss rate is proportional to `(unique_keys - max_lru_keys) / unique_keys`.
  Temporal locality (recent keys accessed more often) improves practical
  accuracy.
- **BLOOM tier**: no false negatives. False positives occur at approximately the
  configured `false_positive_rate`, meaning a previously-unseen key may be
  incorrectly reported as seen. Lower rates cost more memory per key.

## Interpreting metrics

`get_metrics()` returns a dictionary describing the current state:

| Key | Meaning |
|-----|---------|
| `tier` | Current tier: `"EXACT"`, `"LRU"`, or `"BLOOM"` |
| `total_add_calls` | Number of `add()` calls |
| `unique_keys_estimate` | Estimated unique keys seen |
| `transitions` | Number of tier transitions |
| `has_bloom_dependency` | Whether `pybloom-live` is installed |
| `estimated_memory_mb` | Current estimated memory usage |
| `peak_estimated_memory_mb` | Peak estimated memory usage |
| `current_count` | Keys in the active EXACT or LRU store |
| `accuracy_type` | `"exact"`, `"bounded_lru"`, or `"probabilistic"` |

Use `metrics["tier"]` and `metrics["accuracy_type"]` to decide whether the
current accuracy profile fits your deduplication requirements.

## Usage in merges

`PyarrowDatasetIO.merge()` uses an `AdaptiveKeyTracker` internally to track
source keys during incremental rewrites. When you call `merge()` with
`key_columns`, the tracker escalates automatically as the number of distinct
keys grows.

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()
result = io.merge(
    data, "dataset/", strategy="upsert",
    key_columns=["tenant_id", "user_id"],
    enable_streaming_merge=True,
)
```

For manual deduplication of an existing dataset, use
`deduplicate_parquet_dataset_pyarrow()` (see
[Multi-Key API](multi-key-api.md)).

## Related documentation

- [Dataset Handlers](../dataset-handlers.md) - shared merge interface.
- [Multi-Key API](multi-key-api.md) - multi-column key deduplication.
- [Memory-Constrained Environments](../how-to/memory-constrained-environments.md) - practical memory tuning.
- [Generated API](../api/fsspeckit.datasets.pyarrow.md) - signatures.
