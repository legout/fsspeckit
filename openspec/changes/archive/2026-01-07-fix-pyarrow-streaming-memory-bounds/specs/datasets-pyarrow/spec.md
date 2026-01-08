## MODIFIED Requirements
### Requirement: Streaming Deduplication Memory Bounds
The streaming deduplication SHALL use bounded memory for key tracking regardless of dataset cardinality, preventing OOM errors for high-cardinality datasets.

#### Scenario: Low-cardinality deduplication with exact tracking
- **GIVEN** a dataset with fewer than 1M unique keys
- **WHEN** streaming deduplication is performed
- **THEN** exact key tracking SHALL be used
- **AND** deduplication SHALL be 100% accurate
- **AND** memory usage for key tracking SHALL be bounded

#### Scenario: High-cardinality deduplication with approximate tracking
- **GIVEN** a dataset with more than 10M unique keys
- **WHEN** streaming deduplication is performed
- **THEN** approximate key tracking (Bloom filter) SHALL be used
- **AND** false positive rate SHALL be configurable (default: 0.1%)
- **AND** deduplication quality metrics SHALL be reported
- **AND** memory usage SHALL remain bounded regardless of cardinality

#### Scenario: Automatic tier transition
- **GIVEN** key cardinality exceeds exact tier limit during processing
- **WHEN** additional keys are encountered
- **THEN** the system SHALL automatically upgrade to LRU or Bloom tier
- **AND** a warning SHALL be logged about reduced accuracy
- **AND** processing SHALL continue without interruption

## ADDED Requirements
### Requirement: Adaptive Key Tracker
The PyArrow backend SHALL provide an AdaptiveKeyTracker that automatically selects the optimal memory strategy based on key cardinality.

#### Scenario: AdaptiveKeyTracker configuration
- **GIVEN** AdaptiveKeyTracker with max_exact_keys=1000000
- **WHEN** more than 1M unique keys are added
- **THEN** the tracker SHALL upgrade to LRU tier
- **AND** quality metrics SHALL reflect the tier change

### Requirement: Deduplication Quality Metrics
The deduplication results SHALL include quality metrics indicating accuracy and memory trade-offs.

#### Scenario: Quality metrics reporting
- **GIVEN** deduplication is performed on high-cardinality data
- **WHEN** operation completes
- **THEN** results SHALL include:
  - `key_tracker_tier`: Current tier used
  - `keys_tracked`: Number of keys tracked
  - `dedup_quality`: "exact", "approximate", or "probabilistic"
  - `estimated_accuracy`: Estimated deduplication accuracy
