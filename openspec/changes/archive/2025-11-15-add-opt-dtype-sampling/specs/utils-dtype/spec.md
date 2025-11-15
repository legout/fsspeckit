## ADDED Requirements
### Requirement: Sample-based dtype inference for `opt_dtype`
The `opt_dtype` helpers for Polars and PyArrow SHALL accept controls for sampling (`sample_size` and `sample_method`) so that the regex-based dtype inference inspects a bounded number of values instead of every row in very large tables.

#### Scenario: Sample-limited inference still detects integers
- **WHEN** a string column contains only integers but has millions of rows
- **AND** the caller sets `sample_size=128` and `sample_method="first"`
- **THEN** the optimizer only inspects the first 128 cleaned values for the regex match
- **AND** it still casts the entire column to integers because the full series is still valid

#### Scenario: Random sampling is supported
- **WHEN** the caller sets `sample_method="random"` and `sample_size=256`
- **AND** the cleaned column contains enough numeric-looking samples to satisfy the regex rule in that random subset
- **THEN** the inference flow proceeds exactly like the first-sample case and returns the inferred dtype

### Requirement: Safety guard for sampled inference
Inference based on the sample SHALL NOT silently corrupt data when the remainder of the column cannot be cast to the guessed type; instead it SHALL fall back to leaving the column as-is (or retain its original dtype) and not disrupt other columns.

#### Scenario: Sample hints integer but rest of column is invalid
- **WHEN** the sampled subset looks like an integer column but the full column contains non-digit text
- **THEN** the optimizer discovers that casting the eager data would fail or produce extra nulls
- **AND** it leaves the column in its original string form so the DataFrame remains valid
