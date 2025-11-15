## ADDED Requirements
### Requirement: Sample-Driven Schema Inference for `opt_dtype`
`opt_dtype` (Polars and PyArrow) SHALL derive its optimized schema solely from the user-specified sample (`sample_size`/`sample_method`) so that large tables can be profiled without scanning every row.

#### Scenario: First-n sample defines the schema
- **WHEN** a string column is sampled with `sample_size=128` and `sample_method="first"` while the rest of the column contains non-numeric noise
- **THEN** the optimizer infers the numeric type from the sampled 128 values
- **AND** uses that inferred dtype to cast the entire column, leaving the final schema numeric even though the non-sampled tails might not match

### Requirement: Random sampling respects sample schema
`opt_dtype` SHALL honor `sample_method="random"` by randomly selecting `sample_size` entries for schema inference while keeping the optimized dtype consistent across the whole column.

#### Scenario: Random sample derives integer schema
- **WHEN** `sample_method="random"` and `sample_size=256` select only digits out of a mixed column
- **THEN** `opt_dtype` infers an integer schema just once from that sample
- **AND** applies the same schema during the final casting step instead of rescanning all remaining rows
