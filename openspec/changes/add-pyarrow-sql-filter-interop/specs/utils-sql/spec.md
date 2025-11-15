# Capability: SQL to PyArrow Filter Interop

## ADDED Requirements

### Requirement: SQL Filter to PyArrow Expression

The system SHALL provide a `sql2pyarrow_filter(string, schema)` helper that
parses a SQL filter string and returns a `pyarrow.compute.Expression` suitable
for use as a filter in both DuckDB SQL and PyArrow dataset workflows.

#### Scenario: Basic comparisons

- **WHEN** the caller invokes  
  `expr = sql2pyarrow_filter("age > 30 AND score <= 100", schema)`
- **THEN** `expr` SHALL be a valid `pc.Expression`
- **AND** applying it via `dataset.to_table(filter=expr)` SHALL yield only rows
  where `age > 30` and `score <= 100`.

#### Scenario: IN / NOT IN operations

- **WHEN** the caller uses  
  `"category IN ('A', 'C')"` or `"category NOT IN ('B')"`
- **THEN** `sql2pyarrow_filter` SHALL produce an expression using `pc.is_in`
  semantics such that:
  - `IN` selects rows where the column value is in the provided list.
  - `NOT IN` selects rows where the column value is not in the provided list.

### Requirement: NULL and Boolean Handling

The helper SHALL support `IS NULL`, `IS NOT NULL`, and boolean literals.

#### Scenario: IS NULL / IS NOT NULL

- **WHEN** the filter string contains `"name IS NULL"` or
  `"name IS NOT NULL"`
- **THEN** the resulting expression SHALL use `is_null` / `invert` logic and be
  applicable as a dataset filter expression.

#### Scenario: Boolean literals

- **WHEN** the filter string contains `"active = true"` or
  `"active = false"`
- **THEN** the resulting expression SHALL correctly compare a boolean-typed
  field against the corresponding Python boolean literal.

### Requirement: Temporal Literal Handling

The helper SHALL correctly interpret timestamp/date/time literals for fields
with corresponding PyArrow temporal types.

#### Scenario: Timestamp, date, and time filters

- **WHEN** the schema defines:
  - `created_at: timestamp[us, tz=UTC]`
  - `birth_date: date32`
  - `login_time: time64[us]`
- **AND** the caller passes  
  `"created_at > '2023-03-01T00:00:00'"`, `"birth_date > '1990-01-01'"`,
  or `"login_time > '12:00:00'"`
- **THEN** the resulting expression SHALL convert the literal strings into
  appropriate Python/Arrow temporal representations compatible with the column
  types
- **AND** SHALL be usable as a filter in `dataset.to_table(filter=expr)`.

