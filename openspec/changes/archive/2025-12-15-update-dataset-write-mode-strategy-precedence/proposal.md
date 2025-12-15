# Proposal: `mode`/`strategy` Precedence for `write_parquet_dataset`

## Why
Having both `mode` (file-level behavior) and `strategy` (row-level merge semantics) active at the same time is confusing.
Users can easily pass combinations that are conceptually incompatible, leading to avoidable runtime errors.

## What Changes
- Keep safe defaults for basic writes:
  - `mode="append"` remains the default.
  - `strategy=None` remains the default.
- Define a single precedence rule:
  - **If `strategy` is provided, `mode` SHALL be ignored** (merge semantics control the operation).
  - Handlers MAY emit a warning when `mode` is explicitly provided alongside `strategy`.
- This proposal supersedes any behavior that raises errors purely due to `mode` + `strategy` combinations.

## Impact
- Better UX: fewer “invalid combination” errors; the call behaves according to `strategy` as the primary intent.
- Potentially surprising if a user expects `mode="overwrite"` to affect merge behavior; the warning mitigates this.

## Compatibility
- Behavioral change relative to implementations that currently raise on incompatible `mode`+`strategy`.
- No change for callers that use `mode` only with `strategy=None`.

