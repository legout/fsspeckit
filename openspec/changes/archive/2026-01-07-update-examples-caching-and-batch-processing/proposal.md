# Change: Update Caching and Batch Processing Examples (Local-First)

## Why
`examples/caching/caching_example.py` and related batching examples are intended to demonstrate key `fsspeckit` ergonomics, but currently fail or give misleading behavior because:

- `filesystem("file")` returns a `DirFileSystem` rooted at `cwd`, so absolute temp paths (e.g. `/tmp/...`) are not visible via glob/exists, causing empty reads and confusing errors.
- `fs.read_json()` defaults to `as_dataframe=True`, which is not appropriate for single-file “dict JSON” examples unless explicitly configured.

These examples should be runnable by default and demonstrate correct local-first patterns.

## What Changes
- Update caching example to:
  - Root the filesystem at the temp directory (`filesystem(tmpdir, cached=True, ...)`)
  - Use relative paths within that base
  - Use `read_json(..., as_dataframe=False)` for dict-like JSON data (or write JSON in a format matching the default behavior)
- Update batch-processing example to consistently root the filesystem at the generated temp directory and avoid absolute-path mismatches.
- Ensure outputs demonstrate correctness (not just “no exception”) and run quickly.

## Impact
- Fixes high-visibility example failures.
- Establishes correct patterns for local path usage with `DirFileSystem` defaults.

