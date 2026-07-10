## 1. Specs
- [x] Add `datasets-parquet-io` requirements for the public `merge(strategy=...)` API and returned metadata.

## 2. PyArrow implementation
- [x] Add `merge(...)` method to `PyarrowDatasetIO`.
- [x] Implement `insert` by pruning candidates and writing only non-existing keys as new files.
- [x] Implement `update` by rewriting only actually affected files (confirmed by key scan).
- [x] Implement `upsert` as `update` + appended insert files.
- [x] Enforce partition immutability for existing keys.

## 3. Tests
- [x] Add correctness tests for insert/update/upsert semantics.
- [x] Add tests asserting unaffected files are preserved (no rewrite) when possible.
- [x] Add tests asserting returned file metadata includes rewritten + inserted files.
