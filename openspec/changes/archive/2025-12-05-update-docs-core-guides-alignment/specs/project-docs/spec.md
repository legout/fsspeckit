## ADDED Requirements

### Requirement: Core Guides Match Implemented APIs

The core documentation guides (Quickstart, API guide, Advanced Usage, Utils reference) SHALL describe logging, filesystem/caching, and parallelism/sync helpers using the actual implemented APIs, without introducing fictitious helpers or outdated signatures.

#### Scenario: Logging guides use current entrypoints

- **WHEN** a user reads `README.md`, `docs/utils.md`, or logging sections in `docs/api-guide.md`
- **THEN** the examples configure logging via the current entrypoints (for example `setup_logging()` and the documented environment variables)
- **AND** they do not reference logging modules or configuration patterns that no longer exist in `src/fsspeckit`.

#### Scenario: Filesystem and caching examples match implementation

- **WHEN** a user follows filesystem or caching examples in `docs/quickstart.md` or `docs/api-guide.md`
- **THEN** the examples use the implemented `filesystem()` and cache behaviour from `src/fsspeckit/core/filesystem.py` and `src/fsspeckit/core/filesystem_cache.py`
- **AND** they do not reference functions that do not exist (for example `get_cache_size`) or omit critical path-safety semantics such as DirFileSystem base-path enforcement.

#### Scenario: Parallelism and sync docs reflect current helpers

- **WHEN** a user reads about `run_parallel`, `sync_dir`, or `sync_files` in `docs/utils.md`, `docs/api-guide.md`, or related guides
- **THEN** the function signatures and examples match the current implementations in `src/fsspeckit/common/misc.py`
- **AND** any documented optional dependency requirements (for example needing `joblib` via the appropriate extra) are accurate.

