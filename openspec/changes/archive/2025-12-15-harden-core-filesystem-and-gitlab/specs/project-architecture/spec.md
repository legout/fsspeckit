## ADDED Requirements

### Requirement: Core filesystem path handling is explicit and deterministic

Core filesystem factories SHALL handle local vs remote paths explicitly and derive base directories and cache hints in a
deterministic, easy-to-reason-about way.

#### Scenario: Local paths are normalised consistently
- **WHEN** a caller passes a local file or directory path to `fsspeckit.core.filesystem.filesystem()`
- **THEN** the factory SHALL normalise the path using the dedicated helpers in `fsspeckit.core.filesystem_paths`
- **AND** the resulting `DirFileSystem` or base filesystem SHALL have a clear and predictable root.

#### Scenario: Cache path hints are derived from normalised roots
- **WHEN** a caller enables caching on a filesystem created from a local path
- **THEN** the cache storage hint SHALL be derived from the normalised dataset root or base directory
- **AND** SHALL NOT depend on ambiguous “file vs directory” heuristics.

### Requirement: Git-based filesystems handle deep paths and large trees

Git-based filesystem implementations SHALL correctly handle URL encoding, pagination, and timeouts when accessing remote
repositories.

#### Scenario: GitLab filesystem encodes paths and paginates listings
- **WHEN** a caller uses `GitLabFileSystem.ls()` on a repository with nested paths or more than one page of results
- **THEN** the filesystem SHALL URL-encode project identifiers and paths as needed
- **AND** SHALL follow GitLab pagination headers to return a complete listing for the requested path
- **AND** SHALL use bounded HTTP requests with a sensible timeout.

