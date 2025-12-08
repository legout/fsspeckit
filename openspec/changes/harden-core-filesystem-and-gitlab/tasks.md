## 1. Implementation

- [x] 1.1 Refactor `_detect_local_file_path` and related helpers in `fsspeckit.core.filesystem_paths` so naming and return values match usage.
- [x] 1.2 Simplify base path and cache path resolution in `fsspeckit.core.filesystem.filesystem()` for local and remote paths.
- [x] 1.3 Refactor `GitLabFileSystem` to:
  - [x] 1.3.1 URL-encode project identifiers and paths.
  - [x] 1.3.2 Use a shared `requests.Session` with timeouts.
  - [x] 1.3.3 Implement pagination for `ls` calls.
  - [x] 1.3.4 Log HTTP failures with enough context to debug.

## 2. Testing

- [x] 2.1 Add tests for `filesystem()` covering:
  - [x] 2.1.1 Local path resolution vs URL-based protocols.
  - [x] 2.1.2 Base filesystem + DirFileSystem combinations, including cache path hints.
- [x] 2.2 Add tests for `GitLabFileSystem`:
  - [x] 2.2.1 URL construction and path-encoding logic.
  - [x] 2.2.2 Pagination behaviour (using mocked HTTP responses).

## 3. Documentation

- [x] 3.1 Update filesystem documentation and examples to clarify any new GitLab-related options (e.g. timeouts) and expectations around path handling.

