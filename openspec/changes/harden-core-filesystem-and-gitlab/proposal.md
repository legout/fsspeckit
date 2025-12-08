# Change: Harden core filesystem path handling and GitLab filesystem

## Why

The core filesystem factory and GitLab filesystem implementation are critical entrypoints for users, but there are areas
where behaviour is implicit or fragile:

- `_detect_local_file_path` in `core.filesystem_paths` does not clearly communicate whether a path is a file or directory,
  yet its return value is treated as if it distinguishes files from directories.
- The `filesystem()` factory in `core.filesystem` relies on that helper and contains path-handling logic that is harder
  to reason about than necessary.
- `GitLabFileSystem` constructs API URLs without URL-encoding paths, has no explicit timeouts, and only returns the first
  page of results from `repository/tree`.

These behaviours can lead to subtle bugs (e.g. mis-rooted DirFileSystem instances, broken cache hints, truncated
directory listings, or hanging HTTP calls).

## What Changes

- Refactor path handling in `core.filesystem` and `core.filesystem_paths` to:
  - Use clearly named helpers whose return shape matches how they are used.
  - Make the distinction between local and remote paths explicit.
  - Simplify how base directories and cache path hints are derived.
- Harden `GitLabFileSystem` by:
  - URL-encoding project identifiers and paths when building API URLs.
  - Using a `requests.Session` with a sensible default timeout and configuration.
  - Implementing simple pagination for `ls` so large directory trees are handled correctly.
  - Logging meaningful context for HTTP errors via the central logging utilities.

## Impact

- Improves correctness and readability of core path handling.
- Avoids silent truncation of GitLab repository listings and reduces the chance of hung requests.
- Does not change public APIs, but may make behaviour more predictable and robust for edge cases.

