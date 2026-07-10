## Context

The core filesystem factory is responsible for interpreting `protocol_or_path` inputs, deriving base directories, and
choosing whether to wrap underlying filesystems in `DirFileSystem` or caching layers. Its current implementation works
for common cases but relies on a helper whose name and return shape are easy to misinterpret.

Separately, the GitLab filesystem provides a convenient read-only view of GitLab repositories, but its HTTP integration
is minimal and does not handle path encoding or pagination.

## Goals / Non-Goals

- Goals:
  - Make core path handling easier to reason about and less error-prone.
  - Make GitLab filesystem behaviour predictable for deep directory trees and unusual paths.
  - Avoid changing public method signatures.
- Non-Goals:
  - Adding write support to GitLab filesystem.
  - Introducing new protocols beyond what `fsspec` already supports.

## Decisions

- Replace or rename `_detect_local_file_path` with helpers that:
  - Clearly express whether they are checking “local vs URL” or “file vs directory”.
  - Return structures that match their use in `filesystem()`.
- For GitLab:
  - Use `urllib.parse.quote`/`quote_plus` to encode project identifiers and paths.
  - Add a small configuration surface (e.g. timeout attribute, possibly environment overrides) without overcomplicating
    the constructor.
  - Support basic pagination by looping until no `X-Next-Page` header (or equivalent) remains.

## Risks / Trade-offs

- Changes to internal path handling may expose edge cases that were previously masked; tests and examples will guide
  acceptable behaviour.
- More robust HTTP behaviour may slightly increase implementation complexity, but improves reliability for real-world usage.

## Migration Plan

1. Refactor path helpers and `filesystem()` internals, guided by existing tests.
2. Harden `GitLabFileSystem` HTTP behaviour and add focused unit tests with mocks.
3. Update documentation to reflect any new options and assumptions.

