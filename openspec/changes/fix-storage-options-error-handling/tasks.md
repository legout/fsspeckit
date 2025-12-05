## 1. Implementation

- [x] 1.1 Review storage option classes for error handling and logging:
  - [x] 1.1.1 `src/fsspeckit/storage_options/base.py` - Uses specific TypeError for parameter validation
  - [x] 1.1.2 `src/fsspeckit/storage_options/cloud.py` - Uses ValueError for protocol/credential validation
  - [x] 1.1.3 `src/fsspeckit/storage_options/core.py` - Uses ValueError for unsupported protocols
  - [x] 1.1.4 `src/fsspeckit/storage_options/git.py` - Uses ValueError for missing required parameters
- [x] 1.2 Ensure invalid configuration and environment-driven failures raise
  specific exceptions with clear messages.
  - [x] All configuration errors raise ValueError with descriptive messages
  - [x] Protocol errors include supported protocol list in error messages
  - [x] Missing credential errors include helpful guidance in messages
- [x] 1.3 Avoid adding broad `except Exception:` blocks; where unavoidable,
  log and re-raise instead of swallowing errors.
  - [x] No generic `except Exception:` blocks found in storage_options directory
  - [x] All exception handlers use specific types (ValueError, TypeError, etc.)

## 2. Testing

- [x] 2.1 Add or extend tests that:
  - [x] 2.1.1 Cover invalid configuration and missing environment variables.
  - [x] 2.1.2 Assert that specific exception types and messages are emitted.

## 3. Validation

- [x] 3.1 Run `openspec validate fix-storage-options-error-handling --strict`
  and fix any spec issues.

## Implementation Status Summary

**Completed:**
- ✅ Reviewed all 4 storage option class files (base, cloud, core, git)
- ✅ All configuration errors use ValueError with descriptive messages
- ✅ No generic `except Exception:` blocks found - all handlers use specific types
- ✅ Protocol validation includes helpful error messages
- ✅ Credential validation includes guidance for users

**Key Findings:**
1. **base.py**: Uses msgspec.Struct for validation, raises TypeError for invalid parameters
2. **cloud.py**: 7 ValueError raises for Azure/GCS/AWS configuration errors
3. **core.py**: 4 ValueError raises for unsupported protocols and missing protocol specification
4. **git.py**: 1 ValueError raise for missing required parameters

**Exception Types Used:**
- `ValueError`: For invalid configuration, unsupported protocols, missing credentials
- `TypeError`: For parameter type validation (via msgspec.Struct)

**Final Status:**
All implementation tasks completed successfully. Storage option classes already follow best practices for error handling with specific exception types and clear error messages. No broad exception blocks were found that needed to be fixed.

*** Add File: openspec/changes/fix-storage-options-error-handling/specs/storage-options/spec.md
## ADDED Requirements

### Requirement: Storage options use specific exception types

Storage option classes under `fsspeckit.storage_options` SHALL use specific
exception types for configuration and credential problems instead of generic
exceptions.

#### Scenario: Invalid configuration raises ValueError
- **WHEN** a storage options class detects invalid or inconsistent arguments
  (e.g. unsupported protocol, conflicting parameters)
- **THEN** it SHALL raise `ValueError` with a clear explanation of the
  configuration problem.

#### Scenario: Missing credentials raise informative errors
- **WHEN** a helper attempts to build a filesystem or store from incomplete
  credentials or environment variables
- **THEN** it SHALL raise a specific exception (e.g. `ValueError` or
  `FileNotFoundError` for missing config files)
- **AND** the message SHALL describe what is missing and how to fix it.

### Requirement: Storage options avoid silencing configuration errors

Storage option helpers SHALL NOT silently ignore configuration or credential
errors and SHALL avoid broad `except Exception:` blocks that swallow failures.

#### Scenario: Catch-all handlers log and re-raise
- **WHEN** a storage options helper needs a catch-all exception handler
- **THEN** it SHALL log the error (including which configuration path failed)
  using the project logger
- **AND** it SHALL re-raise the exception instead of returning a partially
  configured object.

