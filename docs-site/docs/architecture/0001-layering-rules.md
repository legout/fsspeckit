# ADR-0001: Import Layering Rules for Package Architecture

## Status

Accepted

## Context

The fsspeckit package was refactored to use a domain-driven architecture with clear package boundaries. During the refactor, several critical issues were identified:

1. **Circular Import**: The `core.filesystem` module had a circular import that prevented the package from loading
2. **Layering Violations**: The `core.ext.parquet` module imported from `datasets.pyarrow`, violating architectural layering
3. **Code Duplication**: Schema utilities were duplicated across `common.schema` and `datasets.pyarrow.schema`

These issues undermined the architectural benefits of the refactor and made the codebase difficult to maintain.

## Decision

We will establish strict import layering rules with automated enforcement to prevent future violations.

### Package Structure

```
fsspeckit/
├── common/          # Shared utilities (lowest level)
├── core/            # Core filesystem and I/O operations
├── datasets/        # Backend-specific dataset operations
├── sql/             # SQL-related functionality
└── utils/           # Backwards compatibility façade
```

### Dependency Rules

1. **fsspeckit.common** (lowest level)
   - No dependencies on other fsspeckit packages
   - Can be imported by any package
   - Contains shared utilities, schemas, and common functionality

2. **fsspeckit.core** (core level)
   - May import from: `fsspeckit.common`
   - May import from: `fsspeckit.storage_options`
   - May import from: External packages (fsspec, pyarrow, etc.)
   - **MUST NOT import from**: `fsspeckit.datasets`, `fsspeckit.sql`

3. **fsspeckit.datasets** (backend level)
   - May import from: `fsspeckit.common`
   - May import from: `fsspeckit.core`
   - May import from: `fsspeckit.storage_options`
   - May import from: External packages
   - **MUST NOT import from**: `fsspeckit.sql`

4. **fsspeckit.sql** (highest level)
   - May import from: `fsspeckit.common`
   - May import from: `fsspeckit.core`
   - May import from: `fsspeckit.datasets`
   - May import from: External packages

5. **fsspeckit.utils** (façade layer)
   - Re-exports symbols from all packages for backwards compatibility
   - Does not contain implementation (only re-exports)
   - No layering restrictions (sits outside the dependency graph)

### Dependency Flow Diagram

```
        External Packages
               ↓
        ┌──────────────┐
        │ fsspeckit    │
        │   .common    │ ← Lowest level
        └──────┬───────┘
               ↓
        ┌──────────────┐
        │ fsspeckit    │
        │   .core      │ ← Core level
        └──────┬───────┘
               ↓
        ┌──────────────┐
        │ fsspeckit    │
        │ .datasets    │ ← Backend level
        └──────┬───────┘
               ↓
        ┌──────────────┐
        │  fsspeckit   │
        │   .sql       │ ← Highest level
        └──────┬───────┘
               ↑
        ┌──────────────┐
        │ fsspeckit    │
        │   .utils     │ ← Façade layer
        └──────────────┘
```

### Enforcement Mechanism

1. **Automated CI Check**: A Python script (`scripts/check_layering.py`) validates import layering on every PR
2. **Fast Failure**: The check runs early in CI to fail quickly on violations
3. **Clear Error Messages**: Violations report file, line number, and the violating import
4. **Documentation**: Layering rules documented in CONTRIBUTING.md

### Implementation

The layering check script (`scripts/check_layering.py`):
- Parses all Python files in the codebase
- Extracts import statements
- Validates against the layering rules
- Reports violations with file paths and line numbers
- Exits with error code if violations found

CI integration (`.github/workflows/ci.yml`):
- Runs layering check before tests
- Fails CI on violations
- Runs on all Python versions in the test matrix

## Rationale

### Why Strict Layering?

1. **Maintainability**: Clear dependencies make code easier to understand and modify
2. **Testability**: Packages with fewer dependencies are easier to test in isolation
3. **Reusability**: Lower-level packages can be reused without pulling in higher-level dependencies
4. **Prevent Circular Imports**: Strict rules prevent circular dependency chains
5. **Architectural Clarity**: Enforced boundaries make the architecture visible and enforceable

### Why This Specific Layering?

1. **common** at the bottom: Shared utilities should not depend on anything else
2. **core** above common: Core functionality uses shared utilities
3. **datasets** above core: Backend implementations use core functionality
4. **sql** at the top: SQL features can use everything else

### Why Automated Enforcement?

1. **Prevents Regression**: Manual reviews can miss violations
2. **Fast Feedback**: Developers get immediate feedback
3. **Enforceable**: Rules in code are clearer than rules in documentation
4. **Educational**: Violations teach developers about the architecture

## Alternatives Considered

### Alternative 1: No Layering Rules
- **Pros**: Maximum flexibility for developers
- **Cons**: Circular imports return, code becomes tangled
- **Decision**: Rejected - leads to the problems we're trying to solve

### Alternative 2: Documentation Only
- **Pros**: Easy to implement
- **Cons**: Rules are not enforced, easy to violate accidentally
- **Decision**: Rejected - insufficient for maintaining architectural integrity

### Alternative 3: Linting Rules (ruff, pylint)
- **Pros**: Integrates with existing tooling
- **Cons**: Complex to configure for specific package-level rules
- **Decision**: Could be added in the future, but custom script is simpler now

### Alternative 4: Runtime Validation
- **Pros**: Catches violations at runtime
- **Cons**: Violations only caught when code runs, not during CI
- **Decision**: Rejected - we want to catch violations before they reach production

## Consequences

### Positive

1. **Architectural Integrity**: Package boundaries are maintained
2. **Easier Maintenance**: Dependencies are clear and enforced
3. **Better Testability**: Packages can be tested in isolation
4. **Improved Code Quality**: Prevents anti-patterns like circular imports
5. **Developer Onboarding**: New developers learn the architecture through enforcement

### Negative

1. **Initial Learning Curve**: Developers need to understand layering rules
2. **Occasional Friction**: Some features may require refactoring to fit the rules
3. **Maintenance Overhead**: The check script needs to be maintained
4. **Reduced Flexibility**: Cannot take shortcuts that violate layering

### Migration Path

For existing code:
1. **Phase 1**: Identify violations using the check script
2. **Phase 2**: Refactor code to move functionality to appropriate layers
3. **Phase 3**: Use re-exports in utils for backwards compatibility
4. **Phase 4**: Enforce rules in CI

## References

- [Migration Guide](../how-to/migrate-package-layout.md)
- [CONTRIBUTING.md](../contributing.md)

## Date

2025-12-08

## Authors

- fsspeckit team
