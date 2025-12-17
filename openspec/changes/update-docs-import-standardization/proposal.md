# Change: Standardize import paths across documentation

## Why
Documentation currently shows inconsistent import patterns:
- `from fsspeckit.core import filesystem`
- `from fsspeckit.core.filesystem import filesystem`
- `from fsspeckit import filesystem`

This creates confusion about the "correct" import path. The package exports key symbols
at multiple levels, and documentation should use consistent patterns that are both
clear and practical.

## What Changes
- Establish import hierarchy preference:
  1. Top-level for core symbols: `from fsspeckit import filesystem, AwsStorageOptions`
  2. Package-level for domain features: `from fsspeckit.datasets import PyarrowDatasetIO`
  3. Module-level only when accessing specific submodules
- Update all documentation to follow consistent import patterns
- Add brief import guidance section to `docs/explanation/concepts.md`
- Remove deprecated import path comments where they add confusion

## Impact
- Modifies: Multiple files across `docs/how-to/`, `docs/tutorials/`, `docs/explanation/`
- No breaking changes (all import paths work, this is a consistency improvement)
- Affected specs: `project-docs`
