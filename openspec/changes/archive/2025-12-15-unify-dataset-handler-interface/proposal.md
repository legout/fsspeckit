# Change: Define a shared interface for dataset handlers

## Why
With both DuckDB and PyArrow gaining class-based dataset handlers, a minimal shared interface/protocol will:
- Make documentation and examples clearer (consistent method names/params).
- Help static tooling and IDEs provide better autocomplete.
- Reduce drift between backends over time.

## What Changes
- Define a lightweight protocol or documented interface that covers common dataset handler methods (read, write, merge, compact/optimize where available).
- Align method naming/signatures across DuckDB and PyArrow handlers where feasible; document any backend-specific differences.
- Update docs to present the common surface and note backend-specific capabilities.

## Impact
- No behavioural change; adds clarity and guardrails.
- May require small renames or aliases to achieve parity; otherwise primarily documentation/type-level work.

