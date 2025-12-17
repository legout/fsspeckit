# core-ext Specification (Delta)

## REMOVED Requirements

### Requirement: Legacy `write_pyarrow_dataset(..., strategy=...)` merge-aware write
**Reason**: dataset merges are now handled by the explicit dataset IO `merge(strategy=...)` APIs.
**Migration**: callers SHALL use backend dataset IO `merge(...)` instead of strategy-aware writes in core ext helpers.

