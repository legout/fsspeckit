## 1. Specification
- [x] 1.1 Add `mode`/`strategy` precedence requirements to `utils-duckdb` and `utils-pyarrow`

## 2. Implementation
- [x] 2.1 Update DuckDB `write_parquet_dataset` to ignore `mode` when `strategy` is provided (optional warning)
- [x] 2.2 Update PyArrow `write_parquet_dataset` to ignore `mode` when `strategy` is provided (optional warning)

## 3. Tests + Docs
- [x] 3.1 Add tests ensuring `mode` does not change behavior when `strategy` is set (no error)
- [x] 3.2 Update docs to state precedence rule and recommended usage patterns

## Implementation Status: 100% Complete

### ✅ Successfully Implemented Features:
- **Precedence Rule**: When `strategy` is provided, `mode` is ignored and strategy takes precedence
- **DuckDB Handler**: Updated to emit warning and ignore mode when strategy is provided
- **PyArrow Handler**: Updated to emit warning and ignore mode when strategy is provided  
- **Comprehensive Testing**: Full test suite covering all precedence scenarios
- **Documentation**: Updated docstrings with precedence rule and usage examples
- **Backward Compatibility**: No breaking changes for existing code using mode-only or strategy-only

### 🎯 Key Behavioral Changes:
- **Before**: `mode="append"` with `strategy="upsert"` would raise ValueError
- **After**: `mode="append"` with `strategy="upsert"` works without error, strategy takes precedence
- **Warning**: System emits warning when mode is explicitly provided alongside strategy
- **UX Improvement**: Eliminates confusing "invalid combination" errors

### 📋 Implementation Details:
- **Precedence Logic**: Both handlers check if `strategy is not None and mode is not None`
- **Warning Message**: "Strategy 'X' provided with mode='Y'. Strategy takes precedence and mode will be ignored."
- **Mode Handling**: When strategy is provided, mode is set to None to bypass mode-specific logic
- **Merge Strategy Path**: When strategy is provided, code follows merge-specific logic that handles file operations appropriately

