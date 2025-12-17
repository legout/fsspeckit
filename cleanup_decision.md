# Examples Structure Cleanup - Final Decision

## Kept Examples (Modern, Supported APIs)

The following examples use current, supported fsspeckit APIs and can run offline without credentials:

### Core Learning Path
- `datasets/getting_started/` - Modern DuckDB and PyArrow basics with current APIs
- `datasets/schema/` - Schema management with current APIs
- `datasets/workflows/` - Cloud workflows (graceful failure without credentials)

### Functional Examples
- `sql/` - SQL operations with current APIs
- `common/` - Common utilities with current APIs
- `batch_processing/` - Working batch processing examples
- `caching/` - Working caching examples
- `dir_file_system/` - Working directory filesystem examples
- `read_folder/` - Working folder reading examples
- `storage_options/` - Storage configuration (graceful failure without credentials)

### Test Data
- `local:/` - Sample parquet files for testing
- `temp_multicloud_data/` - Sample data files
- `var/` - Variable test data files

### Infrastructure
- `run_examples.py` - Example runner script
- `test_examples.py` - Example test script
- `README.md` - Main examples documentation (to be updated)

## Removed Examples (Obsolete/Broken)

The following directories were removed due to broken APIs, credential requirements, or duplication:

### 1. `__pydala_dataset/` - REMOVED
- **Reason**: References non-existent `fs.pydala_dataset()` API
- **Impact**: Completely broken, no alternative examples needed

### 2. `deltalake_delta_table/` - REMOVED
- **Reason**: References non-existent APIs and hardcoded AWS profiles
- **Impact**: Broken functionality, not representative of current capabilities

### 3. `s3_pyarrow_dataset/` - REMOVED
- **Reason**: Requires AWS credentials by default, overlaps with other cloud examples
- **Impact**: Non-runnable in clean environment, redundant with `datasets/workflows/`

### 4. `duckdb/` - REMOVED
- **Reason**: Legacy examples, heavily out-of-date with current DuckDB handler APIs
- **Impact**: Duplicates functionality available in `datasets/getting_started/`

### 5. `pyarrow/` - REMOVED
- **Reason**: Legacy examples, out-of-date with current PyArrow dataset APIs
- **Impact**: Duplicates functionality available in `datasets/getting_started/`

### 6. `cross_domain/` - REMOVED
- **Reason**: References non-existent `validate_sql_query` API
- **Impact**: Completely broken, concepts covered in other examples

### 7. `datasets/advanced/` - REMOVED
- **Reason**: Currently broken and/or too heavy for basic examples
- **Impact**: Can be rewritten as docs-only narrative later if needed

## Summary

**Before**: 15 example directories + data files
**After**: 9 example directories + data files

**Key Improvements**:
- ✅ All remaining examples use supported APIs
- ✅ All examples can run offline without credentials
- ✅ No duplicate functionality between examples
- ✅ Simplified maintenance burden
- ✅ Clear learning progression in `datasets/getting_started/`