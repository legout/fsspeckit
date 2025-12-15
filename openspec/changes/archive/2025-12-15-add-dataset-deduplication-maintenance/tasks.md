## 1. Specification
- [x] 1.1 Add dedup maintenance requirements to `utils-duckdb` and `utils-pyarrow`
- [x] 1.2 Specify how dedup composes with `optimize_parquet_dataset`

## 2. Public API
- [x] 2.1 Add `deduplicate_parquet_dataset` to `DatasetHandler` protocol
- [x] 2.2 Add optional dedup parameters to `optimize_parquet_dataset` signatures (both backends)

## 3. DuckDB Backend
- [x] 3.1 Implement `deduplicate_parquet_dataset` for key-based and exact-row dedup
- [x] 3.2 Support `dry_run` returning planned stats without writing/deleting files
- [x] 3.3 Integrate optional dedup step into `optimize_parquet_dataset`

## 4. PyArrow Backend
- [x] 4.1 Implement `deduplicate_parquet_dataset` using PyArrow dataset scanning and compute where feasible
- [x] 4.2 Support `dry_run` returning planned stats without writing/deleting files
- [x] 4.3 Integrate optional dedup step into `optimize_parquet_dataset`

## 5. Tests + Docs
- [x] 5.1 Add tests for dedup maintenance (key-based and exact-row)
- [x] 5.2 Add tests for optimize + dedup integration
- [x] 5.3 Document `deduplicate_parquet_dataset` and optimize parameters with examples

