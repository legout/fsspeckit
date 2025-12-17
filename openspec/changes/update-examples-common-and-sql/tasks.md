## 1. Common Examples

- [x] 1.1 Update `examples/common/logging_setup.py` to use `setup_logging()` (config) + `get_logger()` (logger), and remove unsupported parameters
- [x] 1.2 Update `examples/common/parallel_processing.py` to use correct thread/process identity calls
- [x] 1.3 Update `examples/common/type_conversion.py` to match `fsspeckit.common.types` exports

## 2. SQL Examples

- [x] 2.1 Fix dataset creation in `examples/sql/sql_filter_basic.py` and pass `schema` into `sql2pyarrow_filter`
- [x] 2.2 Fix dataset creation in `examples/sql/sql_filter_advanced.py` (PyArrow 22 compatible)
- [x] 2.3 Fix dataset creation in `examples/sql/cross_platform_filters.py` and ensure both backends work (where dependencies are present)

## 3. Validation

- [x] 3.1 Run each updated script with `.venv/bin/python` and confirm it completes successfully offline
- [x] 3.2 Ensure scripts are included in the updated example runner categories

