# fsspeckit Code & Architecture Review (2025-12-02)

## Scope & Context

- Project: `fsspeckit` (multi-cloud filesystem toolkit around `fsspec`)
- Python: `>=3.11` per `pyproject.toml`
- Focus:
  - Likely bugs / behavioural regressions
  - Mismatches between code, tests, and README
  - Over-complex or brittle areas that could be simplified
  - Optional-dependency handling and backward-compat shims

The notes below are grouped by area with concrete suggestions.

---

## 1. High-Risk / Likely Bugs

### 1.1 Datetime utilities

**File:** `src/fsspeckit/common/datetime.py`

1. **`get_timestamp_column` does not support pandas as advertised/tests expect**
   - Implementation assumes `df` is Polars or PyArrow:
     - Imports `polars` and `pyarrow` via `_import_polars` / `_import_pyarrow`.
     - If `df` is a `pa.Table`, it converts to `pl.LazyFrame`; otherwise it calls `df.select(...)`.
   - `tests/test_utils/test_utils_datetime.py` passes a `pandas.DataFrame` and asserts that the timestamp column is detected.
   - For a pandas DataFrame, the current implementation will call `df.select(...)` and raise `AttributeError`.
   - **Impact:** pandas support for `get_timestamp_column` is broken despite tests and README implying broader DataFrame support.
   - **Suggestion:**
     - Add explicit pandas handling (using `_import_pandas` from `common.optional`) and convert to Polars or Arrow before applying selectors.
     - Alternatively, document that only Polars/Arrow are supported and adjust tests accordingly, but that conflicts with current tests.

2. **`get_timedelta_str` fails on invalid/unknown units and disagrees with tests**
   - Logic:
     - Extracts `unit` via `re.sub("[0-9]", "", timedelta_string).strip()`.
     - For `to="polars"` and unknown unit, it does:
       ```python
       val + dict(zip(duckdb_timedelta_units, polars_timedelta_units))[re.sub("s$", "", unit)]
       ```
     - For `unit="invalid"`, this raises `KeyError` because `"invalid"` is not in `duckdb_timedelta_units`.
   - `tests/test_utils/test_utils_datetime.py` expects:
     - `get_timedelta_str("1invalid", to="polars") == "1 invalid"` (i.e. graceful fallback, not an exception).
   - **Impact:** Any unknown unit raises, contradicting test expectations and making the function fragile to typos.
   - **Suggestion:**
     - Guard the dict lookup and fall back to `f"{val} {unit}"` (or `re.sub("s$", "", unit)` if desired) when the unit is not recognized.
     - Mirror the same error-handling behaviour for the `"duckdb"` branch.

3. **`timestamp_from_string` assumes 3.11+ semantics but README claims broader support**
   - Inline comments say “assumes Python 3.11+ for full ISO 8601 support via `fromisoformat`”.
   - `pyproject.toml` requires `>=3.11` so this is acceptable today, but the README mentions 3.10-style optionality in places.
   - **Suggestion:** Clarify in docstring that the function targets Python 3.11+ only and that some edge formats (e.g., bare “Z”) are not accepted on older runtimes.

### 1.2 `common.misc.run_parallel` regressions

**File:** `src/fsspeckit/common/misc.py`

1. **Joblib is effectively mandatory despite being documented as optional**
   - Top-level imports:
     ```python
     from joblib import Parallel, delayed
     from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn, track
     ```
   - Later, a conditional block checks `importlib.util.find_spec("joblib")` to define `run_parallel`; but this block is only reached if the initial `from joblib import Parallel` succeeds.
   - If `joblib` is not installed, the module import fails immediately, contradicting the README’s “joblib is optional” story.
   - **Impact:** Any import of `fsspeckit.common.misc` (and transitively `fsspeckit.common` / `fsspeckit.utils`) will crash if joblib is missing, even if parallelism is unused.
   - **Suggestion:**
     - Remove the top-level `from joblib import Parallel, delayed` and move it inside the `if importlib.util.find_spec("joblib")` block.
     - Or use a lazy-import pattern like in `common.optional` (e.g., `_import_joblib()` helper) and raise a targeted `ImportError` when `run_parallel` is actually called.

2. **Generator input is no longer supported**
   - Old semantics (per tests) allow:
     ```python
     def gen():
         yield from [1, 2, 3]
     run_parallel(str, gen())
     ```
   - Current `_prepare_parallel_args` only treats `list`/`tuple` as iterables; all other types are considered scalars.
   - Calling `run_parallel` with a generator leads to `first_iterable_len is None` → `ValueError("At least one iterable argument is required")`.
   - `tests/test_utils/test_utils_misc.py` explicitly expects generator support.
   - **Recommendation:** Accept any non-string `collections.abc.Iterable` in the “iterables” path, and materialize generators into lists internally if needed.

3. **Error messages changed and no longer match tests**
   - Tests assert:
     - Length mismatch: `"All iterables must have the same length"`
     - No iterables: `"At least one iterable argument must be provided"`
   - New implementation raises:
     - `"Iterable length mismatch: argument has length X, expected Y"`
     - `"At least one iterable argument is required"`
   - **Impact:** Tests fail and external callers might rely on the previous wording.
   - **Suggestion:** Restore the original error messages (or at least preserve the substrings used in `pytest.match`).

4. **`Progress` patching in tests no longer works due to module relocation**
   - Tests patch `"fsspeckit.utils.misc.Progress"`, but `run_parallel` now lives in `fsspeckit.common.misc`.
   - There is no `fsspeckit.utils.misc` submodule, so patching will raise `AttributeError`.
   - **Suggestion:**
     - Either add a tiny shim module `fsspeckit/utils/misc.py` that re-exports `run_parallel` and `Progress`, or update tests/backwards-compat expectations.

### 1.3 `get_partitions_from_path` behaviour diverges from tests

**File:** `src/fsspeckit/common/misc.py`

1. **Return type and defaults mismatched vs tests**
   - Implementation signature:
     ```python
     def get_partitions_from_path(path: str, partitioning: Union[str, list[str], None] = None) -> list[tuple]:
     ```
   - Behaviour:
     - Returns an empty list when `partitioning is None`.
     - For `partitioning="hive"`, returns a `list[tuple]` of `(key, value)` pairs.
   - Tests in `tests/test_utils/test_utils_misc.py`:
     - Call `get_partitions_from_path(path)` without `partitioning`.
     - Expect a **dict** of `{"year": "2023", "month": "12"}` etc.
   - **Impact:** All partition-detection tests will fail; public API semantics effectively changed.
   - **Suggestion:**
     - Decide on one canonical API:
       - Option A: default to `partitioning="hive"` and return `dict[str, str]`.
       - Option B: keep `list[tuple]` but adjust tests and docstrings.
     - Stronger suggestion: keep the dict-returning behaviour (aligns with test names & expected values).

2. **Path parsing is POSIX-only and breaks for Windows-style paths**
   - Current logic:
     ```python
     if "." in path:
         path = os.path.dirname(path)
     parts = path.split("/")
     ```
   - For a Windows path like `"C:\\data\\year=2023\\month=12\\file.parquet"`, `split("/")` leaves the string intact, so no `key=value` segments are found.
   - Tests explicitly cover Windows paths and expect correct partition detection.
   - **Suggestion:** Normalize to POSIX separators (`Path(path).as_posix()`) or use `os.path` / `pathlib.Path.parts` consistently before scanning for `key=value`.

3. **Type hints and docstring still mention list-of-tuples, but tests speak in dicts**
   - This appears to be a partial refactor where tests and implementations diverged.
   - **Suggestion:** Align type hints, docstring, and tests on a single shape (`dict` is more ergonomic).

### 1.4 Core I/O extensions (`core.ext`)

**File:** `src/fsspeckit/core/ext.py`

1. **`_read_json` parallel branch is effectively dead**
   - In the list-path branch:
     ```python
     if isinstance(path, list):
         if use_threads:
             data = run_parallel(...)
         data = [
             _read_json_file(...)
             for p in path
         ]
     ```
   - The second assignment to `data` unconditionally re-reads all files sequentially, discarding the parallel results.
   - **Impact:** No performance benefit from `use_threads=True`; duplicated I/O.
   - **Suggestion:** Make the second assignment part of an `else:` block, or refactor into a shared helper that takes a configured iterator.

2. **`_read_parquet_file` and `_read_parquet_batches` use `pq`/`pa` without importing them**
   - `_read_parquet_file` calls `pq.read_table(...)` and `_read_parquet_batches` uses `pa.concat_tables(...)`.
   - The module only imports `pyarrow.parquet as pq` inside `write_parquet`, not at the top level; `pa` is never imported at runtime.
   - The `TYPE_CHECKING` imports (`import pyarrow as pa`, `import pyarrow.parquet as pq`) do not define runtime names.
   - **Impact:** Any call into Parquet-reading helpers will hit `NameError` for `pq`/`pa`.
   - **Suggestion:** Add runtime imports at module level or inside these functions:
     ```python
     from fsspeckit.common.optional import _import_pyarrow
     pa = _import_pyarrow()
     import pyarrow.parquet as pq
     ```

3. **`write_json` uses `orjson` without importing it**
   - `write_json` converts various input types to Python dict/list and then calls:
     ```python
     f.write(orjson.dumps(data) + b"\n")
     ```
   - There is no `orjson` import in scope; only `_read_json_file` performs `orjson = _import_orjson()` locally.
   - **Impact:** `NameError: name 'orjson' is not defined` the first time `write_json` is called.
   - **Suggestion:** Mirror `_read_json_file`’s pattern inside `write_json`:
     ```python
     from fsspeckit.common.optional import _import_orjson
     orjson = _import_orjson()
     ```

4. **JSON/CSV helpers assume Polars is available despite optional dependency model**
   - `_read_csv_file`, `_read_csv`, `_read_json`, `_read_json_batches` all use `pl` directly.
   - At module import time:
     ```python
     try:
         from fsspeckit.common.polars import opt_dtype as opt_dtype_pl, pl
     except ImportError:
         opt_dtype_pl = None
         pl = None
     ```
   - If Polars is missing, `pl` is `None`, and calling any of these functions will raise `AttributeError` or `TypeError`.
   - This contradicts the README’s “optional dependency” narrative; users might expect CSV/JSON helpers to work at least in a non-Polars mode.
   - **Recommendation:**
     - Either:
       - Treat these helpers as Polars-only and explicitly guard at entry points with a friendly `ImportError` that directs users to install `fsspeckit[datasets]`.
     - Or:
       - Make them produce PyArrow tables when Polars is absent.

5. **Debug `print(path)` left in `_read_csv_file`**
   - This print will emit file paths on every CSV read.
   - **Suggestion:** Replace with optional logging via `loguru` or remove entirely.

### 1.5 Filesystem utilities (`core.filesystem`)

**File:** `src/fsspeckit/core/filesystem.py`

1. **Monitored cache filesystem overrides `__getattribute__` in a brittle way**
   - `MonitoredSimpleCacheFileSystem.__getattribute__` forwards many attributes to the underlying filesystem, with special-casing for methods, properties, and class attributes.
   - It adds new items (e.g. `"size"`, `"glob"`) to the list of special-case methods.
   - While this is mostly copied from `fsspec`’s caching implementations, it’s highly error-prone:
     - Easy to miss attributes that should be forwarded.
     - Subclassing becomes tricky.
     - Debugging attribute resolution is hard.
   - **Suggestion:** Where possible, reuse `fsspec`’s existing cache filesystem rather than reimplementing the forwarding logic; or keep the override minimal and well-documented.

2. **`filesystem` has complex base_fs + dirfs + protocol inference logic**
   - The `filesystem` function handles:
     - String or `Path` input
     - Protocol inference from URIs
     - Deriving a `DirFileSystem` when `base_fs` is provided
     - Path normalization to prevent escaping base directories
     - Caching behaviour
   - Tests in `tests/test_basic.py` cover many edge cases and currently appear to match the implementation.
   - However, the logic is dense (`_smart_join`, `_is_within`, `_protocol_matches`, etc.) and spread across many helpers.
   - **Suggestion:** Logically the implementation is sound, but:
     - Extract the base-fs/DirFS logic into a small, focused helper with a dedicated docstring and tests.
     - Reduce duplication between the `base_fs` and `protocol_or_path` branches by normalizing early (e.g. always derive a logical “base path” + protocol) and branching only on whether a `base_fs` is present.

### 1.6 Dataset maintenance (PyArrow & DuckDB)

**Files:**
- `src/fsspeckit/core/maintenance.py`
- `src/fsspeckit/datasets/pyarrow.py`
- `src/fsspeckit/datasets/duckdb.py`

Overall, the maintenance and merge layers look well-structured and consistent between PyArrow and DuckDB, but some points stand out:

1. **Potential double-reading of datasets in compaction/optimization flows**
   - Both PyArrow and DuckDB maintenance APIs:
     - Collect stats via `collect_dataset_stats` (which opens each Parquet file to read metadata / row counts).
     - Later re-open the same files to actually read data for compaction/optimization.
   - This is unavoidable to some extent, but consider:
     - Caching the schema and basic metadata from the first pass to avoid re-reading it when planning.
     - Using `_metadata` files or dataset-level `pq.ParquetDataset` when available to reduce per-file overhead.

2. **Error reporting uses bare `print` statements**
   - Both backends use `print(...)` for warning messages (e.g. failed file deletions).
   - **Suggestion:** Route these warnings through the same logging system as the rest of the package (`loguru` via `common.logging`), potentially with a dedicated logger for maintenance operations.

---

## 2. Optional Dependency Handling

### 2.1 `common.optional` is clean, but other modules bypass it

**File:** `src/fsspeckit/common/optional.py`

- This module sets a good pattern:
  - Uses `importlib.util.find_spec` to set availability flags.
  - Provides `_import_*` helpers that raise targeted `ImportError` with clear “pip install fsspeckit[extra]” messages.
  - Tests in `tests/test_optional_dependencies.py` validate both availability flags and error messages.

**Issues:**

1. Other modules (notably `common.misc` and `core.ext`) import optional dependencies directly at module import time, undermining the lazy import model.
2. There is a **second** `check_optional_dependency` in `common.misc` with a different message (“install fsspeckit[full]”), which doesn’t match any extras defined in `pyproject.toml`.

**Recommendations:**

- Consolidate all optional dependency checks through `common.optional`:
  - Remove or deprecate `common.misc.check_optional_dependency`, or make it a thin wrapper around `optional.check_optional_dependency`.
  - Ensure messaging matches actual extras: `[aws]`, `[gcp]`, `[azure]`, `[datasets]`, `[sql]`; avoid referencing `[full]` unless that extra is added.
- For modules that are optional-feature-heavy (`core.ext`, `datasets.pyarrow`, `datasets.duckdb`), gate imports using `_import_*` helpers instead of direct imports wherever possible.

---

## 3. Backwards Compatibility & Tests

### 3.1 `fsspeckit.utils` façade limitations

**File:** `src/fsspeckit/utils/__init__.py`

- This module re-exports a curated set of symbols for backwards compatibility.
- Tests still reference legacy module paths (e.g. `"fsspeckit.utils.misc.Progress"`) that no longer exist.

**Implications:**

- Some existing user code may also rely on deeper `fsspeckit.utils.*` imports.
- The façade currently only exposes function-level names, not the old module hierarchy.

**Suggestions:**

- Consider adding small wrapper modules under `fsspeckit/utils/` (e.g., `misc.py`, `datetime.py`) that import from `fsspeckit.common` and re-export the same symbols.
- Alternatively, clearly document that only top-level `fsspeckit.utils` imports are stable and adjust tests accordingly.

### 3.2 Test expectations vs implementation

At least three clear mismatches exist between tests and implementation:

1. `get_timestamp_column` pandas support (see 1.1.1).
2. `get_timedelta_str` invalid-unit handling (see 1.1.2).
3. `get_partitions_from_path` return type and behaviour (see 1.3).
4. `run_parallel` generator support and error messages (see 1.2).
5. `Progress` patching using `"fsspeckit.utils.misc.Progress"` (see 1.2.4).

**Recommendation:** Decide whether tests or implementation are the source of truth and bring them back into alignment. Given that tests encode explicit behaviour and messages, they are a good reference point for API stability.

---

## 4. Complexity & Architecture Simplification Opportunities

### 4.1 `core.ext` as a monolithic “kitchen sink”

**File:** `src/fsspeckit/core/ext.py`

- This module currently handles:
  - JSON/CSV/Parquet reading and batching.
  - PyArrow dataset helpers.
  - Polars DataFrame conversions.
  - Dataset writing to Parquet and JSON/CSV.
  - Dataset writing to PyArrow datasets.
  - Specialised helpers like `pyarrow_dataset`, `pyarrow_parquet_dataset`, `write_dataset`, etc.
- It also monkey-patches methods onto `AbstractFileSystem` at the bottom:
  ```python
  AbstractFileSystem.read_json = read_json
  AbstractFileSystem.read_parquet = read_parquet
  # ...
  ```

**Consequences:**

- The file is very long and hard to navigate.
- A bug in one helper (e.g., missing `orjson` import) can break unrelated functionality by failing module import.
- Optional dependencies are intertwined (Polars, PyArrow, Pandas, orjson).

**Simplification ideas:**

1. Split `core.ext` by format:
   - `core.ext_json` (JSON/JSONL helpers).
   - `core.ext_csv` (CSV helpers).
   - `core.ext_parquet` (Parquet helpers and dataset integration).
   - Keep a thin `core.ext` that only wires all of them into `AbstractFileSystem`.
2. Defer monkey-patching of `AbstractFileSystem` behind an explicit opt-in function (e.g., `fsspeckit.core.register_extensions()`), so importing the module does not alter global classes by default.
3. Aggressively use `_import_*` from `common.optional` in each submodule to keep failures localized (e.g., JSON helpers should not break if DuckDB is missing).

### 4.2 `common.polars` and `datasets.pyarrow` parallelism

**Files:**
- `src/fsspeckit/common/polars.py`
- `src/fsspeckit/datasets/pyarrow.py`

- Both modules implement similar functionality:
  - Type inference and schema optimisation.
  - Unnest/explode helpers.
  - Schema unification.
  - Partitioning helpers.
- This is deliberate (Polars vs PyArrow), but the logic is quite intricate:
  - Complex regex-based type inference.
  - Multiple sampling strategies.
  - Timezone standardisation logic including multi-schema majority voting.

**Suggestions:**

1. Extract shared concepts into small, pure functions where possible (e.g., the regex constants and datetime detection rules could live in a shared `schema_inference` module).
2. For readability, group related functions with short headings in the file (e.g., “numeric optimisation”, “datetime inference”, “schema unification”), and keep monkey-patching of Polars DataFrame methods (`pl.DataFrame.opt_dtype = opt_dtype`) at the bottom in a clearly separated section.
3. Consider adding simple public wrapper functions (`opt_dtype_polars(df, ..., strict=False)`) that are easier to test and document than a large multipurpose `opt_dtype`.

### 4.3 Storage options layering

**Files:**
- `src/fsspeckit/storage_options/base.py`
- `src/fsspeckit/storage_options/core.py`
- `src/fsspeckit/storage_options/cloud.py`
- `src/fsspeckit/storage_options/git.py`

This area is generally clean and well-structured. Two minor observations:

1. `merge_storage_options` currently returns a concrete type based solely on the first protocol it sees; error handling for conflicting protocols is implicit (later values silently overwrite).
   - Could optionally validate that all `BaseStorageOptions`/dicts agree on `protocol`, or raise if conflicting.
2. `AwsStorageOptions` has multiple ways to specify “allow invalid certs” (`allow_invalid_certificates`, `allow_invalid_certs`) and uses `_parse_bool` to unify.
   - This is robust but verbose; consider documenting a single preferred parameter and marking the alias as deprecated in the docstring.

---

## 5. Suggested Next Steps

1. **Stabilize APIs vs tests**
   - Fix `get_timestamp_column`, `get_timedelta_str`, `get_partitions_from_path`, `run_parallel`, and `write_json` so that tests pass and behaviour matches expectations.
   - Add missing imports in `core.ext` (`pyarrow`, `orjson`).
2. **Re-align optional dependency handling**
   - Make `joblib` truly optional via lazy imports.
   - Ensure JSON/CSV/Parquet helpers fail gracefully with clear errors when required extras are missing.
3. **Introduce small shims for backward compatibility**
   - Decide whether the project wants to support deeper `fsspeckit.utils.*` imports; if yes, add thin wrappers, otherwise update tests and docs.
4. **Progressively modularize `core.ext`**
   - Start by separating read/write helpers by format into smaller modules.
   - Defer monkey-patching of `AbstractFileSystem` into a dedicated registration function.

Addressing these items will remove several real bugs, bring tests back into alignment, and make the codebase easier to evolve without breaking optional-dependency or backward-compat guarantees.

