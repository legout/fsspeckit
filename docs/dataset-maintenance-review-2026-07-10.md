# Dataset maintenance and dtype-optimization review

**Reviewed:** 2026-07-10
**Scope interpretation:** the request refers to this repository's `fsspeckit.datasets` layer (there is no `fsspec.dataset` module here). The review covers public and backend-level Parquet compaction, persisted-dataset deduplication, and `optimize_*` operations, plus the public PyArrow/Polars dtype and schema-optimization helpers. `merge_parquet_dataset_pyarrow` is explicitly excluded.

## Executive assessment

The maintenance layer has a sensible direction: shared file discovery, greedy planning, canonical result data, backend adapters, dry-run support for compaction/deduplication, and an attempt to avoid whole-dataset materialization. However, it is **not safe for partitioned datasets or failure-prone production use** in its current form.

The most important issues are confirmed data-integrity defects:

1. **PyArrow dtype optimization drops nulls, changing row count and potentially fabricating row pairings.**
2. **Compaction writes all replacements into the dataset root, destroying Hive partition layout.**
3. **Deduplication can mix rows from different Hive partitions and write them under a single partition path.**
4. **Compaction and deduplication have no atomic commit/rollback protocol.** Interrupted operations can expose duplicate, partial, or missing data.
5. **Chunked PyArrow deduplication with two or more ordering columns can remove every row for a key.**

The current implementation should be treated as suitable only for **flat, single-writer, recoverable datasets**, after the dtype optimizer is fixed or disabled. For partitioned datasets, do not run compaction or persisted-dataset deduplication until partition-aware rewrite and commit semantics are implemented.

---

## 1. Inventory, scope, and intended interactions

### Public maintenance surface

| Operation | Module/function | Handler method | Intended behavior |
|---|---|---|---|
| Statistics | `datasets.pyarrow.dataset.collect_dataset_stats_pyarrow` | backend internal wrapper | Recursive Parquet-file discovery, bytes/row counts, optional path-prefix partition filter. |
| Compaction | `datasets.pyarrow.dataset.compact_parquet_dataset_pyarrow` | `PyarrowDatasetIO.compact_parquet_dataset` | Greedily combine small files by size and/or row-count target. |
| Compaction | `datasets.duckdb.dataset.compact_parquet_dataset_duckdb` | `DuckDBDatasetIO.compact_parquet_dataset` | Same contract, nominally DuckDB-backed. |
| Deduplication | `datasets.pyarrow.dataset.deduplicate_parquet_dataset_pyarrow` | no equivalent PyArrow handler method | Remove exact duplicates or keep one row per key; optional ordering chooses a winner. |
| Deduplication | `DuckDBDatasetIO.deduplicate_parquet_dataset` | `DuckDBDatasetIO.deduplicate_parquet_dataset` | Equivalent persisted-dataset operation using DuckDB temporary tables. |
| Optimization | `optimize_parquet_dataset_pyarrow` / `DuckDBDatasetIO.optimize_parquet_dataset` | both backends | Optional deduplication followed by compaction; **not** sort, z-order, or repartitioning. |
| Unified fsspec extension | `core.ext.dataset.deduplicate_parquet_dataset` | `fs.deduplicate_parquet_dataset` | Chooses DuckDB when importable, otherwise PyArrow. |
| Dtype/schema optimization | `datasets.schema.opt_dtype`, `datasets.polars.opt_dtype`, schema helper exports | n/a | Downcast/infer types and normalize or unify schemas. |

The declared handler protocol includes compaction and optimization (`src/fsspeckit/datasets/interfaces.py:118-176`) but **omits deduplication**, even though the DuckDB handler implements it. The root `fsspeckit.datasets` package exports PyArrow compaction and optimization but not persisted PyArrow deduplication (`src/fsspeckit/datasets/__init__.py:27-39, 74-99`); the narrower `fsspeckit.datasets.pyarrow` package does export it (`src/fsspeckit/datasets/pyarrow/__init__.py:17-49`).

### Actual execution model

1. `collect_dataset_stats` manually recursively lists `*.parquet`, reads file metadata, and applies `partition_filter` as a relative-path string-prefix filter (`src/fsspeckit/core/maintenance.py:195-312`).
2. `plan_compaction_groups` and `plan_deduplication_groups` sort files by compressed byte size and greedily pack them until target bytes or rows would be exceeded (`:315-451`, `:612-758`). Compaction discards singleton groups; deduplication processes them.
3. The shared compaction executor makes a UUID root-level output for every group, invokes a backend callback for **all** groups, then deletes **all** originals in a later loop (`:887-927`).
4. PyArrow compaction reads every file in a group into `pa.Table` objects and concatenates them before writing one Parquet file (`src/fsspeckit/datasets/pyarrow/dataset.py:484-504`). It is group-bounded, but not streaming.
5. PyArrow persisted deduplication uses an in-memory path or a chunked path. The latter retains a growing table of all distinct keys or best values in memory (`:719-962`).
6. DuckDB’s free compaction function uses `parquet_scan` and `COPY` (`src/fsspeckit/datasets/duckdb/dataset.py:100-239`), but the handler method instead reads and concatenates all group files in PyArrow then writes through DuckDB (`:1116-1202`).
7. Both `optimize` implementations optionally deduplicate, ignore that result, then compact (`PyArrow :560-586`; DuckDB `:1230-1258`). `plan_optimize_groups` is dead code: it is defined in `core/maintenance.py:454-609` but has no caller under `src/`.

---

## 2. Confirmed critical findings

### C1 — `opt_dtype` corrupts null-containing PyArrow tables

**Evidence:** `_process_column` drops nulls (`array.drop_null()`) and returns the shortened array for strings, numeric columns, and all unsupported column types (`src/fsspeckit/datasets/schema.py:1160-1175`). `opt_dtype` then constructs a new table from those shortened arrays (`:1231-1249`).

**Reproduced in the current environment:**

- Input `a=[1, None, 3]`, `b=['1', None, '3']` (3 rows) produced a 2-row output.
- Input `a=[1, None, 3]`, `b=['1', '2', None]` also produced a 2-row output. The independent null removal changes the original pairings to `(1, 1), (3, 2)`.

**Impact:** Silent row loss and relational corruption. The defect affects every null-containing type passed to `opt_dtype`, including unsupported timestamp, boolean, binary, decimal, list, and struct columns because the fallback returns `non_null`.

**Recommendation:** Block `opt_dtype_pa` in read/write paths until fixed. Preserve the full `ChunkedArray`; perform detection on non-null values only, but apply a safe cast to the full original array so null positions remain unchanged. Add an invariant test: input and output have identical row counts and null masks for every column.

---

### C2 — Shared compaction breaks Hive partition layout

**Evidence:** `execute_compaction_template` always generates `dataset_path/compacted-<uuid>.parquet` (`src/fsspeckit/core/maintenance.py:917-921`) and only then removes original files (`:923-925`). It carries no partition identity in `CompactionGroup`.

**Reproduced in the current environment:** compacting two files under `part=A/` with `partition_filter=['part=A']` produced:

```text
compacted-<uuid>.parquet
part=B/one.parquet
```

A subsequent `collect_dataset_stats(..., partition_filter=['part=A'])` raised `FileNotFoundError` because no output remained below `part=A/`.

**Impact:** Partition pruning, maintenance filtering, and path-derived partition semantics break. The same executor is used by PyArrow compaction and the module-level DuckDB compaction, and the DuckDB handler duplicates the same root-write behavior.

**Recommendation:** Make partition identity first-class in discovery/planning. Group only files with the same partition tuple, stage/write output back into that partition directory, and include a test that a filtered maintenance pass leaves a queryable filtered partition afterward. Do not try to repair this by merely retaining partition columns inside the file; physical partition layout is part of the API contract.

---

### C3 — Persisted deduplication is unsafe across partitions

**Evidence:** The deduplication planner is global by default (`target_*` is omitted at `src/fsspeckit/datasets/pyarrow/dataset.py:672-677`); with no threshold, all files are one group (`core/maintenance.py:660-710`). PyArrow overwrites `group.files[0].path` and removes the other group members (`pyarrow/dataset.py:764-775`). DuckDB does the same via `COPY` to `group_files[0]` then removal (`duckdb/dataset.py:1426-1439`).

**Impact:** Rows from partition B can be written into partition A’s physical directory. If partition fields exist both in files and as Hive directories, PyArrow can fail with an incompatible dictionary/string partition-field type; the existing partitioned correctness test currently fails this way.

**Recommendation:** Require one of three explicit semantics:

1. **Partition-local deduplication**: include the physical partition tuple in grouping and deduplicate only inside it.
2. **Global deduplication with repartition**: read all relevant rows, select winners, then write a fresh staged dataset partitioned by specified partition columns.
3. **Flat datasets only**: reject paths with Hive-style partitions until either option is implemented.

Never let a global group overwrite a file selected simply by its position in the group.

---

### C4 — No atomicity, rollback, or concurrency safety for maintenance writes

**Evidence:** The shared executor writes every group before removing originals, with no exception handling or cleanup (`core/maintenance.py:917-925`). The PyArrow in-memory dedup path overwrites the first input file before deleting the others (`pyarrow/dataset.py:764-775`); its chunked path deletes all originals before moving its temporary output (`:958-962`). DuckDB dedup follows a similar overwrite-then-delete sequence (`duckdb/dataset.py:1426-1439`).

**Failure modes:**

- Write failure after an earlier group succeeds: staged-like new files exist alongside all originals for successful groups, duplicating rows.
- Deletion failure: some originals remain alongside new output, duplicating rows.
- Chunked dedup process loss between deleting originals and moving the temp file: a group disappears.
- Two maintenance jobs on the same dataset can race on discovery, output/delete operations, and no lock or commit version prevents it.

**Recommendation:** Replace direct in-place replacement with a versioned commit protocol:

- Write immutable outputs under a job-specific staging prefix/directory.
- Validate row counts, schema, partition paths, and output files before commit.
- For POSIX: atomically swap a manifest/pointer or directory only within one filesystem.
- For object stores: publish a new manifest/version atomically, retain old files until readers have moved, and garbage-collect later.
- On any error, remove only the job staging prefix; leave the committed dataset untouched.
- Document and enforce a dataset-level single-writer lock or optimistic version check.

DuckDB’s `COPY` documents `USE_TMP_FILE` for a safer individual file overwrite, but this still does not provide an atomic multi-file dataset commit.
Source: <https://duckdb.org/docs/current/sql/statements/copy.html>

---

### C5 — Chunked PyArrow custom ordering can drop every winning row

**Evidence:** For custom ordering, the chunked branch aggregates each ordering field independently with `min`/`max` (`src/fsspeckit/datasets/pyarrow/dataset.py:859-897`) and then inner-joins rows on the **combined** key and aggregate values (`:902-954`). Independent maxima need not coexist in one source row.

**Reproduced:** For one key with rows:

| id | priority | tie | payload |
|---:|---:|---:|---|
| 1 | 10 | 1 | priority-winner |
| 1 | 9 | 100 | tie-winner |

With `dedup_order_by=['-priority', '-tie']` and `chunk_size_rows=1`, the operation reported `total_rows_after=0` and wrote an empty table. Correct lexicographic ordering should retain the first row.

**Recommendation:** Do not aggregate independent order fields. Implement lexicographic ranking with a stable tie-breaker (for example source-file ordinal plus row ordinal), then retain `row_number() == 1` per key. For data larger than memory, use external sorting or a database engine with disk spill; a per-key max/min aggregate is not a substitute for a composite ordering.

---

## 3. High-priority correctness, contract, and scalability findings

### H1 — `dedup_order_by` semantics differ or fail by backend

- **PyArrow default/Polars fallback:** `_table_drop_duplicates` falls back to `pl.DataFrame.unique` without an order-preservation flag (`src/fsspeckit/datasets/pyarrow/dataset.py:156-182`). Existing test `test_consistent_ordering_across_runs` fails: different runs retain the same key winners but emit different row order.
- **PyArrow exact dedup + custom ordering:** `dedup_order_by` without `key_columns` enters a code path that calls `group_by(None)`. Current reproduction raises `TypeError: object of type 'NoneType' has no len()` instead of rejecting the unsupported combination (`pyarrow/dataset.py:859-893`).
- **DuckDB static review (DuckDB extra is unavailable in this environment):** documented `['-timestamp']` is quoted as the literal identifier `"-timestamp"` (`src/fsspeckit/datasets/duckdb/dataset.py:1398-1404`). Direction is not parsed. When no explicit ordering is supplied, `SELECT DISTINCT ON` lacks `ORDER BY` (`:1393-1409`), so the retained row is not deterministic.

**Recommendation:** Define one cross-backend contract: either require an explicit complete order for key deduplication or specify stable first-seen order with a deterministic file/row ordinal. Parse `+column`/`-column` exactly once in common code and pass a typed direction structure to both backends. Reject `dedup_order_by` without keys unless exact-dedup ordering semantics are deliberately designed.

### H2 — The claimed "streaming" compaction path materializes complete groups

`compact_parquet_dataset_pyarrow` calls `pq.read_table` for every file and `pa.concat_tables` for the complete group (`src/fsspeckit/datasets/pyarrow/dataset.py:484-504`). The DuckDB handler method repeats this materialization (`duckdb/dataset.py:1175-1195`) even though the module-level DuckDB function can scan/copy with DuckDB.

The grouping limit uses compressed source-file bytes and row counts, neither of which bounds decoded Arrow memory, dictionary expansion, sort memory, or temporary output memory. Thus a group under the configured size threshold can still exhaust RAM.

**Recommendation:** Rename this as group-bounded rather than streaming, or implement actual batch/record-batch streaming. Unify the DuckDB handler with `compact_parquet_dataset_duckdb` so one backend does not silently switch to PyArrow materialization. Expose a memory budget and derive a conservative decoded-size/group limit from Parquet metadata.

### H3 — PyArrow chunked deduplication is not memory-bounded

The one-pass path retains `seen_keys` for every distinct key and repeatedly concatenates it (`pyarrow/dataset.py:810-851`). The custom-order path accumulates `best_values_table` globally (`:859-897`). Both are O(number of unique keys), and repeated `pa.concat_tables` can cause repeated copying. Memory checks happen after a scanner batch is created (`:1085-1112`) and `max_memory_mb` tracks Arrow allocations, not a hard process cap (`pyarrow/memory.py:78-160`).

**Recommendation:** For guaranteed memory bounds, hash-partition data by deduplication key into spillable shards; every duplicate key must map to one shard. Deduplicate each shard independently, then write staged output. Alternatives are a disk-backed key store or DuckDB configured with a memory limit and temporary directory, provided commit semantics are still fixed.

### H4 — "Optimize" is an incomplete and misleading result contract

Both backends implement optimize as optional deduplication plus compaction, without sort, z-order, clustering, repartitioning, or a dry-run option (`pyarrow/dataset.py:516-586`, `duckdb/dataset.py:1204-1258`). Deduplication statistics are discarded. Meanwhile `plan_optimize_groups` advertises z-order validation but is unused (`core/maintenance.py:454-609`; no call under `src/`).

**Recommendation:** Choose one contract:

- rename it to `maintain_parquet_dataset` / `deduplicate_and_compact`, or
- implement genuine clustering with explicit sort/z-order columns, a dry-run plan, partition-aware output, and combined dedup+compaction metrics.

Return nested `deduplication`, `compaction`, and `commit` statistics rather than discarding the first phase.

### H5 — Planning/reporting estimates are returned as execution facts

`plan_compaction_groups` computes `after_total_bytes` by reusing input bytes (`core/maintenance.py:424-443`) and execution returns that same `planned_stats` unchanged (`:927`). Compression can substantially alter output bytes; failures can alter file counts. `MaintenanceStats.to_dict` also omits defined deduplication fields (`key_columns`, `dedup_order_by`, and `deduplicated_rows`; `:106-154`).

**Recommendation:** Mark plans explicitly as estimates. After a successful staged write, compute actual output file count, bytes, schema, partition counts, and deduplicated row count; return those separately from the original plan. Serialize all fields or use operation-specific result dataclasses.

### H6 — API parity and dispatch are incomplete

- `DatasetHandler` lacks the deduplication method (`interfaces.py:118-176`).
- `PyarrowDatasetIO` lacks a `deduplicate_parquet_dataset` wrapper while DuckDB exposes one.
- The root datasets package omits PyArrow persisted deduplication; this was confirmed by import: `from fsspeckit.datasets import deduplicate_parquet_dataset_pyarrow` raises `ImportError`, while the `datasets.pyarrow` import succeeds.
- The `fsspec` extension accepts `**kwargs` but forwards none of the PyArrow memory/chunk/progress controls (`core/ext/dataset.py:159-245`). It creates DuckDB with no `filesystem=self` (`:211-228`), so non-local fsspec dispatch can use the wrong filesystem when DuckDB is installed.

**Recommendation:** Decide whether persisted deduplication is a core handler capability; if yes, add it to the protocol, both handler classes, root exports, and parity tests. Forward typed backend options and supply the caller filesystem to DuckDB connection creation. Use a backend-selection policy that fails predictably instead of silently changing filesystem semantics.

---

## 4. Dtype and schema-optimization review

### D1 — PyArrow schema unification crashes for valid time conflicts

`_handle_temporal_conflicts` calls `pa.time64()` without a required unit (`src/fsspeckit/datasets/schema.py:555-594`). `unify_schemas` does not catch `TypeError`. Reproduction with `time32('s')` plus `time64('us')` raises `TypeError: time64() takes exactly 1 positional argument (0 given)`.

**Fix:** pick a valid precision explicitly, normally the finest compatible unit, and add time32/time64 combinations to schema-unification tests. Prefer Arrow’s permissive unification where it already specifies behavior.

### D2 — Polars changes signed integers to unsigned by default

`opt_dtype` defaults to `shrink_numerics=False` and `allow_unsigned=True` (`src/fsspeckit/datasets/polars.py:483-493`), but non-negative integer columns are converted to `UInt64` independent of the shrink flag (`:74-112`). Reproduction: `Int64([1,2,3])` becomes `UInt64` with default options.

**Impact:** This is a semantic type change, not merely a conservative optimization; it can break joins and arithmetic expecting signed values.

**Fix:** require `shrink_numerics=True` for unsigned conversion, or make `allow_unsigned=False` the safe default.

### D3 — Dtype inference is expensive and unsafe on representative-sample assumptions

- PyArrow string inference converts each whole column to Python values and uses a Python regex loop (`schema.py:1086-1105`), defeating Arrow vectorization and potentially materializing large columns in host memory.
- The Polars path can infer a target type from only a sample then cast the whole column (`polars.py:483-576`); unrepresentative values can become null in non-strict mode.
- Random Polars sampling has no seed, so inferred schemas can vary across runs.
- Boolean strings are cast using Arrow’s direct string-to-bool cast (`schema.py:1134-1140`), which fails in current Arrow and silently leaves the strings unchanged.

**Fix:** use vectorized regex kernels (`pyarrow.compute.match_substring_regex` / Polars string expressions), validate the full column before a lossy conversion, make sampling deterministic, and make any coercive mode explicitly opt-in.

### D4 — Schema conversion/unification loses metadata or precision in fallbacks

`convert_large_types_to_normal` is not recursive and does not preserve schema metadata (`schema.py:89-150`). Numeric signed/unsigned conflicts can be promoted to floats, which loses integer precision for large values; timestamp conflicts return the first encountered unit rather than the highest precision (`:461-594`). Some fallback schema-unification paths treat default `timezone=None` differently and strip timezones.

**Fix:** implement a recursive Arrow type visitor that preserves field/schema metadata and make promotion rules range- and precision-aware. Use one normalized timezone policy on every fallback path.

---

## 5. Tests: observed status and critical gaps

### Executed verification

| Check | Result |
|---|---|
| Focused core template + PyArrow correctness selection | **2 failed, 5 passed**. The partitioned deduplication test fails with Arrow type conflict; the deterministic-order test fails because result order varies. |
| Filtered Hive-partition compaction repro | **Fails contract**: output moved to dataset root; reselecting the filtered partition raises `FileNotFoundError`. |
| Two-column custom-order chunked dedup repro | **Data loss**: 2 input rows became 0 output rows. |
| PyArrow `opt_dtype` null repro | **Data loss**: 3 rows became 2; misaligned nulls silently changed row pairing. |
| `unify_schemas(time32, time64)` repro | **Crash**: `TypeError`. |
| Polars default signedness repro | **Type drift**: `Int64` became `UInt64`. |
| DuckDB runtime repro | Not executed: this environment does not install the DuckDB optional extra. DuckDB findings above are static, source-backed findings and should be verified in the `sql` extra CI job. |

### Missing coverage

1. Partition-preservation tests for both compaction and deduplication, including a follow-up filtered read.
2. Failure injection: callback/write failure, `rm` failure, interrupted temp-file move, stale output, and concurrent maintenance jobs.
3. Cross-backend parity tests that compare the free DuckDB compaction function with `DuckDBDatasetIO.compact_parquet_dataset` on data, schema, partitions, stats, and output layout.
4. A full ordering matrix: default, `+col`, `-col`, multi-column lexicographic ordering, ties, exact dedup, and in-memory versus chunked path.
5. A genuine memory-bound test with high cardinality keys and wide rows; current benchmark-style tests do not prove bounded retained key state.
6. Dtype properties: row count/null-mask preservation, safe values preserved after cast, all inference modes, temporal conflicts, nested large types, metadata preservation, and deterministic sampling.
7. Remote fsspec filesystem tests for discovery, output, moves, and commit behavior.

---

## 6. Recommended remediation plan

### Phase 0 — stop data corruption (before expanding usage)

1. Disable/fix PyArrow `opt_dtype` on null-containing data; preserve full arrays and null masks.
2. Reject partitioned datasets in compaction/deduplication until a partition-safe implementation is present.
3. Fix chunked custom-order deduplication; add the two-column counterexample as a regression test.
4. Reject unsupported `dedup_order_by` combinations and normalize/order directions in common code.
5. Mark maintenance operations as single-writer/non-atomic in documentation until the commit protocol lands.

### Phase 1 — make physical maintenance correct

1. Introduce a `DatasetMaintenancePlan` that records source snapshot/version, partition tuple, source files, output schema, expected rows, and destination partition paths.
2. Build plans partition-by-partition for compaction. For global dedup, explicitly choose partition-local vs full-repartition semantics.
3. Write all output to a job-specific staging location. Validate it, publish a manifest/version, then garbage-collect old files asynchronously.
4. Unify `DuckDBDatasetIO.compact_parquet_dataset` with the module-level DuckDB implementation, eliminating the PyArrow-materializing duplicate path.
5. Return real post-commit statistics and preserve operation metadata in result objects.

### Phase 2 — improve scalability and actual optimization

1. Use batch streaming only where the algorithm is genuinely streaming; avoid claiming group concat is streaming.
2. For large deduplication, use hash sharding by key plus external/disk-backed processing, or a spill-capable engine. Maintain deterministic lexicographic ranking with a stable tie-breaker.
3. Implement real clustering only if required: explicit sort/z-order columns, per-partition rewrite, measured query benefits, and a dry-run plan. Otherwise rename the operation to accurately describe deduplication plus compaction.
4. Replace Python-regex dtype scans and Python timezone UDFs with vectorized Arrow/Polars expressions; make dtype coercion safe by default and configurable.

### Implementation alternatives, with primary references

- **PyArrow output writer:** `pyarrow.dataset.write_dataset` supports explicit partitioning, deterministic basename templates, `max_rows_per_file`, row-group controls, and `existing_data_behavior`. Use it to write staged partitioned output; it does **not** by itself make a multi-file dataset commit atomic.
  <https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html>
- **DuckDB partitioned output:** DuckDB `COPY` supports `PARTITION_BY`, UUID filename patterns, and `USE_TMP_FILE` for safer file replacement. Use it inside the staging/commit protocol, not as a substitute for one.
  <https://duckdb.org/docs/current/sql/statements/copy.html>
- **Stable dedup:** for a spill-capable SQL plan, calculate `row_number() over (partition by keys order by order_columns, stable_tiebreaker)` and retain rank 1. For PyArrow-only operation, hash-shard first, then stably sort each shard by the same composite order.

## Bottom line

The abstractions and planning work are useful foundations, but production readiness requires fixing the critical data-corruption paths before performance tuning. Prioritize null-safe dtype optimization, partition-aware planning/output, a durable multi-file commit protocol, and deterministic deduplication semantics. Only then should the project invest in true clustering or more aggressive dtype inference.
