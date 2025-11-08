# Design: Parquet Dataset Maintenance (Compaction & Z-Order Optimization)

## 1. Context
Large analytical DuckDB-backed parquet datasets often suffer from the "small files problem"—many tiny parquet files increase planning latency, metadata scanning overhead, and reduce the effectiveness of predicate pushdown and data skipping statistics. Additionally, row ordering across files is typically incidental (append order), leading to poor locality for multi-column selective filters (e.g., customer_id + event_date). Systems like Delta Lake, Iceberg, and Databricks provide OPTIMIZE with z-order clustering; we provide a lightweight, file-rewrite based approximation tailored to our existing DuckDB handler without adding table metadata layers or transaction protocols.

This change is additive, building on existing write/append/merge capabilities to complete the dataset lifecycle: write → merge → optimize/compact.

## 2. Goals
- Provide `compact_parquet_dataset` to consolidate small files based on target MB or row thresholds.
- Provide `optimize_parquet_dataset` to reorder data using a z-order style multi-column clustering approximation.
- Support simultaneous optimization+compaction in a single rewrite pass.
- Offer dry-run planning for safety (no data modification) with clear plan/metrics.
- Preserve schema and optionally change compression codec during rewrite.
- Return structured statistics enabling observability & regression tracking.
- Maintain low external dependency surface (DuckDB + PyArrow only).

## 3. Non-Goals
- Implement true Morton code bit-interleaving encoding for z-order (approximation only via multi-column ordering strategy).
- Provide ACID/transaction guarantees, snapshot metadata, or manifest management (out of scope; we operate directly on file set with overwrite semantics).
- Partition evolution or automatic partition discovery/refactoring.
- Automatic adaptive threshold tuning (static user-provided values only).
- Cross-dataset coordination or concurrency management beyond basic input validation.

## 4. Key Concepts
- Small File Grouping: Identify candidate files below threshold (size or rows) and group sequentially until target aggregate size/row count reached; each group becomes one output file.
- Z-Order Approximation: Rather than encoding into a single interleaved key, perform a stable multi-phase ordering producing interleaved locality:
  1. Normalize clustering columns (handle nulls consistently: push nulls last).
  2. Build a composite tuple key (col1, col2, ..., colN) and apply multi-column sort. For high-cardinality leading columns this yields acceptable locality improvement for typical filter combinations.
  3. (Future) Could evolve to space-filling curve encoding.
- Dry Run: Executes discovery, grouping, ordering planning, and statistics estimation without performing writes; returns plan with proposed file groups and counts.
- Statistics Object: Dict capturing before/after file counts & bytes plus operation-specific fields (zorder_columns, compression_codec, compacted_file_count, rewritten_bytes, dry_run flag).

## 5. Detailed Design
### 5.1 API Surface
```
compact_parquet_dataset(
  path: str,
  target_mb_per_file: Optional[int] = None,
  target_rows_per_file: Optional[int] = None,
  partition_filter: Optional[Sequence[str]] = None,
  compression: Optional[str] = None,
  dry_run: bool = False,
) -> Dict[str, Any]

optimize_parquet_dataset(
  path: str,
  zorder_columns: Sequence[str],
  target_mb_per_file: Optional[int] = None,
  target_rows_per_file: Optional[int] = None,
  partition_filter: Optional[Sequence[str]] = None,
  compression: Optional[str] = None,
  dry_run: bool = False,
) -> Dict[str, Any]
```

Validation rules:
- At least one of `target_mb_per_file` or `target_rows_per_file` must be provided for compaction (optimize may rely on size only; rows optional).
- Thresholds must be > 0 when provided.
- `zorder_columns` must all exist in schema; empty sequence invalid.
- Path must exist and contain parquet files; else FileNotFoundError.
- Partition filter entries must match directory prefixes; non-matching filter results in no files (treated as no-op).

### 5.2 File Discovery & Metadata Collection
Helper `_collect_dataset_stats(path, partition_filter)`:
- Enumerate files under path (respecting partition filter prefixes).
- Read footer metadata (size, row count) using PyArrow / DuckDB PRAGMA or reading into Arrow to avoid full scan when possible.
- Aggregate total bytes, total rows, produce list of file descriptors: `{path, size_bytes, row_count}`.
- Early exit if <2 files or all files already exceed thresholds (compaction no-op).

### 5.3 Grouping Algorithm (Compaction)
Input: list of candidate files sorted by size ascending.
Algorithm:
```
current_group = [] ; current_size = 0 ; groups = []
for file in candidates:
  if file.size >= threshold: continue (already large)
  if current_size + file.size > threshold and current_group:
      groups.append(current_group)
      current_group = [] ; current_size = 0
  current_group.append(file)
  current_size += file.size
end
if current_group: groups.append(current_group)
```
Row-based threshold variant replaces `size` with `row_count` logic. For simultaneous thresholds (both provided), treat group boundary when either would exceed threshold.

### 5.4 Rewrite Execution
For each group:
- Read all group files into DuckDB (parquet_scan) or Arrow Table concatenation.
- If optimize path: perform ordering step before writing.
- Write output file name pattern: `part-<i>-c<groupsize>-optimized.parquet` (or `part-<i>-compact.parquet` if only compaction).
- Use DuckDB COPY or PyArrow write with selected compression codec (`compression or original detected codec`); preserve schema.
- After successful write: delete original group files (atomicity: best-effort; partial failure leaves duplicates—documented risk).
Optimization path rewriting all files: treat entire dataset as one logical set; apply grouping after ordering if thresholds provided.

### 5.5 Z-Order Approximation Strategy
- Load dataset into DuckDB temp view.
- Ordering expression: `ORDER BY col1, col2, ..., colN` (simple composite ordering). Rationale: DuckDB lacks native z-order; simple multi-column ordering improves locality for common compound predicates.
- Null handling: `ORDER BY (col1 IS NULL), col1, (col2 IS NULL), col2` ensures nulls cluster at end improving skip for non-null filters.
- Potential improvement: chunk-based reordering to avoid full memory materialization for extremely large datasets (future enhancement).

### 5.6 Dry Run Behavior
Skip read & write heavy operations; still perform discovery, grouping simulation, and schema validation. For optimize dry-run, omit ordering execution. Return stats with `dry_run=True` and include `planned_groups` (list of lists of source file paths) and `estimated_after_file_count`.

### 5.7 Statistics Schema
Common keys: `before_file_count`, `after_file_count` (dry-run uses estimated), `before_total_bytes`, `after_total_bytes` (dry-run estimates unchanged), `compacted_file_count`, `rewritten_bytes`, `compression_codec`, `dry_run`. Optimization adds: `zorder_columns`.

### 5.8 Partition Filtering
If `partition_filter` provided (list of directory prefix strings like `date=2025-11-04`): limit discovery to paths where relative path starts with any filter prefix. Simple string match; does not parse hive partitions.

### 5.9 Error Handling & No-Op Conditions
- No candidates for compaction → return stats with `compacted_file_count=0` and identical before/after counts.
- Optimize with dataset already ordered: optionally detect via sampling first N rows per file and verifying non-decreasing order for leading column; if >90% ordered treat as no-op (future; initial version may always rewrite unless trivial single file dataset).

### 5.10 Concurrency Considerations
We assume exclusive access during maintenance. If concurrent writers append during rewrite, their files may be excluded or overwritten if name collision occurs. Document recommendation: run maintenance in isolated environment. No locking implemented.

## 6. Trade-offs & Risks
- I/O intensive: full rewrites can be expensive; mitigated by dry-run planning.
- Approximate z-order yields less clustering benefit than true space-filling curve; acceptable for initial release to avoid complexity.
- Potential memory pressure for very large datasets during optimize; mitigation: future streaming/chunking enhancement.
- Deletion of originals after write lacks transactional safety; power failure risk leaving duplicates—user should validate via stats and backups.
- Compression recompression may increase CPU cost; optional parameter defaults to preserving existing codec when unset.

## 7. Performance Notes
- Compaction reduces file open and metadata scanning overhead (O(n) files → O(k)).
- Multi-column ordering improves page-level locality, aiding predicate pruning inside DuckDB's parquet reader.
- Grouping algorithm linear in number of files; rewrite cost dominated by scan+write throughput.

## 8. Migration & Rollout
- Purely additive: existing APIs unchanged.
- Recommend initial dry-run on staging datasets; review `planned_groups` and estimated file count changes.
- Monitor query performance metrics (execution time, files scanned) before/after.

## 9. Observability
Return stats dict enabling logging. Suggested user log line: `Maintenance: compacted {compacted_file_count} files; file count {before_file_count}->{after_file_count}; bytes {before_total_bytes}->{after_total_bytes}`.

## 10. Open Questions
- Default threshold values: Should we supply defaults (e.g., 128MB size) if user omits? Currently require explicit value.
- Detection of "already optimized" state: implement heuristic now or defer?
- Consider adding metadata manifest for safer rewrites in future.
- Support true z-order encoding (Morton code) for numeric columns later.
- Parallelism: Should we multi-thread group rewrites? Initial single-thread for simplicity.

## 11. Future Enhancements
- Adaptive grouping by target row group size & statistics.
- Streaming reorder using external sorting for very large datasets.
- Incremental maintenance (only newly appended small files).
- Space-filling curve key generation for high-dimensional clustering.

## 12. Alternatives Considered
- Using external frameworks (e.g., Delta Lake) rejected to keep dependency footprint minimal.
- Relying solely on user scripts or recommending external tooling rejected to reduce operational burden and provide integrated experience.

## 13. Summary
Introduce two maintenance operations (compaction + z-order style optimization) with safety (dry-run), observability (stats), and minimal complexity (approximate ordering). Establishes foundation for advanced layout optimizations in future without breaking current users.
