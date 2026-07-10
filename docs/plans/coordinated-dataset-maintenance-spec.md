# Coordinated Dataset Maintenance Specification

## Problem Statement

Dataset maintenance currently combines planning, backend-specific rewrite logic, publication, and cleanup in ways that give different correctness, recovery, and filesystem behavior to compaction, deduplication, and optimization. Users need to compact and deduplicate both local datasets and S3-style object-store datasets without losing the existing fsspec-oriented capability.

The existing implementation does not provide one explicit contract for partition preservation, source drift, winner selection, validation, rollback, result semantics, or storage guarantees. In particular, callers cannot reliably tell whether a maintenance operation is atomic, whether concurrent changes are protected, or what artifacts are available after a failed rewrite.

The feature introduces coordinated physical maintenance with explicit plans and typed results. Native local/POSIX storage receives the strongest supported `atomic_local` contract. Every other fsspec filesystem remains usable through a clearly reported `best_effort_object_store` contract that reduces irreversible loss without claiming object-store atomicity or distributed locking.

## Solution

Introduce a configured `DatasetMaintenanceCoordinator` as the canonical maintenance service. It creates immutable, backend-pinned `MaintenancePlan` values for compaction, partition-local deduplication, global-repartitioning deduplication, and coordinated optimization, then executes accepted plans through one `execute(plan)` entry point.

The coordinator automatically classifies a strict native local/POSIX allowlist as `atomic_local`; all other fsspec filesystems, including S3-style storage, use `best_effort_object_store`. Every plan and result makes its guarantee level, validation level, selected backend, source snapshot, actual output metrics, concurrency limitations, and recovery artifacts explicit.

All physical rewrites use one lifecycle: plan, stage, reconcile schema, write, validate, publish, recover, and report. Backends contribute only their pinned staged-writing algorithm. The retained filesystem-extension methods are coordinator-backed convenience façades that plan and execute in one call; separate façade planning methods support review workflows.

## User Stories

1. As a dataset operator, I want one coordinator for all physical maintenance operations, so that compaction, deduplication, and optimization follow the same safety contract.
2. As a local dataset operator, I want an `atomic_local` maintenance guarantee, so that cooperating fsspeckit readers do not observe a partial directory rewrite.
3. As an S3 dataset operator, I want maintenance to remain available through my fsspec filesystem, so that the refactor does not remove existing object-store workflows.
4. As an object-store operator, I want the result to say `best_effort_object_store`, so that I know not to assume local atomicity, distributed locking, or automatic rollback.
5. As a direct API user, I want to construct a coordinator with an explicit backend, so that plan semantics are reproducible and reviewable.
6. As a filesystem-façade user, I want convenience methods to choose an available backend automatically and disclose it in the result, so that existing one-call workflows remain usable.
7. As an operator, I want an immutable maintenance plan before execution, so that I can inspect scope, source snapshot, partitions, output targets, and expected effects before mutation.
8. As an operator, I want planning to be lock-free, so that a human review does not block ordinary dataset access.
9. As an executor, I want source-snapshot drift to invalidate a plan, so that maintenance does not silently rewrite a different dataset population than the one reviewed.
10. As a local user, I want bounded advisory locking for cooperating reads and writes, so that lock contention fails predictably rather than hanging forever.
11. As a partitioned-dataset user, I want compaction to stage and publish only affected partition subtrees, so that unrelated partitions are neither copied nor rewritten.
12. As a partitioned-dataset user, I want partition-local deduplication by default, so that duplicate removal never moves retained rows into another physical partition.
13. As a data steward, I want global deduplication to be an explicit full-repartition operation, so that cross-partition data movement is deliberate and auditable.
14. As a user of key-based deduplication, I want null key components to compare equal, so that matching composite keys with nulls have deterministic behavior.
15. As a user of floating-point keys, I want NaN key components to compare equal, so that backend-specific IEEE behavior does not change duplicate grouping.
16. As a user of string keys, I want exact stored-value equality, so that maintenance never silently applies case folding, trimming, locale rules, or Unicode normalization.
17. As a user without an explicit business winner order, I want a documented snapshot-local physical winner order, so that key deduplication remains reproducible within one operation.
18. As a user with explicit winner ordering, I want physical order only to break equal-order ties, so that business ordering remains primary.
19. As a storage operator, I want `max_rows_per_file` enforced as a hard bound, so that output-file cardinality is controllable.
20. As a storage operator, I want byte-size targets reported as estimates with actual output bytes, so that codec and data-dependent variability is visible.
21. As a caller who does not choose a codec, I want rewritten files to use Snappy and report that selection, so that default storage policy is predictable.
22. As a schema owner, I want only lossless metadata-preserving schema reconciliation during maintenance, so that a physical rewrite cannot silently become a lossy schema migration.
23. As a schema owner, I want ambiguous types, precision loss, timezone reinterpretation, field removal, and metadata conflicts to invalidate the plan, so that maintenance preserves data meaning.
24. As a local operator, I want staged output validated before publication and rollback before a failed publish completes, so that failed maintenance does not replace the live dataset.
25. As an object-store operator, I want staged output fully validated before any live copy, so that publication begins only from a coherent candidate rewrite.
26. As an object-store operator, I want every exact planned live key validated after copy, so that prefix-listing consistency is not treated as publication proof.
27. As an object-store operator, I want every source object revalidated immediately before deletion, so that a concurrently changed source object is never deleted by maintenance.
28. As an object-store operator, I want zero source deletion when any source drift is detected during publication, so that partial publication is recoverable rather than destructive.
29. As an operator recovering a failed object-store operation, I want staging locations, copied keys, failed copies, untouched sources, and concurrency limitations in the typed result, so that cleanup and recovery are actionable.
30. As a user of optimization, I want it to mean optional deduplication followed by compaction, so that it does not imply unsupported z-ordering, clustering, sorting, or repartitioning.
31. As a user of dry-run workflows, I want explicit planning methods rather than a boolean that changes return types, so that planning and execution remain type-safe and unambiguous.
32. As an API consumer, I want a typed `MaintenanceResult` with plan, phase outcomes, validation, publication, recovery, and actual metrics, so that I do not depend on ambiguous dictionary keys.
33. As a maintainer, I want all backend rewrite algorithms behind one coordinator lifecycle, so that backend-specific implementation changes cannot reintroduce divergent publication guarantees.
34. As a maintainer, I want object-store support covered by generic memory-filesystem tests, so that the fsspec API path remains exercised without claiming real S3 integration coverage.
35. As a maintainer, I want local publication covered by fault-injection integration tests, so that lock, stage, rename, rollback, drift, partition, schema, and cleanup behavior is verified at the filesystem boundary.
36. As a user, I want ordinary rewritten row order to be unspecified, so that I do not infer business ordering from Parquet file layout.
37. As a documentation reader, I want the major-version migration guidance to distinguish local atomic guarantees from object-store best-effort guarantees, so that I can select an operationally appropriate workflow.

## Implementation Decisions

- Introduce a configured `DatasetMaintenanceCoordinator` service as the canonical direct API. It creates operation-specific immutable plans and provides generic plan-driven execution.
- Replace dictionary-returning maintenance APIs with public `MaintenancePlan` and `MaintenanceResult` types in a major-version migration.
- Replace the overloaded execution `dry_run` flag with explicit planning operations. The existing filesystem façade retains one-call behavior by planning and executing internally and returns a typed result.
- Pin the selected backend and all algorithm configuration in the plan. Direct coordinator users choose a backend explicitly; convenience façades may choose automatically and disclose the selected backend.
- Route every physical rewrite through the shared coordinator lifecycle. Backend implementations supply staged-write behavior, not independent publication or cleanup behavior.
- Classify guarantees conservatively. Only a strict native local/POSIX allowlist receives `atomic_local`; every other fsspec filesystem automatically receives `best_effort_object_store` unless a future capability adapter provides a stronger proven contract.
- For `atomic_local`, use a sibling maintenance workspace, bounded advisory shared/exclusive locks for cooperating fsspeckit access, partition-subtree staging, validated local rename publication, rollback before a failed publication completes, and immediate cleanup of successful-operation backups.
- For `best_effort_object_store`, use a staging prefix, validate before copying, copy every planned output to its live key, validate every planned live key individually, revalidate all planned source objects immediately before deletion, and delete no source objects if any source drift is detected. Preserve staging and partial live outputs after failure as reported recovery artifacts.
- Do not claim generic object-store distributed locking, atomic visibility, automatic rollback, or real-S3 test coverage.
- Make partition-local deduplication the default. Global deduplication is explicit and rewrites output according to declared partition columns.
- Define key semantics centrally: exact typed storage equality; null and NaN equality; snapshot-local physical fallback order; physical tie-breaking after explicit winner ordering.
- Preserve only lossless, metadata-preserving schema reconciliation. Do not invoke dtype inference/downcasting as part of maintenance reconciliation.
- Treat `max_rows_per_file` as a hard output bound. Treat byte-size targets as advisory and report actual output bytes. Use Snappy when no codec is explicitly requested.
- Define coordinated optimization as optional deduplication plus compaction and report the phases separately. Defer clustering, z-ordering, sorting, and repartitioning as separate future features.
- Support default staged-file/schema/partition/count validation and an explicit stronger full duplicate-key scan for deduplication.

## Testing Decisions

The highest primary seam is the public coordinator boundary: create a plan, execute it, and assert the externally observable `MaintenanceResult`, resulting dataset contents, partition placement, publication guarantee, and recovery artifacts. Backend writer tests should exercise their staged-write contract through this coordinator rather than duplicate lifecycle tests.

- Use temporary native-local filesystems for fault-injection integration tests. Verify lock timeouts, source drift, staged-write failure, rename failure, rollback, successful cleanup, partition-subtree scope, schema reconciliation, row-count bounds, key semantics, and result reporting.
- Use existing shared-planning and maintenance-template tests as prior art for planner grouping, dry-run behavior, and callback seams, but elevate new correctness tests to the coordinator seam.
- Use existing PyArrow correctness and parity tests as prior art for persisted deduplication, schema, ordering, partition, and compression behavior; replace expectations that rely on the current unsafe behavior.
- Use the DuckDB optional-extra test environment for backend parity and staged-write behavior. Do not require DuckDB for base-install test execution.
- Add generic object-store tests with an in-memory fsspec filesystem for staging, exact-key validation, source-deletion ordering, source-drift refusal, automatic guarantee classification, and recovery-artifact reporting.
- Do not claim that memory-filesystem tests verify S3. Real S3-compatible integration testing is intentionally outside this specification.
- Test behavior rather than private orchestration details: assert visible files, rows, partitions, schema, result fields, and recovery consequences rather than internal callback call counts.

## Out of Scope

- Strong atomic object-store publication through manifests, version pointers, conditional writes, or distributed leases.
- Generic distributed locks for S3, GCS, Azure Blob, or arbitrary fsspec filesystems.
- Automatic rollback of partially copied object-store outputs.
- A real S3/MinIO/Moto integration-test requirement in CI.
- z-ordering, clustering, automatic sort-order guarantees, or implicit repartitioning as part of optimization.
- Dtype inference/downcasting repair; it is a separate data-correctness workstream and is not allowed in maintenance schema reconciliation.
- Lossy schema promotion, string fallback, timezone stripping, metadata merging, field deletion, or implicit schema migration.
- A long-lived parallel `*_v2` maintenance API; the typed contract is a major-version migration.

## Further Notes

This specification supersedes the forward-looking architectural guidance of the historical compaction-template extraction plan. The existing implementation remains separate from this specification until the coordinated maintenance redesign is implemented.

The object-store mode is intentionally useful but constrained: it minimizes irreversible deletion and produces actionable recovery information, while clearly remaining weaker than local atomic publication. Operators requiring atomic reader visibility or coordinated multi-writer object-store commits need a future manifest/version protocol or a storage layer designed for those guarantees.
