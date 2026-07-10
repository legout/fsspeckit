# Coordinated dataset maintenance guarantees

**Status:** Accepted

Physical dataset maintenance is a coordinated rewrite protocol rather than a
thin compaction callback loop. Every compaction, partition-local
deduplication, explicitly requested global-repartitioning deduplication, and
compatibility optimization creates an immutable, backend-pinned
`MaintenancePlan`. A configured `DatasetMaintenanceCoordinator` executes it
through one coordinator, performs lossless schema reconciliation, validates
the staged output, and returns a typed `MaintenanceResult`. Planning is
explicit and lock-free; direct coordinators require an explicit backend, while
retained filesystem façades may select one automatically and delegate to the
coordinator.

## Guarantee profiles

The coordinator selects a publication guarantee automatically and records it
in the plan and result.

- **`atomic_local`** applies only to a strict native-local/POSIX allowlist. It
  uses bounded advisory locks for cooperating fsspeckit readers and writers, a
  sibling maintenance workspace, partition-subtree staging, local rename
  publication, validation, rollback before publication, and immediate cleanup
  of successful-operation backups.
- **`best_effort_object_store`** applies to every other fsspec filesystem,
  including S3-style object stores. It has no generic distributed lock and
  makes no atomic reader-visibility or automatic rollback promise. It writes a
  complete staged rewrite, validates it, copies every output to its live key,
  validates every exact planned live key, then revalidates every planned source
  object before any source deletion. If a source has drifted, it deletes no
  inputs and retains staging plus partial live outputs as recovery artifacts in
  `MaintenanceResult`.

## Considered options

- Restrict all maintenance to local/POSIX filesystems. Rejected because the
  existing fsspec-oriented capability is required for S3 and other object
  stores.
- Add a versioned manifest/pointer protocol for strong object-store atomicity.
  Deferred: callers need broad best-effort object-store support now and accept
  its concurrency and visibility limits.
- Infer local atomicity from generic `mv` support. Rejected because fsspec
  `mv` can be copy-plus-delete.
- Require explicit best-effort opt-in. Rejected: non-local files automatically
  receive best-effort mode, but the weaker guarantee is always visible in
  plan/result data.
- Keep commit behavior backend-local. Rejected because publication, validation,
  partition placement, and recovery rules must remain consistent across all
  physical rewrites.

## Consequences

- Partition-local deduplication is the default; global deduplication is an
  explicit full-repartition operation.
- Null and `NaN` key components compare equal; strings use exact stored-value
  equality. Snapshot-local physical order `(partition path, file path, row
  offset)` is the default winner order and ties break with that same tuple.
- `max_rows_per_file` is a hard output bound. Byte-size targets are advisory.
  Rewrites without an explicit codec target use Snappy and report actual output
  bytes. Ordinary output row order is unspecified.
- Schema reconciliation permits only defined lossless promotions with identical
  schema and field metadata; ambiguity invalidates the plan. Dtype
  inference/downcasting repair is separate and is prohibited from maintenance
  reconciliation.
- The major release replaces dictionary results with public
  `MaintenancePlan`/`MaintenanceResult` types and replaces the overloaded
  `dry_run` execution flag with explicit planning plus generic `execute(plan)`.
- Deduplication validates readable staged files, schema, partitions, and count
  invariants by default. A full distinct-key scan is an explicit stronger
  verification level.
- Local publication is gated by a local-filesystem fault-injection matrix.
  Generic object-store support is covered with a memory filesystem only; it
  does not claim to verify real S3 behavior.
