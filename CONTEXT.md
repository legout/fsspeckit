# fsspeckit Documentation Context

The terms in this context define the user-facing documentation concepts used to plan and maintain fsspeckit documentation.

## Language

**Local dataset lifecycle**:
The offline-first user journey in which a Python user who can manage a virtual environment installs fsspeckit, configures a local filesystem, writes a dataset, reads it back, and learns when merge is the appropriate next operation.
_Avoid_: Quickstart, basic workflow, local demo

**Merge handoff**:
The tutorial boundary that explains when a simple dataset write is sufficient and routes users who need to reconcile existing data to a dedicated merge how-to guide.
_Avoid_: Merge tutorial, basic merge

**Minimal explicit schema**:
The smallest readable field definition used in the local dataset lifecycle to make a dataset’s stored shape deterministic without teaching schema design, evolution, or validation in depth.
_Avoid_: Inferred schema, schema tutorial

**Tutorial sandbox**:
A clearly named temporary directory that contains every file created by the local dataset lifecycle and is the only location the example explicitly cleans up.
_Avoid_: Working directory cleanup, project-root data

**Canonical tutorial**:
A narrative documentation page that explains the local dataset lifecycle step by step and includes a complete copyable script; repository examples support it rather than replacing it.
_Avoid_: Example-first documentation, notebook-first tutorial

**Integration preview**:
A brief next-steps pointer from the local dataset lifecycle to cloud, SQL, monitoring, or other advanced integrations without using them in its copyable script.
_Avoid_: Integration setup, optional-dependency tutorial

**Lifecycle success signal**:
The read-back assertion and concise human-readable output that demonstrate the local dataset lifecycle produced the expected stored data.
_Avoid_: Visual check, write-only success

**Tutorial diagnosis handoff**:
The short failure-oriented section following a happy-path tutorial that routes path, schema, and dependency problems to installation, filesystem, and troubleshooting references.
_Avoid_: Inline troubleshooting branches, error-free tutorial

**Concept handoff**:
The division in which a tutorial defines filesystem, dataset, write, and merge terms only when needed, then links to an Explanation page for their durable conceptual model and rationale.
_Avoid_: Tutorial-as-reference, explanation prerequisite

**Primary documentation entry point**:
The homepage call to action that starts a library user on the local dataset lifecycle while retaining how-to, reference, explanation, migration, and integration routes as visible secondary choices.
_Avoid_: API-first homepage, role-selection gate

**Migration entry point**:
The visible homepage and architecture-route link to refactor migration guidance, reinforced by contextual notices wherever retained legacy imports or workflows may still be encountered.
_Avoid_: Redirect-only migration, hidden migration section

**Hybrid API reference**:
Reference presentation in which generated pages provide authoritative public symbols and docstrings while curated pages explain API selection, imports, configuration, and result interpretation.
_Avoid_: Generated-only reference, hand-written symbol inventory

**Manual tutorial review**:
The pre-minor-release review of the canonical tutorial’s copyable script against the supported public API, without extracting or continuously executing the script in CI.
_Avoid_: Snippet-execution framework, unreviewed tutorial code

**Manual documentation audit**:
The pre-minor-release human review that checks site buildability, navigation, links, refactor accuracy, API discoverability, and tutorial prose without making any automated documentation check mandatory.
_Avoid_: Required documentation CI, build-only validation

**Bounded-impact documentation review**:
The manual content-review scope for each minor release: always check the canonical
tutorial, then check modified user-facing pages and every reference or migration
page affected by a public-API, extras, or navigation change against the
supported-public-API inventory.
_Avoid_: Full-site reread, changed-files-only review

**Legacy import policy**:
The rule that current tutorials, how-to guides, and migration routes use canonical imports, while legacy import paths and renamed APIs appear only as deprecation notes in reference.
_Avoid_: Contextual legacy notices, side-by-side imports

**Supported migration source**:
The immediately preceding publicly supported pre-refactor layout from which documentation provides an actionable workflow upgrade path to fsspeckit 0.22.x.
_Avoid_: All-version migration, versionless migration

**Shared documentation audit**:
The pre-minor-release manual documentation audit performed as a best-effort team convention without a designated accountable owner or release-blocking effect.
_Avoid_: Release-owner audit, documentation-maintainer gate

**Supported public API**:
A stable export or explicitly inventoried direct-import module intended for library
users. Public-test imports provide audit evidence but do not alone create a
support commitment, and underscore-prefixed helpers remain internal.
_Avoid_: Test-defined API, all-imports public surface

**Integration and extras matrix**:
The authoritative curated reference that maps each optional workflow or provider
to its `fsspeckit` extra. Individual API pages state their requirement and link
back to this matrix.
_Avoid_: Per-page-only installation guidance, integration setup in the
canonical tutorial

**Compatibility façade**:
The legacy `fsspeckit.utils` import surface, documented only through concise
migration/deprecation mappings to canonical domain modules and omitted from
generated API reference pages.
_Avoid_: Primary API surface, generated compatibility reference

**Composite-key dataset workflow**:
An offline example workflow in which a persisted local dataset identifies a
record by two or more fields, then demonstrates reconciliation and duplicate
removal against that dataset.
_Avoid_: In-memory compatibility demo, single-key-only example

**Import-safe example**:
An example whose module can load with the base development environment; optional
provider setup is reached only through an explicitly selected integration path.
_Avoid_: Import-time provider construction, hidden extra requirement

**Dataset maintenance commit coordinator**:
The shared maintenance boundary that publishes a completed dataset rewrite as one
logical transition under an exclusive maintenance window. Cooperating readers
wait for the transition and then observe a fully validated replacement; they
never observe a partial rewrite.
_Avoid_: Best-effort file cleanup, backend-specific commit semantics, partial
dataset visibility to cooperating readers

**Partition-local deduplication**:
Duplicate removal that treats each physical partition tuple as an independent
data domain and never moves a retained row to another partition.
_Avoid_: Implicit cross-partition winner selection, path-derived partition drift

**Global repartitioning deduplication**:
An explicitly requested whole-dataset rewrite that selects duplicate winners
across partitions and writes every retained row according to declared partition
columns.
_Avoid_: Global deduplication as a default maintenance side effect

**Local atomic maintenance publication**:
The maintenance commit guarantee available only on local/POSIX filesystems,
where staged directory transitions are published through atomic rename steps
inside an exclusive maintenance window. Maintenance rejects filesystems that
cannot make this guarantee.
_Avoid_: Best-effort object-store publication, silently weakened atomicity

**Exclusive maintenance window**:
The interval in which a dataset rewrite holds exclusive access; cooperating
readers wait rather than attempting to traverse a directory being swapped.
_Avoid_: Lock-free concurrent reads during physical directory replacement

**Cooperating dataset access**:
The fsspeckit read and maintenance operations that acquire the dataset's POSIX
advisory lock before accessing a dataset. Direct external file or PyArrow reads
are outside the maintenance visibility guarantee.
_Avoid_: Assuming arbitrary external readers participate in maintenance locking

**Recoverable maintenance publication**:
A maintenance rewrite that automatically restores the prior dataset when
publication fails and retains the previous complete version after publication
until cleanup is confirmed.
_Avoid_: Manual recovery as the normal failure path, deleting the only rollback
candidate before publication is known to have succeeded

**Maintenance validation invariant**:
A pre-publication condition requiring readable staged files, schema compatibility,
correct operation-level row counts, intended partition placement, and internally
consistent deduplication statistics.
_Avoid_: Readability-only publication checks, unverified row or partition loss

**Physical-order deduplication**:
Key-based duplicate removal that retains the first row under the deterministic
order `(partition path, file path, row offset)` when no business winner order is
provided.
_Avoid_: Unspecified first-row behavior, storage-order-dependent results that
are undocumented

**Deterministic deduplication tie-breaker**:
The physical-order tuple applied only after explicit deduplication ordering
columns tie, ensuring one reproducible winner per key.
_Avoid_: Arbitrary equal-order winners, unstable backend-specific ties

**Snapshot-local physical order**:
The deterministic storage order captured for one maintenance operation; it is
not a durable business ordering and may change after compaction or
repartitioning.
_Avoid_: Treating rewritten file layout as immutable row provenance

**Maintenance operation plan**:
The internal typed description of one proposed dataset rewrite, including its
source snapshot, partition scope, output destinations, validation expectations,
and recovery state.
_Avoid_: Loosely coupled callback arguments, untyped maintenance dictionaries

**Source-snapshot drift**:
Any change to a planned input file's local identity or metadata between
maintenance planning and publication. Drift invalidates the plan and requires a
new operation.
_Avoid_: Silent replanning that changes the deduplication population or winner
selection

**Coordinated physical rewrite**:
Any compaction, deduplication, or optimization that changes stored dataset files
and therefore follows the shared plan, stage, validate, publish, and recover
lifecycle.
_Avoid_: Backend-local publication logic, operation-specific safety guarantees

**Coordinated optimization**:
The compatibility maintenance operation that performs optional deduplication
followed by compaction under one coordinated physical rewrite; it does not imply
clustering, z-ordering, or repartitioning.
_Avoid_: Calling deduplication plus compaction a clustering guarantee

**Maintenance planning**:
The explicit non-mutating creation of a maintenance plan, including partition
scope, source snapshot, intended outputs, validation invariants, and estimated
phase statistics.
_Avoid_: Overloaded dry-run execution flags, execute-only optimization

**Backend-native maintenance capability failure**:
The filesystem error surfaced by the selected backend when a required local
maintenance capability—such as locking, staging, or rename—is unavailable.
_Avoid_: A new universal preflight capability exception

**Typed maintenance result**:
The public structured result of a coordinated physical rewrite, exposing its
plan, phase outcomes, validation, publication, recovery, and actual measured
statistics rather than an untyped compatibility dictionary.
_Avoid_: Additive growth of ambiguous legacy result dictionaries

**Maintenance API major migration**:
The major-version replacement of dictionary-returning maintenance methods with
the typed maintenance-result contract and dedicated migration guidance.
_Avoid_: Parallel long-lived `*_v2` maintenance APIs

**Maintenance plan**:
The immutable public proposal for a snapshot-bound physical rewrite, returned
without mutation and accepted by execution only while its source snapshot
remains valid.
_Avoid_: Dry-run results with optional execution state

**Maintenance result**:
The public observed outcome of executing an accepted maintenance plan, including
actual phase statistics, validation, publication, and recovery information.
_Avoid_: Treating estimated plan statistics as execution facts

**Plan-driven maintenance execution**:
The single public execution entry point that accepts an immutable maintenance
plan and dispatches its coordinated rewrite internally.
_Avoid_: Operation-specific public execution lifecycles, caller-selected
backend dispatch after planning

**Backend-pinned maintenance plan**:
A maintenance plan whose backend and algorithm configuration are selected during
planning and must remain available unchanged during execution.
_Avoid_: Execution-time backend substitution, environment-dependent plan meaning

**Partition-subtree publication**:
The local publication strategy that stages and swaps only the partition
directories affected by a maintenance plan while one exclusive maintenance
window protects the multi-directory transaction.
_Avoid_: Rewriting unrelated dataset partitions, independently visible partial
partition swaps

**Maintenance workspace**:
The managed sibling filesystem location, outside a live dataset's discoverable
tree, that holds same-filesystem staging and rollback artifacts for one or more
maintenance operations.
_Avoid_: Staged Parquet files under the live dataset root

**Immediate successful-backup cleanup**:
The policy that removes replaced partition backups after a validated maintenance
publication succeeds, while retaining artifacts only for failed publication or
cleanup paths.
_Avoid_: Treating successful publication as a long-lived rollback facility

**Maintenance schema reconciliation**:
The automatic unification and casting of compatible input schemas within one
maintenance rewrite scope before staged output is written.
_Avoid_: Treating every schema difference as a separate migration prerequisite

**Lossless maintenance schema reconciliation**:
Schema reconciliation limited to defined promotions that preserve every input
value and its meaning; incompatible or ambiguous conflicts invalidate the plan.
_Avoid_: String fallbacks, timezone stripping, precision loss, field removal

**Metadata-preserving reconciliation**:
The lossless schema policy requiring identical schema and field metadata for
matching keys; metadata conflicts invalidate maintenance planning.
_Avoid_: Implicit metadata precedence, metadata union, metadata deletion

**Bounded maintenance lock acquisition**:
The configurable finite waiting policy for cooperating shared or exclusive
dataset locks; timeout aborts the attempted access without mutation.
_Avoid_: Indefinite maintenance or reader blocking

**Lock-free maintenance planning**:
The non-mutating plan-creation policy that does not acquire a dataset lock;
execution later obtains exclusive access and rejects any source-snapshot drift.
_Avoid_: Holding a lock during human plan review, treating a plan as a live lock

**Hard row-count compaction bound**:
The guarantee that a staged compaction writer splits output so no produced file
exceeds `max_rows_per_file`; byte-size targets remain data- and codec-dependent
estimates reported from actual output.
_Avoid_: Promising an unenforceable compressed-byte maximum

**Unordered maintenance output**:
The rule that coordinated rewrites preserve validated dataset semantics but make
no promise about ordinary scan or presentation row order.
_Avoid_: Inferring business ordering from rewritten Parquet file order

**Null-equal key deduplication**:
The key-deduplication rule that considers null key components equal when they
occur in the same positions of an otherwise matching composite key.
_Avoid_: Treating unknown-key rows as automatically distinct

**NaN-equal key deduplication**:
The cross-backend key comparison rule that canonicalizes floating-point `NaN`
values as equal within the same key component.
_Avoid_: Backend-dependent IEEE NaN grouping behavior

**Exact storage key equality**:
The key-deduplication rule that compares stored typed values exactly, including
case, whitespace, and Unicode codepoint distinctions for strings.
_Avoid_: Implicit business normalization in a physical maintenance operation

**Maintenance fault-injection matrix**:
The local-filesystem integration-test standard that exercises every publication
transition and verifies rollback, locking, source drift, partitions, schema
reconciliation, key semantics, and cleanup.
_Avoid_: Happy-path-only maintenance verification

**Maintenance-independent dtype repair**:
The separate data-correctness workstream for dtype inference and downcasting.
Maintenance schema reconciliation must not invoke it and uses only defined
lossless schema promotions and casts.
_Avoid_: Making physical maintenance depend on unsafe dtype inference

**Dataset maintenance coordinator**:
The configured public service that creates maintenance plans and executes them
under the local coordinated-rewrite policy.
_Avoid_: Stateless maintenance functions with repeated policy configuration

**Coordinator-backed filesystem façade**:
The retained fsspec filesystem-extension convenience surface that delegates to a
configured dataset maintenance coordinator and returns typed maintenance values.
_Avoid_: Independent filesystem-extension maintenance lifecycle logic

**One-call maintenance façade**:
The filesystem-extension convenience behavior that creates and immediately
executes a coordinator plan, returning a typed maintenance result; separate
façade planning helpers remain available for review workflows.
_Avoid_: A second direct maintenance implementation outside the coordinator

**Explicit coordinator backend**:
The direct coordinator construction rule requiring callers to choose a backend;
only the convenience façade may apply documented automatic selection and records
its choice in the resulting plan.
_Avoid_: Hidden backend selection in direct maintenance workflows

**Default Snappy maintenance compression**:
The rewrite policy that selects Snappy whenever a maintenance caller does not
request a target codec, regardless of source-file codec variation; the selected
codec and actual output bytes remain visible in plan and result data.
_Avoid_: Implicit preservation of source compression

**Best-effort object-store maintenance**:
The explicitly selected fsspec object-storage maintenance mode that stages and
validates a rewrite but does not promise atomic dataset publication, distributed
reader locking, or automatic rollback. Its result reports the guarantee level
and any recovery artifacts.
_Avoid_: Claiming POSIX atomicity for S3-style copy/delete publication

**Automatic object-store maintenance selection**:
The capability policy that chooses best-effort object-store maintenance for
non-POSIX filesystems without a caller mode flag, while always exposing the
weaker guarantee in typed plan and result data.
_Avoid_: Hidden guarantee-level changes in maintenance results

**Staged best-effort object-store publication**:
The non-atomic object-store sequence that validates a complete staged rewrite,
copies all outputs to live paths, and only then deletes inputs. Failed copies
retain staging and partial live outputs as reported recovery artifacts.
_Avoid_: Direct overwrite followed by irreversible incremental source deletion

**Tiered deduplication validation**:
The policy that validates staged deduplication by readable files, schema,
partition placement, and row/count invariants by default, while making a full
distinct-key scan an explicitly selected stronger verification level.
_Avoid_: Paying a full duplicate scan by default, or presenting count-only
validation as proof that no duplicates remain

**Exact-key object-store publication validation**:
The best-effort post-copy check that verifies every output key specified by the
plan individually, rather than inferring a complete publication from prefix
listing.
_Avoid_: Listing-consistency assumptions as a publication proof

**Unlocked generic object-store maintenance**:
The generic best-effort object-store mode that offers no distributed writer
lease and treats concurrent external writers as unsupported, while recording
source-snapshot drift and concurrency limitations in plan/result data.
_Avoid_: Claiming that generic fsspec provides a portable distributed lock

**Conservative maintenance guarantee classification**:
The automatic policy that grants local atomic publication only to a strict
native-local/POSIX allowlist; every other fsspec filesystem receives the
best-effort object-store contract unless a future capability adapter says
otherwise.
_Avoid_: Treating generic `mv` support as atomic rename support

**Memory-only object-store verification**:
The release-test boundary that exercises generic object-store maintenance through
an in-memory fsspec filesystem but does not require an S3-compatible integration
service in CI.
_Avoid_: Claiming that memory-filesystem coverage verifies S3 behavior

**Pre-deletion object-store source revalidation**:
The best-effort publication rule that rechecks every planned input immediately
before deletion and deletes none when any source snapshot has drifted, retaining
staging and copied outputs as recovery artifacts.
_Avoid_: Deleting an object that a concurrent writer changed during publication
