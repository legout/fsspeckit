# Information Architecture Prototype (for #24)

> Status: **PROTOTYPE — directions confirmed** (see §5). Resolves the
> question in [#24](https://github.com/legout/fsspeckit/issues/24). Built on the
> [documentation drift audit](documentation-drift-audit.md) and the agreed
> vocabulary in `CONTEXT.md`. Read this top to bottom, then push back on any
> label, placement, or handoff that feels wrong.

## Question

What page-level information architecture, reader journeys, navigation labels,
cross-reference rules, and content placements let library users distinguish
tutorials, how-to guides, reference, explanation, and migrations **without
duplicating API behavior**?

## Design principles (non-negotiable, from the map Notes)

1. **Diátaxis is the user-facing skeleton** — four quadrants (Learning / Goals /
   Reference / Understanding) plus a Migration lane. Audience: library users.
2. **One source per fact.** API behavior lives in **Reference only**. Every other
   quadrant links to it; none restates a signature or semantics.
3. **The local dataset lifecycle is the primary entry point.** The homepage funnels
   library users into it; everything else is a deliberate handoff.
4. **Canonical imports everywhere active.** Legacy import paths and renamed APIs
   appear **only as deprecation notes in Reference** (`legacy import policy`).
5. **Code/tests are the authority.** When prose conflicts with supported code,
   code wins. No documentation of fictitious systems.

---

## 1. Target navigation (Material tabs)

Five top-level tabs. Migration folds **under Explanation** (a lane, not a peer of
the four Diátaxis quadrants) and stays discoverable from the homepage and the
architecture route (`migration entry point`).

| Tab (nav label) | Diátaxis role | What it is for |
| --- | --- | --- |
| **Home** | — | `primary documentation entry point`; funnels to the lifecycle tutorial, keeps secondary routes visible |
| **Tutorials** | Learning | learn by doing, guided narrative, copyable script |
| **How-to Guides** | Goals | solve one specific task, recipe-style, assume you know basics |
| **Reference** | Reference | authoritative API behavior + selection/config help + deprecation notes |
| **Explanation** | Understanding | durable conceptual model, architecture, why-we-built-it-this-way |

```
Home: index.md
Tutorials:
  - Local Dataset Lifecycle: tutorials/local-dataset-lifecycle.md     # was getting-started.md (cloud-first), REWRITTEN
How-to Guides:
  - Read and Write Datasets:     how-to/read-and-write-datasets.md
  - Merge Datasets:              how-to/merge-datasets.md              # <-- merge handoff target
  - Work with Filesystems:       how-to/work-with-filesystems.md
  - Configure Cloud Storage:     how-to/configure-cloud-storage.md     # rewritten, extras-accurate
  - Use SQL Filters:             how-to/use-sql-filters.md
  - Sync and Manage Files:       how-to/sync-and-manage-files.md
  - Optimize Performance:        how-to/optimize-performance.md
  - Memory-Constrained Environments: how-to/memory-constrained-environments.md
  - Adaptive Key Tracking:       how-to/adaptive-key-tracking.md
  - Multi-Key Examples:          how-to/multi-key-examples.md           # rewritten to class handlers
  - Multi-Key Performance:       how-to/multi-key-performance.md        # rewritten to class handlers
  - Merge Operations Examples:   how-to/merge-operations-examples.md
Reference:
  - Installation:                installation.md
  - API Guide:                   reference/api-guide.md                 # curated: API selection, imports, config, result interpretation
  - Dataset Handlers:            dataset-handlers.md
  - Storage Options:             reference/storage-options.md           # curated, accurate from_env
  - Deprecation & Legacy Imports: reference/legacy-imports.md           # <-- ONLY place legacy/renamed APIs live
  - Generated API:               (mkdocstrings tree — see §4)
Explanation:
  - Key Concepts:                explanation/concepts.md
  - Architecture:                explanation/architecture.md            # rewritten, current layout only, no fictitious systems
  - Migration:                                                          # folded lane — discoverable from Home + Architecture
      - Upgrade from the pre-refactor layout: migration/dataset-module-refactor.md   # primary actionable workflow
      - Move your package imports:   migration/migrate-package-layout.md             # relocated from how-to/
      - (historical) Append Mode Default:   migration/append-mode-default.md         # revised to current API or labeled historical
Contributing: contributing.md     # footer link, outside the library-user journey
```

**Removed / relocated from the current nav**
- `migrate-package-layout.md` moves **how-to → the Migration lane** under Explanation (it is migration, not a goal-recipe).
- `Layering Rules` (`architecture/0001-layering-rules.md`) leaves the Explanation tab and becomes a **historical** link reachable from Architecture only (it is an ADR describing prior placement; not active reference).
- The broken `Optimize Memory Monitoring Performance` indent entry is dropped from the flat how-to list; memory guidance consolidates under `memory-constrained-environments.md`.
- The mis-nested API Reference hierarchy (currently *inside* How-to due to an indent bug) becomes its own top-level **Reference** tab.

---

## 2. Reader journeys

Five journeys, each named for what the reader is doing, not the page they land on.

### A. First-time user (the spine)
`Home` → **Local Dataset Lifecycle** tutorial → (one or more handoffs):
- wants more data in one dataset → **Merge Datasets** how-to (`merge handoff`)
- something went wrong → **Installation** / **Storage Options** / troubleshooting reference (`tutorial diagnosis handoff`)
- wants the *why* behind filesystem/dataset/merge → **Explanation: Key Concepts** (`concept handoff`)
- ready for cloud/SQL/monitoring → corresponding **How-to** (`integration preview` — preview only, never in the copyable script)

The tutorial ends on the `lifecycle success signal` (read-back assertion + concise output), **not** on a write-only listing.

### B. Returning user (lookup)
`Reference` → **API Guide** (curated: "which thing do I import?") → **Generated API** page (the authoritative signature) or **Dataset Handlers**.

### C. Task-driven user (goal)
`How-to Guides` → pick the recipe → follow canonical imports → link out to **Reference** only when they need the full signature.

### D. Understanding-driven user
`Explanation` → **Key Concepts** (durable model) → **Architecture** (current domain-package layout, real diagrams).

### E. Upgrading user (migration)
`Home` (migration banner) *or* `Explanation: Architecture` (migration route) → **Upgrade from the pre-refactor layout** → if they hit a renamed symbol, that detail lives **only** in `Reference: Deprecation & Legacy Imports`.

---

## 3. Cross-reference rules (the anti-duplication discipline)

These rules are what stop API behavior from being restated across quadrants.

| Rule | Meaning |
| --- | --- |
| **R1 — Reference owns API behavior** | Signatures, parameter semantics, return shapes, and accepted values are stated **once**, in Reference (generated page authoritative; curated page adds selection/config/interpretation). Tutorials and how-tos **link**, they do not re-describe. |
| **R2 — Tutorials hand off, they don't branch** | The lifecycle tutorial stays on the happy path. Every off-path need is a link: merge → how-to, failure → diagnosis reference, concept → explanation, cloud/SQL → integration-preview how-to. No inline troubleshooting forks. |
| **R3 — Legacy is quarantined** | Renamed APIs and legacy import paths appear **only** in `Reference: Deprecation & Legacy Imports`. Active tutorials, how-tos, and migration workflows use canonical imports exclusively (`legacy import policy`). |
| **R4 — Migration is workflow, not reference** | Migration pages describe the *move* (before/after mappings, version scope). The *current* signature a user lands on after the move is always a Reference link, never re-documented in Migration. |
| **R5 — Historical material stays off the active path** | ADRs and plan docs are reachable from Explanation/Architecture as *history* and clearly labeled; they are never in the active tutorial/how-to/reference journey. |
| **R6 — Concepts defined once, linked everywhere** | Filesystem, dataset, write, and merge terms are given their durable definition in **Explanation: Key Concepts**. The tutorial defines them only as far as needed to proceed, then links (`concept handoff`). |

---

## 4. Content placement (what lives where)

The matrix that answers "where does this content go?" — and enforces no duplication.

| Content type | Lives in | NOT in |
| --- | --- | --- |
| Public symbol signatures, docstrings | **Generated API** (Reference) | tutorials, how-to, explanation |
| "Which API do I import / how do I pick?" | **API Guide** (Reference, curated) | tutorials, explanation |
| Step-by-step happy-path narrative + one copyable script | **Local Dataset Lifecycle** (Tutorial) | how-to, reference |
| Reconcile/merge an existing dataset (the next step) | **Merge Datasets** (How-to) | tutorial (tutorial only links to it) |
| Provider/cloud/SQL/monitoring setup | dedicated **How-to** + **Installation** extras matrix | the tutorial script |
| Renamed/legacy APIs | **Deprecation & Legacy Imports** (Reference) | any active page |
| Before/after import mappings, version scope | **Migration** pages | reference, how-to |
| Durable conceptual model, rationale | **Explanation** | tutorial (links to it) |
| ADRs, historical plans | **Explanation/Architecture** (labeled historical) | active journeys |
| Fictitious / unimplemented systems | **nowhere** — removed | everywhere |

### Generated API tree (within Reference)

Reorganized so domain packages are primary and `utils` is the demoted façade. Pages
are *generated* from public exports + public tests (exact coverage decided in #26);
the structure here is the placement.

```
Generated API:
  - Overview: api/index.md
  - Datasets:        fsspeckit.datasets (+ duckdb, pyarrow.io/dataset, schema, types)
  - Core:            fsspeckit.core (base, filesystem, merge, incremental, maintenance)
  - Storage Options: fsspeckit.storage_options (base, core, cloud, git)
  - SQL:             fsspeckit.sql.filters
  - Common:          fsspeckit.common (parallel, partitions, sync, security, optional, datetime)
  - Compatibility façade (legacy): fsspeckit.utils.*    # clearly labelled, last
```
Private methods (e.g. `_check_cache`, `_check_file`) are excluded from the generated
surface — that is enforced by #26's contract, but the placement rule is "never in the
public reference path".

### Installation extras matrix
A single authoritative table in **Installation** mapping each declared extra
(`aws`, `gcp`, `azure`, `polars`, `datasets`, `monitoring`, `sql`) to the integration
it enables. Provider how-tos and reference pages link to it; none duplicates it.

---

## 5. What this prototype deliberately leaves to other tickets

- **#25** decides exactly which migration pages survive as actionable vs. historical,
  and the version scope.
- **#26** decides the precise public-API inventory (which symbols/pages get generated
  coverage, what stays curated) and removes private exposure.
- **#27** decides the validation/audit policy (build check, link check, manual review).
- **#28** scopes the OpenSpec change that implements all of this.

This ticket settles only the **shape**: the page tree, the nav labels, the journeys,
the cross-reference rules, and the placement discipline above.

---

## Decisions confirmed (human reaction)

The four open questions were put to the map driver; the agreed directions are:

1. **Migration folds under Explanation** — not a top-level tab. It stays a lane
   discoverable from the homepage and the architecture route.
2. **Single canonical tutorial** — the Tutorial tab is one page (the local dataset
   lifecycle) for now; no stubbed second tutorial.
3. **`migrate-package-layout.md` → Migration lane** — relocated out of How-to.
4. **Single curated Deprecation & Legacy Imports page** — all renamed APIs and legacy
   imports funnel to one Reference page; no per-page side notes.

(Reference tab label stays "Reference" — short; the tab context implies API.)
