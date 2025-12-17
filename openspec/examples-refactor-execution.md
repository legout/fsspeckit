# Examples Refactor: Recommended Order + Parallelization

This note describes how to execute the following OpenSpec changes with minimal rework/conflicts:

- `cleanup-examples-structure`
- `fix-examples-runner`
- `update-examples-getting-started`
- `update-examples-common-and-sql`
- `update-examples-caching-and-batch-processing`
- `update-examples-schema-and-workflows`

## Recommended Order (Waves)

### Wave 1 (Define scope)
1. `cleanup-examples-structure`

Why first:
- It defines the “kept” vs “removed/docs-only” example set.
- It prevents spending time updating examples that will be deleted.
- It reduces churn in `examples/README.md` and `examples/requirements.txt`.

### Wave 2 (Update runnable examples — can be parallel)
Run these in parallel once the kept set is confirmed:
- `update-examples-getting-started`
- `update-examples-common-and-sql`
- `update-examples-caching-and-batch-processing`
- `update-examples-schema-and-workflows`

Why parallel-safe:
- They mostly touch different subtrees under `examples/`:
  - `examples/datasets/getting_started/*`
  - `examples/common/*` and `examples/sql/*`
  - `examples/caching/*` and `examples/batch_processing/*`
  - `examples/datasets/schema/*` and `examples/datasets/workflows/*`

Guidance:
- Avoid editing `examples/README.md` or `examples/requirements.txt` in these waves to reduce conflicts with Wave 1.
- Keep changes local to each example script unless a shared helper is introduced intentionally (not currently planned).

### Wave 3 (Make validation reliable)
3. `fix-examples-runner`

Why last:
- The runner/category list should match the post-cleanup, post-update tree.
- After Wave 2, the runner can be used as a regression gate for the final example set.

## What Can Run in Parallel (Quick Matrix)

- `cleanup-examples-structure`
  - Best run **alone** (touches deletions + global docs/requirements).
- `fix-examples-runner`
  - Can run in parallel with Wave 2 *only if* it is implemented as auto-discovery and/or you accept some rebase work.
  - Recommended: run after Wave 2 to avoid category drift.
- The four “update-*” changes
  - Can run in parallel with each other (Wave 2), as long as they don’t all edit shared top-level example docs.

## Suggested Practical Execution

1. Approve and implement `cleanup-examples-structure`.
2. Approve and implement the four update changes in parallel (separate PRs/branches).
3. Merge the update changes.
4. Approve and implement `fix-examples-runner` and use it to validate the merged example suite.

