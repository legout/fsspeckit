# Agent instructions

Issues live in GitHub Issues via `gh`; PRs are not a triage surface. Read issue comments and labels along with the body. See `docs/agents/issue-tracker.md`.

The five triage labels are documented in `docs/agents/triage-labels.md`.

<!-- pi-implementation-orchestrator:start -->
## Agent workflow

- Every validation unit receives one test obligation: `new-test`, `existing-check`, or `no-new-test`; related tasks may share a validation unit, and focused TDD is required only for `new-test` work.
- Review is adaptive and orchestrator-owned: low-risk work uses parent diff inspection; normal-risk work gets one candidate review; high-risk or dependency-defining work gets immediate plus candidate review.
- Plans and tickets reference exact feature sources; this file defines stable repository-wide scope.
- Scoped authority: glossaries own terminology; ADRs own accepted architectural constraints; specifications own behavior; plans/tickets own execution decomposition. No scope silently overrides another; reconcile owner decisions into the affected artifacts before dependent work proceeds.
- Stop before implementation when authoritative sources conflict.

### Review ground rules

- Priority: agreed feature, then correctness, then proven risk. Written conventions are binding; taste never blocks. Name the applicable instruction, style, lint, or contract source; if none exists, say so rather than inventing conventions.
- Findings need a named requirement or written rule, a problem this change caused or worsened, reachability through real callers/inputs/environment, material impact, and a proportionate response.
- Security requires a touched boundary (untrusted/external input, credentials, auth, dependency changes), named asset, realistic attacker, and actual attack path. Stories needing stolen secrets, broken TLS, malicious admins, or generic hardening are not findings. No boundary touched: `security: n/a`; missing security facts: `unverified`, never invent a threat model. Trusted internal callers and the user's own local files are not hostile by default.
- Test requests are findings: name a real scenario or drop them. Coverage percentage is not a reason.
- Disposition before repair: parent rejects failed gates in one line, authorizes small in-scope fixes, or hands large/out-of-scope fixes to the human. Reviewers never start fixes or re-reviews.
- Reviews end when criteria, real risks, and written rules are covered: `pass or fix-first`, then stop. One fix pass, one delta recheck; unresolved findings go to the human, never round three. Candidate review checks integration effects, not settled findings again.
- Paste the full reviewer contract from `orchestrate-implementation` into every fresh reviewer prompt, with criteria, conventions, and real-use context; file links alone do not deliver it.
- After each task: restate it, compare the result, choose `accept / fix / hand back / ask`. Extra ideas get one line, not code. Smallest safe change; one behavior and, for `new-test`, one failing test first. Dependencies and abstractions need a job today. No extra ledgers or sign-off artifacts.

### Routing and authority

- Read `docs/agents/artifacts.md` for the project artifact mapping and load the `planning-contract` skill for artifact classification and planning handoffs; read `docs/agents/issue-tracker.md` and `docs/agents/domain.md` when their scope applies. Preserve established project conventions.
- Use `shape-design` for unresolved behavior/design choices, `write-implementation-plan` for approved multi-step work, and `orchestrate-implementation` to execute approved work. Do not turn a trivial edit into a planning exercise.
- Default orchestrated execution to `supervised`: builtin `worker` may implement and validate, but candidate assembly, integration, and publication retain explicit approval gates.
- Route implementation to builtin `worker` and, when required by the selected policy, independent review to a fresh read-only builtin `reviewer`; confirm both are executable before dispatch and record the resolved names in the run manifest.
- Keep one writer per worktree. Use `pi-subagents` for spawned-child lifecycle; named persistent `pi-intercom` peers are read-only advisors, not implementation or review agents.
- Use `systematic-debugging` for unexpected failures and `verification-before-completion` before success claims; match evidence to the exact change and report skipped checks.
- Use `merge-worktree` for target integration and `make-release` for releases. Local integration does not authorize pushing; opening a PR does not authorize merging; release or publication requires its own approved plan.
- Stop on conflicting authoritative sources, unclear ownership, failed required gates, or missing required tooling. Never silently switch execution modes to bypass a blocker.

### Documentation map

- `CONTEXT.md`: canonical domain vocabulary for the whole repository.
- `docs/adr/`: accepted architecture decisions.
- `docs/agents/`: workflow, tracker, and artifact-map configuration.
- `docs/specs/` or the configured tracker: feature behavior and acceptance.
- implementation plans/tickets: execution entry points and explicit source references.
<!-- pi-implementation-orchestrator:end -->
