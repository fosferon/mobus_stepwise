# Changelog

## v0.4.0-dev (unreleased) — graph mode for workflows stored as data

Everything here is in the `:flow` profile. The `:stepwise` profile is
untouched.

### Added

- **Conditional edges.** An edge may carry `when`: a data condition
  (`Mobus.Stepwise.Condition`) evaluated against the run's context. Conditional
  edges are tried in declared order; an edge without a condition is the
  "otherwise". This gives decisions, and loops with an exit.
- **`for_each` nodes.** One branch per item of a list found at run time, with
  `max_concurrency`, `on_error: stop | continue`, and an optional `collect`
  path. The body ends at a join, which hands on a summary
  (`total / completed / failed / errors / items`). Items can wait.
- **Timeout edges.** An edge with `on: timeout` is followed when a wait runs
  out, instead of failing the branch.
- **Resume context.** A `context` map in a resume payload lands in the
  resumed branch's context.
- **`context` in capability input**, the key the `:stepwise` profile uses, so
  one capability serves both profiles.
- **`:all_settled` join policy.**

### Changed

- **Specs loaded from data work.** Node types, join policies and failure
  policies are accepted as strings, through fixed vocabularies; no atom is
  created from spec or checkpoint data. An unknown node type is refused.
- **Checkpoints survive JSON.** `checkpoint/1` carries no tuples, and
  `restore/3` rebuilds tokens, waits, join buffers and loops from their
  string-keyed form.
- **History, trace and errors are bounded** (most recent 500), so a long loop
  does not grow every checkpoint.
- **A fork inside a branch joins back into that branch.** Previously its
  successor lost the branch it came from. Join matching is nesting-aware.

### Fixed

- An untargeted event no longer walks ready branches it was not meant for
  past their node with the action skipped.
- A join that needs only some branches cancels the rest when it completes;
  a late branch could complete it a second time.
- A cycle made only of fork / join / end nodes is refused at init instead of
  recursing.

## v0.3.1 (June 2026) — Public release hygiene

### Changed

- **Documentation cleanup.** Removed internal project references and stale
  sections from the changelog, `ARCHITECTURE.md`, and module docs so the
  package reads as a standalone library.
- `package/0` now declares an explicit `files` list, and `ARCHITECTURE.md` is
  included in the Hex package.

### Internal (unreleased on Hex, now tagged)

- Hardened stepwise transition and meta handling in the advance/profile path,
  with expanded transition-policy and meta integration tests.

## v0.3.0 (May 2026)

### Added

- **New `:flow` profile**: `Mobus.Stepwise.Engine` now supports explicit graph execution alongside
  the existing `:stepwise` compatibility profile. Consumers select the mode per execution through
  `spec.profile`.

- **Token-based graph runtime**: `:flow` executions track active/completed/failed/cancelled tokens,
  branch-local state, join buffers, pending waits, aggregated branch results, and versioned
  checkpoints suitable for restore of in-flight parallel work.

- **Fork / join execution model**: `:flow` supports explicit fork nodes, parallel branch execution,
  join nodes, deterministic branch result aggregation, and projection output under
  `projection.extensions.flow`.

- **Policy-aware graph semantics**: `:flow` now supports join policies (`:all`, `:any_success`,
  `{:any_success, n}`, `{:quorum, n}`) and branch failure policies (`:fail_fast`, `:isolate`) so
  consumers can model more realistic orchestration behavior without overloading the stepwise runtime.

- **Deadline-aware waits and operational controls**: waiting flow tokens can carry deadlines,
  `:timeout` can expire overdue waits without explicit token targeting, and `:cancel` / `:timeout`
  can target individual tokens or whole branches through event payloads.

- **Profile-aware telemetry**: engine telemetry metadata now includes `:profile`, and flow runtimes
  enrich spans with token / node / branch context when available.

### Changed

- **Profile dispatch in `Mobus.Stepwise.Engine`**: the public engine API is unchanged, but execution
  now routes internally to profile-specific runtime modules. Existing `:stepwise` behavior is
  preserved behind `Mobus.Stepwise.Profiles.Stepwise`; `:flow` is implemented independently in
  `Mobus.Stepwise.Profiles.Flow`.

- **IR normalization expanded**: `Mobus.Stepwise.IR.normalize/1` now recognizes both `"stepwise"`
  and `"flow"` profiles and normalizes graph-specific `nodes` / `edges` shapes.

- **Projection contract expanded, not broken**: both profiles still return
  `%Mobus.Stepwise.Projection{}`. `:flow` uses `current_state` as a focus node and places graph-wide
  execution details into `projection.extensions.flow`.

### Fixed

- **Restore validation for graph checkpoints**: corrupted `:flow` checkpoints now fail fast on
  restore when tokens reference missing nodes or waiting metadata no longer matches active waiting
  tokens.

## v0.2.0 (May 2026)

### Added

- **Pluggable `pipeline_mod`**: `Mobus.Stepwise.Engine` now accepts `pipeline_mod` from `runtime_context`
  at init/2 time. When present, the engine uses it for all pipeline calls (`ensure_started/1`, `call/1`).
  Defaults to `Mobus.Stepwise.Pipeline.Stepwise` when not provided. Enables consumers to inject per-agent
  or per-spec compiled pipelines without forking the engine. Testing harnesses, alternate stage
  compositions, and per-agent dynamic compilation are all natural use cases.

- **`:strict` mode on capability runner nil-adapter**: New config flag
  `:mobus_stepwise, :capability_runner_strict` (default: `false`, backwards compatible). When `true`,
  calling `execute/3` with no adapter configured returns `{:error, :capability_runner_disabled}`
  instead of the default no-op success. Consumers that always configure an adapter (e.g. workflow_stem)
  should set this to `true` so accidental nil-adapter calls surface as errors.

- **`:conversation` action type**: `Mobus.Stepwise.Components.StepwiseAction` now supports a
  `:conversation` action type alongside `:capability`. Host applications configure a conversation
  handler via `:mobus_stepwise, :conversation_handler`. The handler must implement
  `handle_conversation/5` with signature `(event, runtime, trigger, payload, action_config) :: event`.

  Conversation completion is fully generalized — when a trigger is NOT `:enter` or `:chat_message`,
  the event payload is merged transparently into `runtime.context` with no hardcoded field names.
  Each consumer's handler owns its domain vocabulary.

- **`Mobus.Stepwise.SpecHelpers`**: Shared pure functions (`equivalent_state?/2`, `ordered_steps/1`)
  extracted from `StepwiseAdvance`, `StepwiseProjection`, and `FsmBreakpoint`. Eliminates 7+ copies
  of `equivalent_state?` and 4 copies of `ordered_steps` within the foundation alone.

### Changed

- **Extracted Ecto schemas**: `WizardConfig` and `WizardStep` moved to a separate
  companion library. Module names unchanged (`Mobus.Stepwise.UI.WizardConfig`,
  `Mobus.Stepwise.UI.WizardStep`). `mobus_stepwise` no longer depends on `:ecto`.

  The wizard UI schemas live in a separate companion library; `mobus_stepwise`
  itself no longer depends on `:ecto`. No code changes are needed for consumers
  that do not use the wizard schemas.

- Engine `runtime` map now carries a `:pipeline_mod` field. `checkpoint/1` drops it (re-resolved on
  `restore/3`).

- `EngineBehaviour` callback spec updated to include the third `init/2` return shape
  (`{:error, {:initial_entry_action_failed, ...}}`). Downstream mock modules should
  update their `@impl` clauses accordingly.

### Fixed

- **Init-error propagation** (BREAKING): Previously, errors from the initial state's entry
  capability were silently discarded. Now `init/2` returns
  `{:error, {:initial_entry_action_failed, reason, runtime}}` with `blocked_reasons`
  populated. Consuming code that only matches `{:ok, runtime}` or `{:error, reason}`
  will need a third clause for this tuple. This is a correct bug fix but **breaks
  existing callers**; run your test suite against this release before upgrading.

## v0.1.0 (March 2026)

Initial public release: ALF-backed stepwise workflow engine with spec → IR
normalization and a static stepwise pipeline.
