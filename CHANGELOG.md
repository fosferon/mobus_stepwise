# Changelog

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

- **Extracted Ecto schemas**: `WizardConfig` and `WizardStep` moved to a new sibling library
  `mobus_stepwise_wizard_ui`. Module names unchanged (`Mobus.Stepwise.UI.WizardConfig`,
  `Mobus.Stepwise.UI.WizardStep`). `mobus_stepwise` no longer depends on `:ecto`.

  **Migration**: Consumers using the wizard schemas should add `{:mobus_stepwise_wizard_ui, github: "fosferon/mobus_stepwise_wizard_ui"}` to their `mix.exs`. No code changes needed.

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
  existing callers** — notably HuMan, which should run its test suite against this
  release before upgrading.

## v0.1.0 (March 2026)

Initial extraction from MOBuS / Atrapos codebase.
