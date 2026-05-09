# Architecture

MobusStepwise is an ALF-backed workflow execution engine with two explicit profiles:

- `:stepwise` for ordered, wizard-like progression
- `:flow` for graph execution with explicit forks, joins, waits, and checkpointable parallel state

## Core Model

The engine separates **specification** (what the workflow looks like) from **runtime** (where the workflow is right now).

### `:stepwise` Spec (static definition)

```
%{
  profile:       :stepwise,
  initial_state: :step_one,
  steps:         [:step_one, :step_two, :step_three],
  states: %{
    step_one:   %{step_number: 1, ui: %{key: :step_one}, action: %{...}},
    step_two:   %{step_number: 2, ui: %{key: :step_two}},
    step_three: %{step_number: 3, ui: %{key: :step_three}}
  },
  transitions:   %{},          # optional explicit transitions
  breakpoints:   [],            # optional debug breakpoints
  subscriptions: []             # optional PubSub topics
}
```

Key fields:

| Field           | Purpose                                                       |
|-----------------|---------------------------------------------------------------|
| `profile`       | Always `:stepwise` — identifies this as a stepwise workflow   |
| `initial_state` | The step the engine starts on                                 |
| `steps`         | Ordered list of step identifiers (atoms or strings)           |
| `states`        | Per-step metadata: UI descriptor, actions, step number        |
| `transitions`   | Optional explicit event→state mappings for custom events      |
| `breakpoints`   | Optional debug breakpoints on events or state entries         |
| `subscriptions` | PubSub topic templates interpolated with runtime context      |

### `:stepwise` Runtime (live state)

```
%{
  execution_id:    "exec-001",
  tenant_id:       "tenant-123",
  spec:            %{...},           # normalized IR
  current_state:   :step_two,
  context:         %{name: "Alice"}, # accumulated user input
  artifacts:       %{},              # durable workflow-scoped data
  history:         [%{event: :next, from: :step_one, to: :step_two, at: ~U[...]}],
  trace:           [%{kind: :step, ...}],
  blocked_reasons: %{},
  breakpoint_hits: [],
  projection:      %Projection{...}  # canonical UI contract
}
```

## Engine Lifecycle

```
                     ┌─────────────────────────────────────────────┐
                     │                  SPEC                       │
                     │  profile, initial_state, steps, states      │
                     └─────────────────┬───────────────────────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │  Engine.init/2   │
                              │  normalize → IR  │
                              │  start pipeline  │
                              │  entry action    │
                              │  → projection    │
                              └────────┬────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │    RUNTIME       │◄──────────────┐
                              │  current_state   │               │
                              │  context         │               │
                              │  projection      │               │
                              └────────┬────────┘               │
                                       │                         │
                            event(:next, payload)                │
                                       │                         │
                                       ▼                         │
                     ┌─────────────────────────────────┐         │
                     │      ALF Pipeline               │         │
                     │                                 │         │
                     │  1. StepwiseContextMerge        │         │
                     │     merge payload → context     │         │
                     │                                 │         │
                     │  2. StepwiseAction              │         │
                     │     run capability (if defined) │         │
                     │                                 │         │
                     │  3. StepwiseAdvance             │         │
                     │     move to next/prev step      │         │
                     │                                 │         │
                     │  4. StepwiseEntryAction         │         │
                     │     run entry action on new     │         │
                     │     state (if state changed)    │         │
                     │                                 │         │
                     │  5. FsmBreakpoint               │         │
                     │     record breakpoint hits      │         │
                     │                                 │         │
                     │  6. StepwiseProjection          │         │
                     │     build UI projection         │         │
                     └────────────────┬────────────────┘         │
                                      │                          │
                                      ▼                          │
                              ┌─────────────────┐               │
                              │  updated RUNTIME │───────────────┘
                              │  new state       │
                              │  new projection  │
                              └─────────────────┘
```

### init/2

1. Extracts `tenant_id` and `execution_id` from runtime context
2. Starts the ALF pipeline (if not already running)
3. Normalizes the spec into internal representation (IR)
4. Builds initial runtime with `current_state` set to `initial_state`
5. Fires entry action for the initial state (capabilities with `:enter` trigger)
6. Computes initial projection

### handle_event/3

Sends an event (`:next`, `:back`, or custom) through the ALF pipeline with a payload. The pipeline stages execute in order:

1. **StepwiseContextMerge** — merges the event payload into `runtime.context`
2. **StepwiseAction** — if the current step defines an action (e.g. capability), executes it
3. **StepwiseAdvance** — advances or reverses `current_state` based on step ordering
4. **StepwiseEntryAction** — if the state changed, runs entry-triggered actions on the new state
5. **FsmBreakpoint** — records any breakpoint hits for debugging
6. **StepwiseProjection** — computes the canonical `Projection` struct

Returns `{:ok, runtime}`, `{:wait, runtime, wait_cfg}`, or `{:error, reason, runtime}`.

### get_state/1

Returns the current `Projection` struct from the runtime. If the projection is stale or missing, recomputes it by running the pipeline in projection-only mode.

### checkpoint/1 and restore/3

`checkpoint/1` strips the runtime down to a serializable map (no projection, no process references). `restore/3` re-hydrates a runtime from a checkpoint, re-normalizing the spec and recomputing the projection.

## Profiles

The engine uses `spec.profile` to select profile-specific behavior.

### `:stepwise`

- Ordered step progression from `steps` or `step_number`
- Single `current_state`
- ALF pipeline-backed step/action/projection flow
- Existing compatibility profile for all current consumers

### `:flow`

- Explicit `nodes` + `edges` graph model
- Token-based execution with multiple active branches
- Fork and join semantics with branch result aggregation
- Shared `%Projection{}` wrapper with graph data in `projection.extensions.flow`
- Versioned checkpoints that preserve active tokens, waits, join buffers, and branch results

## Events

Events drive state changes through the engine:

| Event             | Behavior                                                    |
|-------------------|-------------------------------------------------------------|
| `:next` / `"next"`| Advance to the next step in order                           |
| `:back` / `"back"`| Return to the previous step in order                        |
| Custom atoms      | Looked up in `spec.transitions` for explicit target state   |
| `:__enter__`      | Internal — fired on state entry to trigger entry actions    |

Events carry a `payload` map that is merged into `runtime.context` by `StepwiseContextMerge`, accumulating user input across steps.

For `:flow`, events are interpreted by the graph runtime. `:next` advances ready tokens, `:resume` resumes waiting tokens, and `:cancel` / `:timeout` can target specific tokens or branches through the payload.

## Projection

The `Mobus.Stepwise.Projection` struct is the canonical contract between the engine and the UI layer. It contains everything the UI needs to render the current step:

- `current_state` — which step we're on
- `available_events` — what the user can do (`:next`, `:back`)
- `ui` — `%{key: atom, assigns: map}` descriptor for component rendering
- `artifacts` — durable workflow-scoped data
- `blocked_reasons` — why certain events are blocked
- `errors` / `trace` — debugging information

For `:flow`, `current_state` is the focus node and graph-wide execution state lives under `projection.extensions.flow`.

## Capabilities

Steps can define **actions** that execute via a pluggable capability runner adapter:

```elixir
action: %{type: :capability, handle: "myapp.validate", triggers: [:next]}
```

- The adapter is configured via `config :mobus_stepwise, :capability_runner_adapter, Module`
- When no adapter is set, capability execution is a no-op (`{:ok, %{context: %{}}}`)
- Capabilities receive the full execution context and return context updates and/or artifacts
- Failed capabilities block the event and populate `blocked_reasons`

## Artifacts

Artifacts are durable, workflow-scoped data that survive refreshes, retries, and external callbacks. They are normalized into a canonical format with `kind`, `version`, `inserted_at`, and `data` fields. Capabilities can produce artifacts that are merged into the runtime.

## IR (Internal Representation)

Before the engine operates on a spec, it normalizes it via `Mobus.Stepwise.IR.normalize/1`. This:

- Coerces known profile strings to atoms
- Ensures `states`, `transitions`, and `nodes` exist in normalized shapes
- Normalizes the profile identifier

## UI Schemas

`WizardConfig` and `WizardStep` are Ecto embedded schemas for structuring wizard UI configuration in the consuming application. They provide validation via changesets and are used to define wizard metadata (title, step display mode) and individual step definitions (title, description, component).

## Integration: HuMan GrooveExecutor

The primary consumer of MobusStepwise is `HuMan.GrooveExecutor`, which:

1. Builds a stepwise spec from a groove definition (steps, states, UI keys)
2. Calls `Engine.init/2` with the spec and a runtime context containing `tenant_id` and `execution_id`
3. On user interaction, calls `Engine.handle_event/3` with `:next` and the form payload
4. Reads `Engine.get_state/1` to get the `Projection` for LiveView rendering
5. Uses `checkpoint/1` / `restore/3` for persistence across page refreshes

The flow:

```
Groove Definition
       │
       ▼
  Build stepwise spec (steps, states, UI keys)
       │
       ▼
  Engine.init(spec, %{tenant_id: t, execution_id: e, sync: true})
       │
       ▼
  Render projection.ui.key as LiveComponent
       │
       ▼
  User submits form → Engine.handle_event(runtime, :next, form_data)
       │
       ▼
  Re-render with updated projection
       │
       ▼
  Repeat until final step
```
