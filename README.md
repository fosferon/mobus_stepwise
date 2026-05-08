# MobusStepwise

ALF-backed stepwise engine for multi-step wizards and workflows.

MobusStepwise provides a lightweight, linear(ish) step execution engine built on top of the [ALF](https://github.com/antonmi/ALF) pipeline framework. It powers wizard flows, import pipelines, and any process that progresses through an ordered sequence of steps — collecting data, executing actions, and producing UI projections along the way.

## Installation

Add `mobus_stepwise` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:mobus_stepwise, "~> 0.2.0"}
  ]
end
```

## Quick Start

### 1. Define a spec

A spec describes the steps, their order, and per-step UI/action metadata:

```elixir
spec = %{
  profile: :stepwise,
  initial_state: :step_one,
  steps: [:step_one, :step_two, :step_three],
  states: %{
    step_one:   %{step_number: 1, ui: %{key: :step_one}},
    step_two:   %{step_number: 2, ui: %{key: :step_two}},
    step_three: %{step_number: 3, ui: %{key: :step_three}}
  }
}
```

### 2. Initialize the engine

```elixir
runtime_context = %{
  tenant_id: "tenant-123",
  execution_id: "exec-001",
  sync: true
}

{:ok, runtime} = Mobus.Stepwise.Engine.init(spec, runtime_context)
# runtime.current_state => :step_one
```

### 3. Walk through steps with events

```elixir
# Advance to step two, merging user input into context
{:ok, runtime} = Mobus.Stepwise.Engine.handle_event(runtime, :next, %{name: "Alice"})
# runtime.current_state => :step_two
# runtime.context.name  => "Alice"

# Advance to step three with more data
{:ok, runtime} = Mobus.Stepwise.Engine.handle_event(runtime, :next, %{email: "alice@example.com"})
# runtime.current_state => :step_three

# Go back
{:ok, runtime} = Mobus.Stepwise.Engine.handle_event(runtime, :back, %{})
# runtime.current_state => :step_two
```

### 4. Read the projection

The projection is the canonical contract between the engine and the UI layer:

```elixir
projection = Mobus.Stepwise.Engine.get_state(runtime)
# %Mobus.Stepwise.Projection{
#   execution_id: "exec-001",
#   profile: :stepwise,
#   current_state: :step_two,
#   available_events: [:back, :next],
#   ui: %{key: :step_two, assigns: %{context: %{name: "Alice", ...}, state: :step_two}},
#   ...
# }
```

### 5. Checkpoint and restore

Save and restore engine state for resumable workflows:

```elixir
checkpoint = Mobus.Stepwise.Engine.checkpoint(runtime)
# => serializable map (no projection, no PIDs)

{:ok, restored} = Mobus.Stepwise.Engine.restore(spec, checkpoint, runtime_context)
# Picks up exactly where it left off
```

## Capability Actions

Steps can declare actions that execute via a pluggable capability runner:

```elixir
states: %{
  step_two: %{
    step_number: 2,
    ui: %{key: :step_two},
    action: %{type: :capability, handle: "myapp.validate_email"}
  }
}
```

Configure the adapter in your application config:

```elixir
config :mobus_stepwise, :capability_runner_adapter, MyApp.CapabilityRunner
```

When no adapter is configured, capability execution is a no-op — suitable for form-only wizards.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design overview, pipeline flow, and integration patterns.

## License

MIT
