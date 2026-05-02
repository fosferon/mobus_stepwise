defmodule Mobus.Stepwise.Components.StepwiseActionTest do
  use ExUnit.Case, async: false

  alias Mobus.Stepwise.Engine

  @spec_with_actions %{
    profile: :stepwise,
    initial_state: :step_a,
    steps: [:step_a, :step_b],
    states: %{
      step_a: %{
        step_number: 1,
        ui: %{key: :step_a},
        action: %{type: :capability, handle: "test.action"}
      },
      step_b: %{
        step_number: 2,
        ui: %{key: :step_b},
        action: %{type: :capability, handle: "test.finish"}
      }
    }
  }

  @runtime_context %{
    tenant_id: "test-tenant",
    execution_id: "test-exec-trace",
    sync: true
  }

  setup do
    # Use default no-op adapter (returns {:ok, %{context: %{}}})
    previous = Application.get_env(:mobus_stepwise, :capability_runner_adapter)
    Application.delete_env(:mobus_stepwise, :capability_runner_adapter)
    on_exit(fn -> if previous, do: Application.put_env(:mobus_stepwise, :capability_runner_adapter, previous) end)
    :ok
  end

  describe "trace entry step_id enrichment" do
    test "trace entries include step_id matching current_state (context path)" do
      # Default adapter returns {:ok, %{context: %{}}} — hits the context-update path
      {:ok, runtime} = Engine.init(@spec_with_actions, @runtime_context)
      assert runtime.current_state == :step_a

      # Advance — this triggers the action on step_a
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})

      # Trace should have an entry for step_a's action with step_id
      assert [trace_entry | _] = runtime.trace
      assert trace_entry.kind == :action
      assert trace_entry.handle == "test.action"
      assert trace_entry.step_id == :step_a
    end

    test "multiple steps accumulate trace entries with correct step_ids" do
      {:ok, runtime} = Engine.init(@spec_with_actions, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})

      action_traces = Enum.filter(runtime.trace, &(&1.kind == :action))
      assert length(action_traces) >= 1

      first = hd(action_traces)
      assert first.step_id == :step_a
      assert first.handle == "test.action"
    end

    test "step_id is an atom (consumers convert to string)" do
      {:ok, runtime} = Engine.init(@spec_with_actions, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})

      [trace_entry | _] = runtime.trace
      assert is_atom(trace_entry.step_id)
      assert to_string(trace_entry.step_id) == "step_a"
    end
  end

  describe "trace entry step_id with custom adapter (no-context path)" do
    test "bare result path also includes step_id in trace" do
      # Configure adapter that returns {:ok, :bare_result} — no context key
      Application.put_env(:mobus_stepwise, :capability_runner_adapter, __MODULE__.BareAdapter)

      {:ok, runtime} = Engine.init(@spec_with_actions, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})

      action_traces = Enum.filter(runtime.trace, &(&1.kind == :action))
      assert length(action_traces) >= 1

      first = hd(action_traces)
      assert first.step_id == :step_a
      assert first.handle == "test.action"
    end
  end

  # Test adapter that returns a bare result (no context key)
  defmodule BareAdapter do
    def execute(_tenant_id, _handle, _input) do
      {:ok, :bare_result}
    end
  end
end
