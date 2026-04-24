defmodule Mobus.Stepwise.EngineTest do
  # async: false — tests in "init/2 initial entry action errors" mutate the
  # shared :mobus_stepwise, :capability_runner_adapter application env.
  use ExUnit.Case, async: false

  alias Mobus.Stepwise.Engine
  alias Mobus.Stepwise.Projection

  @spec_3_steps %{
    profile: :stepwise,
    initial_state: :step_one,
    steps: [:step_one, :step_two, :step_three],
    states: %{
      step_one: %{step_number: 1, ui: %{key: :step_one}},
      step_two: %{step_number: 2, ui: %{key: :step_two}},
      step_three: %{step_number: 3, ui: %{key: :step_three}}
    }
  }

  @runtime_context %{
    tenant_id: "test-tenant",
    execution_id: "test-exec-001",
    sync: true
  }

  describe "init/2" do
    test "creates runtime with current_state == first step" do
      assert {:ok, runtime} = Engine.init(@spec_3_steps, @runtime_context)
      assert runtime.current_state == :step_one
      assert runtime.execution_id == "test-exec-001"
      assert runtime.tenant_id == "test-tenant"
    end
  end

  describe "handle_event/3 :next" do
    test "advances current_state to step 2" do
      {:ok, runtime} = Engine.init(@spec_3_steps, @runtime_context)
      assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert runtime.current_state == :step_two
    end

    test "with payload merges payload into context" do
      {:ok, runtime} = Engine.init(@spec_3_steps, @runtime_context)
      assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{name: "Alice"})
      assert runtime.context.name == "Alice"

      # Context accumulates across steps
      assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{email: "alice@test.com"})
      assert runtime.context.name == "Alice"
      assert runtime.context.email == "alice@test.com"
    end

    test "on last step does not advance (stays on last step)" do
      {:ok, runtime} = Engine.init(@spec_3_steps, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert runtime.current_state == :step_three

      # :next on last step -- stays on step_three, available_events only has :back
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert runtime.current_state == :step_three
    end
  end

  describe "handle_event/3 :back" do
    test "from step 2 returns to step 1" do
      {:ok, runtime} = Engine.init(@spec_3_steps, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert runtime.current_state == :step_two

      assert {:ok, runtime} = Engine.handle_event(runtime, :back, %{})
      assert runtime.current_state == :step_one
    end
  end

  describe "get_state/1" do
    test "returns Projection with current_state, available_events, and ui" do
      {:ok, runtime} = Engine.init(@spec_3_steps, @runtime_context)
      projection = Engine.get_state(runtime)

      assert %Projection{} = projection
      assert projection.current_state == :step_one
      assert :next in projection.available_events
      assert projection.ui != nil
      assert projection.ui.key == :step_one
    end
  end

  describe "checkpoint/1 and restore/3" do
    test "checkpoint returns serializable map and restore recreates equivalent runtime" do
      {:ok, runtime} = Engine.init(@spec_3_steps, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{name: "Bob"})

      checkpoint = Engine.checkpoint(runtime)
      assert is_map(checkpoint)
      assert checkpoint.current_state == :step_two
      assert checkpoint.context.name == "Bob"
      refute Map.has_key?(checkpoint, :projection)

      # Restore from checkpoint
      assert {:ok, restored} = Engine.restore(@spec_3_steps, checkpoint, @runtime_context)
      assert restored.current_state == :step_two
      assert restored.context.name == "Bob"

      # Restored runtime should be functional
      assert {:ok, restored} = Engine.handle_event(restored, :next, %{})
      assert restored.current_state == :step_three
    end
  end

  describe "init/2 initial entry action errors" do
    setup do
      previous = Application.get_env(:mobus_stepwise, :capability_runner_adapter)

      on_exit(fn ->
        case previous do
          nil -> Application.delete_env(:mobus_stepwise, :capability_runner_adapter)
          mod -> Application.put_env(:mobus_stepwise, :capability_runner_adapter, mod)
        end
      end)

      :ok
    end

    test "propagates {:error, reason} from the initial state's entry capability" do
      Application.put_env(
        :mobus_stepwise,
        :capability_runner_adapter,
        __MODULE__.BoomOnEnterAdapter
      )

      spec = %{
        profile: :stepwise,
        initial_state: :step_one,
        steps: [:step_one, :step_two],
        states: %{
          step_one: %{
            step_number: 1,
            ui: %{key: :step_one},
            action: %{type: :capability, handle: "test.boom", triggers: [:enter]}
          },
          step_two: %{step_number: 2, ui: %{key: :step_two}}
        }
      }

      assert {:error, {:initial_entry_action_failed, :boom, runtime}} =
               Engine.init(spec, @runtime_context)

      # Capability-populated blocked_reasons survives the merge so the caller can
      # inspect which state/event produced the error.
      assert is_map(runtime)
      assert runtime.current_state == :step_one
      assert runtime.blocked_reasons != %{}
      assert runtime.blocked_reasons |> Map.values() |> Enum.member?(:boom)
    end
  end

  # Capability adapter that always returns {:error, :boom}. Used to verify that
  # a failing entry capability on the initial state surfaces as an error from
  # Engine.init/2 rather than being silently swallowed.
  defmodule BoomOnEnterAdapter do
    def execute(_tenant_id, _handle, _input), do: {:error, :boom}
  end
end
