defmodule Mobus.Stepwise.TelemetryTest do
  use ExUnit.Case, async: false

  alias Mobus.Stepwise.Engine
  alias Mobus.Stepwise.Telemetry

  @spec_2_steps %{
    profile: :stepwise,
    initial_state: :step_one,
    steps: [:step_one, :step_two],
    states: %{
      step_one: %{step_number: 1, ui: %{key: :step_one}},
      step_two: %{step_number: 2, ui: %{key: :step_two}}
    }
  }

  @runtime_context %{tenant_id: "t", execution_id: "e-tel", sync: true}

  setup do
    test_pid = self()

    handler_id = "test-#{System.unique_integer([:positive])}"

    events =
      for prefix <- Telemetry.events(), suffix <- [:start, :stop] do
        prefix ++ [suffix]
      end

    :telemetry.attach_many(
      handler_id,
      events,
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  describe "init/2 telemetry" do
    test "emits start and stop events" do
      {:ok, _runtime} = Engine.init(@spec_2_steps, @runtime_context)

      assert_received {:telemetry, [:mobus_stepwise, :engine, :init, :start], _, meta}
      assert meta.tenant_id == "t"

      assert_received {:telemetry, [:mobus_stepwise, :engine, :init, :stop], measurements, meta}
      assert meta.status == :ok
      assert meta.current_state == :step_one
      assert is_integer(measurements.duration)
    end
  end

  describe "handle_event/3 telemetry" do
    test "emits start and stop with event name" do
      {:ok, runtime} = Engine.init(@spec_2_steps, @runtime_context)

      # Drain init events
      flush_telemetry()

      {:ok, _runtime} = Engine.handle_event(runtime, :next, %{})

      assert_received {:telemetry, [:mobus_stepwise, :engine, :handle_event, :start], _, meta}
      assert meta.event == :next
      assert meta.execution_id == "e-tel"

      assert_received {:telemetry, [:mobus_stepwise, :engine, :handle_event, :stop], _, meta}
      assert meta.status == :ok
      assert meta.current_state == :step_two
    end
  end

  describe "restore/3 telemetry" do
    test "emits start and stop events" do
      {:ok, runtime} = Engine.init(@spec_2_steps, @runtime_context)
      checkpoint = Engine.checkpoint(runtime)
      flush_telemetry()

      {:ok, _restored} = Engine.restore(@spec_2_steps, checkpoint, @runtime_context)

      assert_received {:telemetry, [:mobus_stepwise, :engine, :restore, :start], _, _}
      assert_received {:telemetry, [:mobus_stepwise, :engine, :restore, :stop], _, meta}
      assert meta.status == :ok
    end
  end

  describe "Telemetry.events/0" do
    test "returns a list of event prefixes" do
      events = Telemetry.events()
      assert is_list(events)
      assert [:mobus_stepwise, :engine, :init] in events
      assert [:mobus_stepwise, :engine, :handle_event] in events
      assert [:mobus_stepwise, :engine, :restore] in events
    end
  end

  defp flush_telemetry do
    receive do
      {:telemetry, _, _, _} -> flush_telemetry()
    after
      0 -> :ok
    end
  end
end
