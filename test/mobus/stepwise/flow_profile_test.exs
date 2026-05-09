defmodule Mobus.Stepwise.FlowProfileTest do
  use ExUnit.Case, async: false

  alias Mobus.Stepwise.Engine

  @runtime_context %{tenant_id: "flow-tenant", execution_id: "flow-exec-001", sync: true}

  @fork_join_spec %{
    profile: :flow,
    initial_state: :start,
    nodes: %{
      start: %{type: :task, ui: %{key: :start}},
      fork: %{type: :fork, ui: %{key: :fork}},
      left: %{
        type: :task,
        ui: %{key: :left},
        action: %{type: :capability, handle: "test.left"}
      },
      right: %{
        type: :task,
        ui: %{key: :right},
        action: %{type: :capability, handle: "test.right"}
      },
      join: %{type: :join, ui: %{key: :join}},
      done: %{type: :end, ui: %{key: :done}}
    },
    edges: [
      %{from: :start, to: :fork},
      %{from: :fork, to: :left, branch_id: :left},
      %{from: :fork, to: :right, branch_id: :right},
      %{from: :left, to: :join, branch_id: :left},
      %{from: :right, to: :join, branch_id: :right},
      %{from: :join, to: :done}
    ]
  }

  setup do
    previous = Application.get_env(:mobus_stepwise, :capability_runner_adapter)
    Application.put_env(:mobus_stepwise, :capability_runner_adapter, __MODULE__.FlowAdapter)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:mobus_stepwise, :capability_runner_adapter)
        adapter -> Application.put_env(:mobus_stepwise, :capability_runner_adapter, adapter)
      end
    end)

    :ok
  end

  test "init returns a flow projection with graph extensions" do
    assert {:ok, runtime} = Engine.init(@fork_join_spec, @runtime_context)

    projection = Engine.get_state(runtime)

    assert projection.profile == :flow
    assert projection.current_state == :start
    assert projection.extensions.flow.focus_node == :start
    assert projection.extensions.flow.active_nodes == [:start]
    assert projection.available_events == [:next, :cancel]
  end

  test "forks branches, joins them, and aggregates branch results deterministically" do
    {:ok, runtime} = Engine.init(@fork_join_spec, @runtime_context)

    assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
    assert Map.keys(runtime.active_tokens) |> Enum.sort() == [
             "tok:root:start:left:left",
             "tok:root:start:right:right"
           ]

    assert runtime.projection.extensions.flow.active_nodes == [:left, :right]

    assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
    assert runtime.current_state == :done
    assert runtime.active_tokens == %{}

    assert runtime.branch_results.join.branches.left == %{value: "left"}
    assert runtime.branch_results.join.branches.right == %{value: "right"}
    assert runtime.context.flow_results.join.left == %{value: "left"}
    assert runtime.context.flow_results.join.right == %{value: "right"}
  end

  test "extends capability input for flow nodes" do
    spec = %{
      profile: :flow,
      initial_state: :capture,
      nodes: %{
        capture: %{
          type: :task,
          ui: %{key: :capture},
          action: %{type: :capability, handle: "test.capture"}
        }
      },
      edges: []
    }

    {:ok, runtime} = Engine.init(spec, @runtime_context)
    assert {:ok, _runtime} = Engine.handle_event(runtime, :next, %{hello: "world"})

    assert_received {:flow_capability_input, input}
    assert input.profile == :flow
    assert input.node_id == :capture
    assert input.token_id == "tok:root:capture"
    assert input.branch_id == nil
    assert input.execution_id == "flow-exec-001"
    assert input.payload == %{hello: "world"}
    assert input.graph_context == %{}
    assert input.branch_context == %{}
  end

  test "waiting tokens survive checkpoint/restore and resume with explicit token targeting" do
    spec = %{
      profile: :flow,
      initial_state: :waiter,
      nodes: %{
        waiter: %{
          type: :task,
          ui: %{key: :waiter},
          action: %{type: :capability, handle: "test.wait", triggers: [:next, :resume]}
        },
        done: %{type: :end, ui: %{key: :done}}
      },
      edges: [%{from: :waiter, to: :done}]
    }

    {:ok, runtime} = Engine.init(spec, @runtime_context)

    assert {:wait, runtime, wait_cfg} = Engine.handle_event(runtime, :next, %{})
    assert wait_cfg.status == :waiting
    assert runtime.pending_waits["tok:root:waiter"].resume_event == :resume

    checkpoint = Engine.checkpoint(runtime)
    assert checkpoint.checkpoint_version == 1

    assert {:ok, restored} = Engine.restore(spec, checkpoint, @runtime_context)
    assert restored.pending_waits["tok:root:waiter"].resume_event == :resume

    assert {:ok, restored} =
             Engine.handle_event(restored, :resume, %{token_id: "tok:root:waiter"})

    assert restored.current_state == :done
    assert restored.pending_waits == %{}
    assert restored.context.approved == true
  end

  test "supports branch cancellation and timeout events" do
    {:ok, runtime} = Engine.init(@fork_join_spec, @runtime_context)
    {:ok, runtime} = Engine.handle_event(runtime, :next, %{})

    assert {:ok, runtime} = Engine.handle_event(runtime, :cancel, %{token_id: "tok:root:start:left:left"})
    assert Map.has_key?(runtime.cancelled_tokens, "tok:root:start:left:left")
    refute Map.has_key?(runtime.active_tokens, "tok:root:start:left:left")

    assert {:ok, runtime} = Engine.handle_event(runtime, :timeout, %{token_id: "tok:root:start:right:right"})
    assert Map.has_key?(runtime.failed_tokens, "tok:root:start:right:right")
    assert runtime.failed_tokens["tok:root:start:right:right"].failure_reason == :timeout
    assert runtime.active_tokens == %{}
  end

  test "branch-local capability failures surface as runtime errors" do
    spec = put_in(@fork_join_spec, [:nodes, :left, :action, :handle], "test.error")
    {:ok, runtime} = Engine.init(spec, @runtime_context)
    {:ok, runtime} = Engine.handle_event(runtime, :next, %{})

    assert {:error, :branch_boom, runtime} = Engine.handle_event(runtime, :next, %{})
    assert Map.has_key?(runtime.failed_tokens, "tok:root:start:left:left")
    assert runtime.blocked_reasons["tok:root:start:left:left"] == :branch_boom
  end

  test "isolated failures can still satisfy any-success joins" do
    spec =
      @fork_join_spec
      |> put_in([:nodes, :fork, :failure_policy], :isolate)
      |> put_in([:nodes, :join, :join_policy], :any_success)
      |> put_in([:nodes, :left, :action, :handle], "test.error")

    {:ok, runtime} = Engine.init(spec, @runtime_context)
    {:ok, runtime} = Engine.handle_event(runtime, :next, %{})

    assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
    assert runtime.current_state == :done
    assert runtime.context.flow_results.join.right == %{value: "right"}
    assert runtime.context.flow_results.join.left == %{error: :branch_boom}
    assert runtime.failed_tokens["tok:root:start:left:left"].failure_reason == :branch_boom
  end

  test "timeout without explicit token targets expired waits only" do
    spec = %{
      profile: :flow,
      initial_state: :waiter,
      nodes: %{
        waiter: %{
          type: :task,
          ui: %{key: :waiter},
          action: %{type: :capability, handle: "test.expiring_wait", triggers: [:next]}
        },
        done: %{type: :end, ui: %{key: :done}}
      },
      edges: [%{from: :waiter, to: :done}]
    }

    {:ok, runtime} = Engine.init(spec, @runtime_context)
    assert {:wait, runtime, wait_cfg} = Engine.handle_event(runtime, :next, %{})
    assert wait_cfg.deadline

    assert {:ok, runtime} = Engine.handle_event(runtime, :timeout, %{})
    assert runtime.active_tokens == %{}
    assert runtime.failed_tokens["tok:root:waiter"].failure_reason == :timeout
  end

  test "restore rejects corrupted flow checkpoints" do
    {:ok, runtime} = Engine.init(@fork_join_spec, @runtime_context)
    checkpoint = Engine.checkpoint(runtime)
    corrupted = put_in(checkpoint, [:active_tokens, "tok:root:start", :node_id], :missing_node)

    assert {:error, :invalid_checkpoint_tokens} =
             Engine.restore(@fork_join_spec, corrupted, @runtime_context)
  end

  test "telemetry includes flow profile and token/node context" do
    test_pid = self()
    handler_id = "flow-telemetry-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      for(prefix <- Mobus.Stepwise.Telemetry.events(), suffix <- [:start, :stop], do: prefix ++ [suffix]),
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, runtime} = Engine.init(@fork_join_spec, @runtime_context)
    flush_telemetry()

    assert {:ok, _runtime} =
             Engine.handle_event(runtime, :next, %{token_id: "tok:root:start"})

    assert_received {:telemetry, [:mobus_stepwise, :engine, :handle_event, :stop], _, meta}
    assert meta.profile == :flow
    assert meta.token_id == "tok:root:start"
    assert meta.node_id == :fork
    assert meta.status == :ok
  end

  defp flush_telemetry do
    receive do
      {:telemetry, _, _, _} -> flush_telemetry()
    after
      0 -> :ok
    end
  end

  defmodule FlowAdapter do
    def execute(_tenant_id, "test.left", _input), do: {:ok, %{context: %{value: "left"}}}
    def execute(_tenant_id, "test.right", _input), do: {:ok, %{context: %{value: "right"}}}

    def execute(_tenant_id, "test.capture", input) do
      send(self(), {:flow_capability_input, input})
      {:ok, %{context: %{captured: true}}}
    end

    def execute(_tenant_id, "test.wait", %{event: event}) when event in [:next, "next"] do
      {:wait, %{wait: %{reason: :approval_needed}, context: %{requested: true}}}
    end

    def execute(_tenant_id, "test.wait", %{event: event}) when event in [:resume, "resume"] do
      {:ok, %{context: %{approved: true}}}
    end

    def execute(_tenant_id, "test.expiring_wait", _input) do
      deadline = DateTime.utc_now() |> DateTime.add(-5, :second) |> DateTime.truncate(:second)
      {:wait, %{wait: %{reason: :expired_approval, deadline: deadline}}}
    end

    def execute(_tenant_id, "test.error", _input), do: {:error, :branch_boom}
  end
end
