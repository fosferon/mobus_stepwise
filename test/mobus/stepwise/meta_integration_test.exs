defmodule Mobus.Stepwise.MetaIntegrationTest do
  use ExUnit.Case, async: false

  alias Mobus.Stepwise.Engine

  @spec_2_steps %{
    profile: :stepwise,
    initial_state: :step_one,
    steps: [:step_one, :step_two],
    states: %{
      step_one: %{step_number: 1, ui: %{key: :step_one}},
      step_two: %{step_number: 2, ui: %{key: :step_two}}
    }
  }

  @runtime_context %{
    tenant_id: "test-tenant",
    execution_id: "test-meta-001",
    sync: true,
    meta: %{session_id: "sess-1", dag_id: "dag-1", tags: ["onboarding"]}
  }

  describe "meta through init" do
    test "meta from runtime_context is preserved in runtime" do
      {:ok, runtime} = Engine.init(@spec_2_steps, @runtime_context)
      assert runtime.meta == %{session_id: "sess-1", dag_id: "dag-1", tags: ["onboarding"]}
    end

    test "meta defaults to empty map when not provided" do
      {:ok, runtime} = Engine.init(@spec_2_steps, %{tenant_id: "t", sync: true})
      assert runtime.meta == %{}
    end
  end

  describe "meta through handle_event" do
    test "meta survives event processing" do
      {:ok, runtime} = Engine.init(@spec_2_steps, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{name: "Alice"})
      assert runtime.meta.session_id == "sess-1"
      assert runtime.meta.dag_id == "dag-1"
    end
  end

  describe "meta through checkpoint/restore" do
    test "meta survives checkpoint and restore" do
      {:ok, runtime} = Engine.init(@spec_2_steps, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{name: "Bob"})

      checkpoint = Engine.checkpoint(runtime)
      assert checkpoint.meta == %{session_id: "sess-1", dag_id: "dag-1", tags: ["onboarding"]}

      {:ok, restored} =
        Engine.restore(@spec_2_steps, checkpoint, %{tenant_id: "test-tenant", sync: true})

      assert restored.meta == %{session_id: "sess-1", dag_id: "dag-1", tags: ["onboarding"]}
    end

    test "restore merges runtime_context meta on top of checkpoint meta" do
      {:ok, runtime} = Engine.init(@spec_2_steps, @runtime_context)
      checkpoint = Engine.checkpoint(runtime)

      {:ok, restored} =
        Engine.restore(@spec_2_steps, checkpoint, %{
          tenant_id: "test-tenant",
          sync: true,
          meta: %{session_id: "new-session", node_id: "n-1"}
        })

      assert restored.meta.session_id == "new-session"
      assert restored.meta.node_id == "n-1"
      assert restored.meta.dag_id == "dag-1"
    end
  end

  describe "meta in projection extensions" do
    test "projection.extensions.meta reflects runtime meta" do
      {:ok, runtime} = Engine.init(@spec_2_steps, @runtime_context)
      projection = Engine.get_state(runtime)
      assert projection.extensions.meta == %{session_id: "sess-1", dag_id: "dag-1", tags: ["onboarding"]}
    end
  end
end
