defmodule Mobus.Stepwise.RuntimeTest do
  use ExUnit.Case, async: true

  alias Mobus.Stepwise.Runtime

  @runtime %{
    execution_id: "exec-1",
    tenant_id: "tenant-1",
    current_state: :step_one,
    context: %{name: "Alice"},
    artifacts: %{},
    meta: %{session_id: "sess-1", dag_id: "dag-1"}
  }

  describe "meta accessors" do
    test "meta/1 returns the full meta map" do
      assert Runtime.meta(@runtime) == %{session_id: "sess-1", dag_id: "dag-1"}
    end

    test "meta/1 returns empty map when absent" do
      assert Runtime.meta(%{}) == %{}
    end

    test "get_meta/2 returns a single key" do
      assert Runtime.get_meta(@runtime, :session_id) == "sess-1"
    end

    test "get_meta/3 returns default when key missing" do
      assert Runtime.get_meta(@runtime, :missing, "default") == "default"
    end

    test "put_meta/3 sets a key" do
      updated = Runtime.put_meta(@runtime, :node_id, "n-1")
      assert Runtime.get_meta(updated, :node_id) == "n-1"
      assert Runtime.get_meta(updated, :session_id) == "sess-1"
    end

    test "put_meta/3 works on runtime without meta" do
      updated = Runtime.put_meta(%{}, :key, "val")
      assert Runtime.get_meta(updated, :key) == "val"
    end

    test "merge_meta/2 shallow-merges" do
      updated = Runtime.merge_meta(@runtime, %{node_id: "n-1", session_id: "overridden"})
      assert Runtime.get_meta(updated, :node_id) == "n-1"
      assert Runtime.get_meta(updated, :session_id) == "overridden"
      assert Runtime.get_meta(updated, :dag_id) == "dag-1"
    end
  end

  describe "context accessors" do
    test "context/1 returns the context map" do
      assert Runtime.context(@runtime) == %{name: "Alice"}
    end

    test "overlay_context/3 sets a key" do
      updated = Runtime.overlay_context(@runtime, :email, "a@b.com")
      assert Runtime.context(updated).email == "a@b.com"
      assert Runtime.context(updated).name == "Alice"
    end

    test "merge_context/2 merges a map" do
      updated = Runtime.merge_context(@runtime, %{email: "a@b.com", name: "Bob"})
      assert Runtime.context(updated).name == "Bob"
      assert Runtime.context(updated).email == "a@b.com"
    end
  end

  describe "simple getters" do
    test "execution_id/1" do
      assert Runtime.execution_id(@runtime) == "exec-1"
    end

    test "tenant_id/1" do
      assert Runtime.tenant_id(@runtime) == "tenant-1"
    end

    test "current_state/1" do
      assert Runtime.current_state(@runtime) == :step_one
    end

    test "artifacts/1" do
      assert Runtime.artifacts(@runtime) == %{}
    end
  end
end
