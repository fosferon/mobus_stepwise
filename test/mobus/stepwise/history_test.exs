defmodule Mobus.Stepwise.HistoryTest do
  use ExUnit.Case, async: true

  alias Mobus.Stepwise.History

  @runtime %{
    history: [
      %{event: :next, from: :step_one, to: :step_two, at: ~U[2025-01-01 10:00:00Z]},
      %{event: :next, from: :step_two, to: :step_three, at: ~U[2025-01-01 10:01:00Z]},
      %{event: :back, from: :step_three, to: :step_two, at: ~U[2025-01-01 10:02:00Z]}
    ],
    trace: [
      %{kind: :action, handle: "test.validate", step_id: :step_one},
      %{kind: :step, direction: :next, from: :step_one, to: :step_two},
      %{kind: :action, handle: "test.process", step_id: :step_two},
      %{kind: :step, direction: :next, from: :step_two, to: :step_three}
    ]
  }

  describe "events/1" do
    test "returns full history" do
      assert length(History.events(@runtime)) == 3
    end

    test "returns empty list when missing" do
      assert History.events(%{}) == []
    end
  end

  describe "trace/1" do
    test "returns full trace" do
      assert length(History.trace(@runtime)) == 4
    end
  end

  describe "append/2" do
    test "appends with automatic timestamp" do
      updated = History.append(@runtime, %{event: :custom, from: :a, to: :b})
      assert length(History.events(updated)) == 4
      last = History.last_event(updated)
      assert last.event == :custom
      assert %DateTime{} = last.at
    end
  end

  describe "append_trace/2" do
    test "appends a trace entry" do
      updated = History.append_trace(@runtime, %{kind: :custom, data: "test"})
      assert length(History.trace(updated)) == 5
    end
  end

  describe "filtering" do
    test "events_by/2 filters by event name" do
      assert length(History.events_by(@runtime, :next)) == 2
      assert length(History.events_by(@runtime, :back)) == 1
    end

    test "trace_by_kind/2 filters by kind" do
      assert length(History.trace_by_kind(@runtime, :action)) == 2
      assert length(History.trace_by_kind(@runtime, :step)) == 2
    end
  end

  describe "last_event/1" do
    test "returns the last history entry" do
      last = History.last_event(@runtime)
      assert last.event == :back
    end

    test "returns nil for empty history" do
      assert History.last_event(%{}) == nil
    end
  end

  describe "transition_count/1" do
    test "counts transitions" do
      assert History.transition_count(@runtime) == 3
    end
  end
end
