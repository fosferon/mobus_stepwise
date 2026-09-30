defmodule Mobus.Stepwise.FlowGraphTest do
  @moduledoc """
  Graph mode as a workflow stored as data would use it: string ids and string
  keys throughout, decisions on edges, loops, per-item branches, waits with a
  way out, and a checkpoint that survives JSON.
  """

  use ExUnit.Case, async: false

  alias Mobus.Stepwise.Engine

  @runtime_context %{tenant_id: "graph-tenant", execution_id: "graph-exec-001", sync: true}

  setup do
    previous = Application.get_env(:mobus_stepwise, :capability_runner_adapter)
    Application.put_env(:mobus_stepwise, :capability_runner_adapter, __MODULE__.Adapter)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:mobus_stepwise, :capability_runner_adapter)
        adapter -> Application.put_env(:mobus_stepwise, :capability_runner_adapter, adapter)
      end
    end)

    :ok
  end

  defp action(handle, config \\ %{}),
    do: %{"type" => "capability", "handle" => handle, "config" => config}

  defp init!(spec, context \\ %{}) do
    assert {:ok, runtime} =
             Engine.init(spec, Map.put(@runtime_context, :initial_context, context))

    runtime
  end

  # Fires `:next` until nothing is ready to move.
  defp run(runtime, steps \\ 50)
  defp run(runtime, 0), do: flunk("run did not settle: #{inspect(runtime.active_tokens)}")

  defp run(runtime, steps) do
    ready? = Enum.any?(Map.values(runtime.active_tokens), &(&1.status == :ready))

    if ready? do
      case Engine.handle_event(runtime, :next, %{}) do
        {:ok, runtime} -> run(runtime, steps - 1)
        {:wait, runtime, _wait} -> run(runtime, steps - 1)
        {:error, reason, runtime} -> {:error, reason, runtime}
      end
    else
      runtime
    end
  end

  defp decision_spec do
    %{
      "profile" => "flow",
      "initial_state" => "check",
      "nodes" => %{
        "check" => %{"action" => action("test.availability")},
        "redirect" => %{"action" => action("test.mark", %{"as" => "redirected"})},
        "show_form" => %{"action" => action("test.mark", %{"as" => "form_shown"})},
        "done" => %{"type" => "end"}
      },
      "edges" => [
        %{
          "from" => "check",
          "to" => "redirect",
          "when" => %{"op" => "truthy", "left" => "availability.found"}
        },
        %{"from" => "check", "to" => "show_form"},
        ["redirect", "done"],
        ["show_form", "done"]
      ]
    }
  end

  describe "decisions" do
    test "a node leaves by the first edge whose condition holds" do
      runtime = init!(decision_spec(), %{"rooms" => 2}) |> run()

      assert runtime.context["marked"] == "redirected"
      assert runtime.current_state == "done"
      assert runtime.active_tokens == %{}
    end

    test "with no condition holding, the edge without one is the otherwise" do
      runtime = init!(decision_spec(), %{"rooms" => 0}) |> run()

      assert runtime.context["marked"] == "form_shown"
    end

    test "conditional edges are tried in declared order, not sorted by target" do
      spec =
        put_in(decision_spec(), ["edges"], [
          %{"from" => "check", "to" => "show_form", "when" => true},
          %{"from" => "check", "to" => "redirect", "when" => true},
          ["redirect", "done"],
          ["show_form", "done"]
        ])

      runtime = init!(spec, %{"rooms" => 2}) |> run()
      assert runtime.context["marked"] == "form_shown"
    end

    test "every edge conditional and none holding is an error, not a silent stop" do
      spec =
        put_in(decision_spec(), ["edges"], [
          %{
            "from" => "check",
            "to" => "redirect",
            "when" => %{"op" => "truthy", "left" => "availability.found"}
          },
          ["redirect", "done"],
          ["show_form", "done"]
        ])

      assert {:error, {:no_matching_edge, "check"}, runtime} =
               init!(spec, %{"rooms" => 0}) |> run()

      assert runtime.failed_tokens["tok:root:check"].failure_reason ==
               {:no_matching_edge, "check"}
    end

    test "a malformed condition is refused when the run starts" do
      spec =
        put_in(decision_spec(), ["edges"], [
          %{
            "from" => "check",
            "to" => "redirect",
            "when" => %{"op" => "regex", "left" => "a", "right" => "b"}
          }
        ])

      assert {:error,
              {:invalid_edge_condition, "check", "redirect",
               {:invalid_condition, {:unknown_op, "regex"}}}} =
               Engine.init(spec, @runtime_context)
    end
  end

  describe "loops" do
    test "an edge back to an earlier node loops until its exit condition holds" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "count",
        "nodes" => %{
          "count" => %{"action" => action("test.increment")},
          "done" => %{"type" => "end"}
        },
        "edges" => [
          %{
            "from" => "count",
            "to" => "done",
            "when" => %{"op" => "gte", "left" => "n", "right" => 3}
          },
          ["count", "count"]
        ]
      }

      runtime = init!(spec) |> run()

      assert runtime.context["n"] == 3
      assert runtime.current_state == "done"
    end
  end

  describe "capability input" do
    test "carries `context`, the key the step-by-step profile uses" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "capture",
        "nodes" => %{"capture" => %{"action" => action("test.capture")}},
        "edges" => []
      }

      init!(spec, %{"seed" => 1}) |> run()

      assert_received {:capability_input, input}
      assert input.context == %{"seed" => 1}
      assert input.graph_context == %{"seed" => 1}
    end
  end

  describe "events reach only the tokens they are meant for" do
    test "an untargeted resume does not walk a ready branch past its action" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "fork",
        "nodes" => %{
          "fork" => %{"type" => "fork"},
          "wait" => %{"action" => Map.put(action("test.wait"), "triggers", ["next", "resume"])},
          "work" => %{"action" => action("test.mark", %{"as" => "worked"})},
          "after_work" => %{"action" => action("test.mark", %{"as" => "after"})},
          "join" => %{"type" => "join"},
          "done" => %{"type" => "end"}
        },
        "edges" => [
          %{"from" => "fork", "to" => "wait", "branch_id" => "a"},
          %{"from" => "fork", "to" => "work", "branch_id" => "b"},
          %{"from" => "wait", "to" => "join", "branch_id" => "a"},
          ["work", "after_work"],
          %{"from" => "after_work", "to" => "join", "branch_id" => "b"},
          ["join", "done"]
        ]
      }

      runtime = init!(spec)
      # fork
      assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      # branch a waits; branch b runs `work` and sits ready at `after_work`
      assert {:wait, runtime, _} = Engine.handle_event(runtime, :next, %{})

      b = "tok:root:fork:b:work"
      assert runtime.active_tokens[b].node_id == "after_work"
      assert runtime.active_tokens[b].context["marked"] == "worked"

      # resume with no token named: branch a resumes, branch b must not move
      assert {:ok, runtime} = Engine.handle_event(runtime, :resume, %{})
      assert runtime.active_tokens[b].node_id == "after_work"
      assert runtime.active_tokens[b].context["marked"] == "worked"

      runtime = run(runtime)
      assert runtime.current_state == "done"
      assert runtime.context["flow_results"]["join"]["b"]["marked"] == "after"
    end
  end

  describe "a spec loaded from data" do
    test "string node types, policies and ids behave as their atom forms do" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "fork",
        "nodes" => %{
          "fork" => %{"type" => "fork", "failure_policy" => "isolate"},
          "good" => %{"action" => action("test.mark", %{"as" => "good"})},
          "bad" => %{"action" => action("test.fail")},
          "join" => %{"type" => "join", "join_policy" => "any_success"},
          "done" => %{"type" => "end"}
        },
        "edges" => [
          %{"from" => "fork", "to" => "bad", "branch_id" => "a"},
          %{"from" => "fork", "to" => "good", "branch_id" => "b"},
          %{"from" => "bad", "to" => "join", "branch_id" => "a"},
          %{"from" => "good", "to" => "join", "branch_id" => "b"},
          ["join", "done"]
        ]
      }

      runtime = init!(spec) |> run()

      assert runtime.current_state == "done"
      assert runtime.context["flow_results"]["join"]["b"] == %{"marked" => "good"}
      assert runtime.context["flow_results"]["join"]["a"] == %{error: :boom}
    end

    test "an unknown node type is refused rather than run as a plain task" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "a",
        "nodes" => %{"a" => %{"type" => "frok"}},
        "edges" => []
      }

      assert {:error, {:unknown_node_type, "a", "frok"}} = Engine.init(spec, @runtime_context)
    end

    test "a cycle of nodes that never wait for an event is refused" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "start",
        "nodes" => %{
          "start" => %{},
          "fork" => %{"type" => "fork"},
          "join" => %{"type" => "join"}
        },
        "edges" => [["start", "fork"], ["fork", "join"], ["join", "fork"]]
      }

      assert {:error, :structural_cycle} = Engine.init(spec, @runtime_context)
    end
  end

  describe "checkpoints through JSON" do
    defp through_json(checkpoint), do: checkpoint |> Jason.encode!() |> Jason.decode!()

    defp wait_spec do
      %{
        "profile" => "flow",
        "initial_state" => "ask",
        "nodes" => %{
          "ask" => %{"action" => action("test.wait")},
          "thank" => %{"action" => action("test.mark", %{"as" => "thanked"})},
          "done" => %{"type" => "end"}
        },
        "edges" => [["ask", "thank"], ["thank", "done"]]
      }
    end

    test "a waiting run is restored from a JSON checkpoint and resumes with what it is handed" do
      assert {:wait, runtime, _wait} =
               Engine.handle_event(init!(wait_spec(), %{"seed" => 1}), :next, %{})

      stored = runtime |> Engine.checkpoint() |> Map.delete(:spec) |> through_json()
      assert is_binary(stored["active_tokens"]["tok:root:ask"]["status"])

      assert {:ok, restored} = Engine.restore(wait_spec(), stored, @runtime_context)
      assert restored.active_tokens["tok:root:ask"].status == :waiting
      assert restored.context == %{"seed" => 1}

      assert {:ok, restored} =
               Engine.handle_event(restored, :resume, %{
                 "token_id" => "tok:root:ask",
                 "context" => %{"guest" => %{"email" => "ada@example.com"}}
               })

      restored = run(restored)

      assert restored.current_state == "done"
      assert restored.context["guest"]["email"] == "ada@example.com"
      assert restored.context["marked"] == "thanked"
    end

    test "a spec written with atoms is restored from a JSON checkpoint too" do
      spec = %{
        profile: :flow,
        initial_state: :ask,
        nodes: %{
          ask: %{action: %{type: :capability, handle: "test.wait"}},
          done: %{type: :end}
        },
        edges: [%{from: :ask, to: :done}]
      }

      assert {:wait, runtime, _wait} = Engine.handle_event(init!(spec), :next, %{})
      stored = runtime |> Engine.checkpoint() |> Map.delete(:spec) |> through_json()

      assert {:ok, restored} = Engine.restore(spec, stored, @runtime_context)
      assert {:ok, restored} = Engine.handle_event(restored, :resume, %{})
      assert restored.active_tokens == %{}
      assert map_size(restored.completed_tokens) == 1
    end

    test "a failed run's checkpoint encodes: failure reasons carry no tuples" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "check",
        "nodes" => %{"check" => %{}, "next" => %{}},
        "edges" => [%{"from" => "check", "to" => "next", "when" => false}]
      }

      assert {:error, {:no_matching_edge, "check"}, runtime} = init!(spec) |> run()

      stored = runtime |> Engine.checkpoint() |> through_json()

      assert stored["failed_tokens"]["tok:root:check"]["failure_reason"] == [
               "no_matching_edge",
               "check"
             ]

      assert {:ok, _restored} = Engine.restore(spec, stored, @runtime_context)
    end
  end

  describe "a wait that runs out" do
    defp expiring_spec(edges) do
      %{
        "profile" => "flow",
        "initial_state" => "ask",
        "nodes" => %{
          "ask" => %{"action" => action("test.expired_wait")},
          "claimed" => %{"action" => action("test.mark", %{"as" => "claimed"})},
          "lapsed" => %{"action" => action("test.mark", %{"as" => "lapsed"})},
          "done" => %{"type" => "end"}
        },
        "edges" => edges ++ [["claimed", "done"], ["lapsed", "done"]]
      }
    end

    test "follows the node's timeout edge" do
      spec =
        expiring_spec([
          ["ask", "claimed"],
          %{"from" => "ask", "to" => "lapsed", "on" => "timeout"}
        ])

      assert {:wait, runtime, _wait} = Engine.handle_event(init!(spec), :next, %{})
      assert {:ok, runtime} = Engine.handle_event(runtime, :timeout, %{})

      assert runtime.pending_waits == %{}
      assert runtime.active_tokens["tok:root:ask"].node_id == "lapsed"

      runtime = run(runtime)
      assert runtime.context["marked"] == "lapsed"
      assert runtime.failed_tokens == %{}
    end

    test "the timeout edge is never taken by an ordinary resume" do
      spec =
        expiring_spec([
          ["ask", "claimed"],
          %{"from" => "ask", "to" => "lapsed", "on" => "timeout"}
        ])

      assert {:wait, runtime, _wait} = Engine.handle_event(init!(spec), :next, %{})
      assert {:ok, runtime} = Engine.handle_event(runtime, :resume, %{})

      assert run(runtime).context["marked"] == "claimed"
    end

    test "fails the branch when the node has no timeout edge" do
      spec = expiring_spec([["ask", "claimed"]])

      assert {:wait, runtime, _wait} = Engine.handle_event(init!(spec), :next, %{})
      assert {:ok, runtime} = Engine.handle_event(runtime, :timeout, %{})

      assert runtime.failed_tokens["tok:root:ask"].failure_reason == :timeout
    end
  end

  describe "for_each" do
    defp for_each_spec(node \\ %{}, body_action \\ "test.double") do
      %{
        "profile" => "flow",
        "initial_state" => "each",
        "nodes" => %{
          "each" =>
            Map.merge(%{"type" => "for_each", "over" => "numbers", "collect" => "doubled"}, node),
          "work" => %{"action" => action(body_action)},
          "collected" => %{"type" => "join"},
          "report" => %{"action" => action("test.capture")},
          "done" => %{"type" => "end"}
        },
        "edges" => [
          ["each", "work"],
          ["work", "collected"],
          ["collected", "report"],
          ["report", "done"]
        ]
      }
    end

    test "runs the body once per item and hands back what it was asked to collect" do
      runtime = init!(for_each_spec(), %{"numbers" => [1, 2, 3]}) |> run()

      assert runtime.current_state == "done"

      assert runtime.context["flow_results"]["collected"] == %{
               "total" => 3,
               "completed" => 3,
               "failed" => 0,
               "errors" => [],
               "items" => [2, 4, 6]
             }

      # the step after the loop sees the results, and no item leaks into it
      assert_received {:capability_input, input}
      assert input.context["flow_results"]["collected"]["completed"] == 3
      refute Map.has_key?(input.context, "item")
    end

    test "each item sees itself and its position, and nothing of its siblings" do
      init!(for_each_spec(%{"as" => "booking"}, "test.capture"), %{"numbers" => ["a", "b"]})
      |> run()

      assert_received {:capability_input,
                       %{context: %{"booking" => "a", "booking_index" => 0} = first}}

      assert_received {:capability_input, %{context: %{"booking" => "b", "booking_index" => 1}}}
      assert first["numbers"] == ["a", "b"]
    end

    test "at most max_concurrency items are under way at once" do
      runtime = init!(for_each_spec(%{"max_concurrency" => 2}), %{"numbers" => [1, 2, 3, 4, 5]})

      assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert map_size(runtime.active_tokens) == 2

      # the first two come back, and the next two take their places
      assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert map_size(runtime.active_tokens) == 2
      assert runtime.for_each["collected"].completed == 2
      assert runtime.for_each["collected"].next_index == 4

      assert run(runtime).context["flow_results"]["collected"]["items"] == [2, 4, 6, 8, 10]
    end

    test "an empty list goes straight on" do
      runtime = init!(for_each_spec(), %{"numbers" => []}) |> run()

      assert runtime.current_state == "done"
      assert runtime.context["flow_results"]["collected"]["total"] == 0
    end

    test "on_error stop ends the run at the first failing item" do
      assert {:error, :odd_item, runtime} =
               init!(for_each_spec(%{}, "test.fail_odd"), %{"numbers" => [2, 3, 4]}) |> run()

      assert runtime.for_each["collected"].completed == 1
    end

    test "on_error continue finishes the list and reports what failed" do
      runtime =
        init!(for_each_spec(%{"on_error" => "continue"}, "test.fail_odd"), %{
          "numbers" => [2, 3, 4, 5]
        })
        |> run()

      assert runtime.current_state == "done"
      results = runtime.context["flow_results"]["collected"]

      assert %{"total" => 4, "completed" => 2, "failed" => 2, "items" => [4, 8]} = results

      assert results["errors"] == [
               %{"index" => 1, "reason" => :odd_item},
               %{"index" => 3, "reason" => :odd_item}
             ]

      # failed items are counted, not kept
      assert runtime.failed_tokens == %{}
      assert runtime.blocked_reasons == %{}
    end

    test "the edges out of the loop can decide on how it went" do
      spec =
        for_each_spec(%{"on_error" => "continue"}, "test.fail_odd")
        |> put_in(["nodes", "alert"], %{"action" => action("test.mark", %{"as" => "alerted"})})
        |> Map.put("edges", [
          ["each", "work"],
          ["work", "collected"],
          %{
            "from" => "collected",
            "to" => "alert",
            "when" => %{"op" => "gt", "left" => "flow_results.collected.failed", "right" => 0}
          },
          ["collected", "done"],
          ["alert", "done"]
        ])

      assert run(init!(spec, %{"numbers" => [2, 3]})).context["marked"] == "alerted"
      assert run(init!(spec, %{"numbers" => [2, 4]})).context["marked"] == nil
    end

    test "items can wait, and a loop in flight survives a JSON checkpoint" do
      spec = for_each_spec(%{"max_concurrency" => 2, "collect" => "answer"}, "test.wait")
      runtime = init!(spec, %{"numbers" => [1, 2, 3]})

      assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert {:wait, runtime, _waits} = Engine.handle_event(runtime, :next, %{})
      assert map_size(runtime.pending_waits) == 2

      stored =
        runtime |> Engine.checkpoint() |> Map.delete(:spec) |> Jason.encode!() |> Jason.decode!()

      assert {:ok, runtime} = Engine.restore(spec, stored, @runtime_context)
      assert runtime.for_each["collected"].next_index == 2

      runtime =
        Enum.reduce(0..2, runtime, fn index, acc ->
          acc = run(acc)
          token_id = "tok:root:each:#{index}:work"

          assert {:ok, acc} =
                   Engine.handle_event(acc, :resume, %{
                     "token_id" => token_id,
                     "context" => %{"answer" => "yes-#{index}"}
                   })

          acc
        end)
        |> run()

      assert runtime.current_state == "done"
      assert runtime.context["flow_results"]["collected"]["items"] == ["yes-0", "yes-1", "yes-2"]
    end

    test "a fork inside the body joins back into the item's own branch" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "each",
        "nodes" => %{
          "each" => %{"type" => "for_each", "over" => "numbers", "collect" => "doubled"},
          "split" => %{"type" => "fork"},
          "left" => %{"action" => action("test.mark", %{"as" => "left"})},
          "right" => %{"action" => action("test.mark", %{"as" => "right"})},
          "merge" => %{"type" => "join"},
          "work" => %{"action" => action("test.double")},
          "collected" => %{"type" => "join"},
          "done" => %{"type" => "end"}
        },
        "edges" => [
          ["each", "split"],
          %{"from" => "split", "to" => "left", "branch_id" => "l"},
          %{"from" => "split", "to" => "right", "branch_id" => "r"},
          %{"from" => "left", "to" => "merge", "branch_id" => "l"},
          %{"from" => "right", "to" => "merge", "branch_id" => "r"},
          ["merge", "work"],
          ["work", "collected"],
          ["collected", "done"]
        ]
      }

      runtime = init!(spec, %{"numbers" => [5, 6]}) |> run()

      assert runtime.current_state == "done"
      assert runtime.context["flow_results"]["collected"]["items"] == [10, 12]
    end

    test "a body that never reaches a join is refused when the run starts" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "each",
        "nodes" => %{"each" => %{"type" => "for_each", "over" => "numbers"}, "work" => %{}},
        "edges" => [["each", "work"]]
      }

      assert {:error, {:invalid_for_each, "each", :no_join}} = Engine.init(spec, @runtime_context)
    end

    test "something that is not a list is an error" do
      assert {:error, {:for_each_not_a_list, "each"}, _runtime} =
               init!(for_each_spec(), %{"numbers" => "1,2,3"}) |> run()

      assert {:error, {:for_each_list_missing, "each"}, _runtime} =
               init!(for_each_spec()) |> run()
    end

    test "a long list does not grow the run's logs with it" do
      numbers = Enum.to_list(1..400)
      runtime = init!(for_each_spec(%{"collect" => nil}), %{"numbers" => numbers}) |> run(2_000)

      assert runtime.current_state == "done"
      assert runtime.context["flow_results"]["collected"]["completed"] == 400
      assert length(runtime.trace) <= 500
      assert length(runtime.history) <= 500
      assert map_size(runtime.completed_tokens) <= 2
    end
  end

  describe "retry" do
    test "puts a failed branch back at the step it failed on, and the run carries on" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "each",
        "nodes" => %{
          "each" => %{"type" => "for_each", "over" => "numbers", "collect" => "doubled"},
          "work" => %{"action" => action("test.flaky")},
          "collected" => %{"type" => "join"},
          "done" => %{"type" => "end"}
        },
        "edges" => [["each", "work"], ["work", "collected"], ["collected", "done"]]
      }

      Process.put(:flaky_fails, true)
      assert {:error, :not_now, runtime} = init!(spec, %{"numbers" => [1, 2, 3]}) |> run()
      assert map_size(runtime.failed_tokens) == 1

      # through storage, as a failed run would be
      stored =
        runtime |> Engine.checkpoint() |> Map.delete(:spec) |> Jason.encode!() |> Jason.decode!()

      assert {:ok, runtime} = Engine.restore(spec, stored, @runtime_context)

      Process.put(:flaky_fails, false)
      assert {:ok, runtime} = Engine.handle_event(runtime, :retry, %{})
      assert runtime.failed_tokens == %{}
      assert runtime.blocked_reasons == %{}

      runtime = run(runtime)
      assert runtime.current_state == "done"
      assert runtime.context["flow_results"]["collected"]["items"] == [2, 4, 6]
    end

    test "leaves alone a failure that a join already took into account" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "fork",
        "nodes" => %{
          "fork" => %{"type" => "fork", "failure_policy" => "isolate"},
          "good" => %{"action" => action("test.mark", %{"as" => "good"})},
          "bad" => %{"action" => action("test.fail")},
          "join" => %{"type" => "join", "join_policy" => "any_success"},
          "done" => %{"type" => "end"}
        },
        "edges" => [
          %{"from" => "fork", "to" => "bad", "branch_id" => "a"},
          %{"from" => "fork", "to" => "good", "branch_id" => "b"},
          %{"from" => "bad", "to" => "join", "branch_id" => "a"},
          %{"from" => "good", "to" => "join", "branch_id" => "b"},
          ["join", "done"]
        ]
      }

      runtime = init!(spec) |> run()
      assert runtime.current_state == "done"
      assert runtime.failed_tokens["tok:root:fork:a:bad"].absorbed == true

      assert {:ok, runtime} = Engine.handle_event(runtime, :retry, %{})
      assert runtime.active_tokens == %{}
      assert Map.has_key?(runtime.failed_tokens, "tok:root:fork:a:bad")
    end
  end

  describe "a join that needs only some branches" do
    test "cancels the branches still running, so none can complete it twice" do
      spec = %{
        "profile" => "flow",
        "initial_state" => "fork",
        "nodes" => %{
          "fork" => %{"type" => "fork"},
          "quick" => %{"action" => action("test.mark", %{"as" => "quick"})},
          "slow" => %{"action" => action("test.wait")},
          "join" => %{"type" => "join", "join_policy" => "any_success"},
          "after" => %{"action" => action("test.increment")},
          "done" => %{"type" => "end"}
        },
        "edges" => [
          %{"from" => "fork", "to" => "quick", "branch_id" => "q"},
          %{"from" => "fork", "to" => "slow", "branch_id" => "s"},
          %{"from" => "quick", "to" => "join", "branch_id" => "q"},
          %{"from" => "slow", "to" => "join", "branch_id" => "s"},
          ["join", "after"],
          ["after", "done"]
        ]
      }

      runtime = init!(spec)
      assert {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      runtime = run(runtime)

      assert runtime.current_state == "done"
      assert runtime.context["n"] == 1
      assert runtime.pending_waits == %{}
      assert Map.has_key?(runtime.cancelled_tokens, "tok:root:fork:s:slow")

      # the late branch is gone: resuming it changes nothing
      assert {:ok, runtime} =
               Engine.handle_event(runtime, :resume, %{"token_id" => "tok:root:fork:s:slow"})

      assert run(runtime).context["n"] == 1
    end
  end

  defmodule Adapter do
    def execute(_tenant, "test.availability", %{context: context}) do
      {:ok, %{context: %{"availability" => %{"found" => Map.get(context, "rooms", 0) > 0}}}}
    end

    def execute(_tenant, "test.mark", %{action_config: config}) do
      {:ok, %{context: %{"marked" => config["as"]}}}
    end

    def execute(_tenant, "test.increment", %{context: context}) do
      {:ok, %{context: %{"n" => Map.get(context, "n", 0) + 1}}}
    end

    def execute(_tenant, "test.capture", input) do
      send(self(), {:capability_input, input})
      {:ok, %{context: %{}}}
    end

    def execute(_tenant, "test.fail", _input), do: {:error, :boom}

    def execute(_tenant, "test.flaky", %{context: %{"item" => item}}) do
      if Process.get(:flaky_fails) and item == 2,
        do: {:error, :not_now},
        else: {:ok, %{context: %{"doubled" => item * 2}}}
    end

    def execute(_tenant, "test.double", %{context: %{"item" => item}}) do
      {:ok, %{context: %{"doubled" => item * 2}}}
    end

    def execute(_tenant, "test.fail_odd", %{context: %{"item" => item}}) do
      if rem(item, 2) == 1,
        do: {:error, :odd_item},
        else: {:ok, %{context: %{"doubled" => item * 2}}}
    end

    def execute(_tenant, "test.expired_wait", %{event: event}) when event in [:next, "next"] do
      deadline = DateTime.utc_now() |> DateTime.add(-5, :second)
      {:wait, %{wait: %{reason: "link", deadline: deadline}}}
    end

    def execute(_tenant, "test.wait", %{event: event}) when event in [:next, "next"] do
      {:wait, %{wait: %{reason: "approval"}}}
    end

    def execute(_tenant, "test.wait", _input), do: {:ok, %{context: %{"resumed" => true}}}
  end
end
