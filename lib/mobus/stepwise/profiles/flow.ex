defmodule Mobus.Stepwise.Profiles.Flow do
  @moduledoc false

  @behaviour Mobus.Stepwise.ProfileBehaviour

  alias Mobus.Stepwise.Artifacts
  alias Mobus.Stepwise.Capabilities
  alias Mobus.Stepwise.Condition
  alias Mobus.Stepwise.IR
  alias Mobus.Stepwise.Projection
  alias Mobus.Stepwise.ProjectionHelpers

  @checkpoint_version 1
  @default_wait_event :resume

  # Fixed vocabularies. A spec or a checkpoint that has been through JSON
  # carries these as strings; they are mapped back through these tables, so
  # no atom is ever created from stored data.
  @node_types %{
    "task" => :task,
    "fork" => :fork,
    "join" => :join,
    "end" => :end,
    "for_each" => :for_each
  }
  @token_statuses %{
    "ready" => :ready,
    "waiting" => :waiting,
    "completed" => :completed,
    "failed" => :failed,
    "cancelled" => :cancelled
  }
  @failure_policies %{"fail_fast" => :fail_fast, "isolate" => :isolate}
  @join_policies %{"all" => :all, "any_success" => :any_success, "all_settled" => :all_settled}

  @token_keys ~w(token_id node_id branch_id status context artifacts wait parent_token_id
                 region_id failure_policy join_target failure_reason for_each_join)a
  @parent_keys ~w(token_id branch_id region_id failure_policy join_target for_each_join
                  parent_token_id context)a
  @for_each_keys ~w(node_id body items next_index total as max_concurrency failure_policy collect
                    parent completed failed results errors)a

  # Bounds that keep a long run's bookkeeping from growing with its length.
  @max_for_each_items 100_000
  @max_for_each_errors 50
  @log_limit 500
  @wait_keys ~w(reason deadline resume_event status)a
  @join_entry_keys ~w(token_id branch_id status context artifacts arrived_at reason)a

  @impl true
  def init(spec, runtime_context) do
    with {:ok, tenant_id} <- fetch_tenant_id(runtime_context),
         {:ok, execution_id} <- fetch_execution_id(runtime_context),
         ir <- IR.normalize(spec),
         :ok <- validate_flow_spec(ir),
         {:ok, initial_state} <- fetch_initial_state(ir) do
      runtime = %{
        execution_id: execution_id,
        tenant_id: tenant_id,
        profile: :flow,
        spec: ir,
        current_state: initial_state,
        context: Map.get(runtime_context, :initial_context, %{}) || %{},
        artifacts: %{},
        history: [],
        trace: [],
        blocked_reasons: %{},
        meta: Map.get(runtime_context, :meta, %{}) || %{},
        active_tokens: %{
          root_token_id(initial_state) =>
            new_token(root_token_id(initial_state), initial_state, nil, %{})
        },
        completed_tokens: %{},
        failed_tokens: %{},
        cancelled_tokens: %{},
        join_buffers: %{},
        branch_results: %{},
        pending_waits: %{},
        for_each: %{},
        join_parents: %{},
        errors: [],
        telemetry_scope: %{node_id: initial_state}
      }

      {:ok, compute_projection(runtime)}
    end
  end

  @impl true
  def handle_event(runtime, event, payload) do
    runtime = Map.delete(runtime, :projection)

    cond do
      cancel_event?(event) ->
        runtime
        |> cancel_tokens(payload)
        |> ok_result()

      timeout_event?(event) ->
        runtime
        |> timeout_tokens(payload)
        |> ok_result()

      true ->
        advance(runtime, event, payload)
    end
  end

  @impl true
  def get_state(%{projection: %Projection{} = projection}), do: projection
  def get_state(runtime), do: runtime |> compute_projection() |> Map.fetch!(:projection)

  @impl true
  def checkpoint(runtime) do
    runtime
    |> Map.drop([:projection, :telemetry_scope])
    |> Map.put(:checkpoint_version, @checkpoint_version)
    |> Map.take([
      :checkpoint_version,
      :execution_id,
      :tenant_id,
      :profile,
      :spec,
      :current_state,
      :context,
      :artifacts,
      :history,
      :trace,
      :blocked_reasons,
      :meta,
      :active_tokens,
      :completed_tokens,
      :failed_tokens,
      :cancelled_tokens,
      :join_buffers,
      :branch_results,
      :pending_waits,
      :for_each,
      :join_parents,
      :errors
    ])
    |> checkpoint_safe()
  end

  # Everything the engine owns in a checkpoint must survive a JSON store. The
  # only terms that do not are tuples (failure reasons such as
  # `{:no_matching_edge, node}`); they become lists. The spec is left alone:
  # `restore/3` is handed the spec and never reads it from the checkpoint.
  defp checkpoint_safe(checkpoint) do
    Map.new(checkpoint, fn
      {:spec, spec} -> {:spec, spec}
      {key, value} -> {key, json_safe(value)}
    end)
  end

  defp json_safe(%DateTime{} = value), do: value
  defp json_safe(%_{} = struct), do: struct |> Map.from_struct() |> json_safe()
  defp json_safe(%{} = map), do: Map.new(map, fn {key, value} -> {key, json_safe(value)} end)
  defp json_safe(list) when is_list(list), do: Enum.map(list, &json_safe/1)
  defp json_safe(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> json_safe()

  defp json_safe(value)
       when is_pid(value) or is_reference(value) or is_function(value) or is_port(value),
       do: inspect(value)

  defp json_safe(value), do: value

  @impl true
  def restore(spec, checkpoint, runtime_context) do
    with {:ok, tenant_id} <- fetch_tenant_id(runtime_context),
         ir <- IR.normalize(spec),
         :ok <- validate_flow_spec(ir),
         :ok <- validate_checkpoint_version(checkpoint) do
      execution_id =
        Map.get(checkpoint, :execution_id) ||
          Map.get(checkpoint, "execution_id") ||
          Map.get(runtime_context, :execution_id) ||
          Map.get(runtime_context, "execution_id") ||
          "stem-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))

      checkpoint_meta = Map.get(checkpoint, :meta) || Map.get(checkpoint, "meta") || %{}
      context_meta = Map.get(runtime_context, :meta) || %{}

      runtime = %{
        execution_id: execution_id,
        tenant_id: tenant_id,
        profile: :flow,
        spec: ir,
        current_state:
          Map.get(checkpoint, :current_state) ||
            Map.get(checkpoint, "current_state") ||
            Map.get(ir, :initial_state),
        context: Map.get(checkpoint, :context) || Map.get(checkpoint, "context") || %{},
        artifacts: Map.get(checkpoint, :artifacts) || Map.get(checkpoint, "artifacts") || %{},
        history: Map.get(checkpoint, :history) || Map.get(checkpoint, "history") || [],
        trace: Map.get(checkpoint, :trace) || Map.get(checkpoint, "trace") || [],
        blocked_reasons:
          Map.get(checkpoint, :blocked_reasons) || Map.get(checkpoint, "blocked_reasons") || %{},
        meta: Map.merge(checkpoint_meta, context_meta),
        active_tokens: checkpoint |> checkpoint_field(:active_tokens) |> rehydrate_tokens(),
        completed_tokens: checkpoint |> checkpoint_field(:completed_tokens) |> rehydrate_tokens(),
        failed_tokens: checkpoint |> checkpoint_field(:failed_tokens) |> rehydrate_tokens(),
        cancelled_tokens: checkpoint |> checkpoint_field(:cancelled_tokens) |> rehydrate_tokens(),
        join_buffers: checkpoint |> checkpoint_field(:join_buffers) |> rehydrate_join_buffers(),
        branch_results:
          checkpoint |> checkpoint_field(:branch_results) |> rehydrate_branch_results(),
        pending_waits: checkpoint |> checkpoint_field(:pending_waits) |> rehydrate_waits(),
        for_each: checkpoint |> checkpoint_field(:for_each) |> rehydrate_for_each(),
        join_parents: checkpoint |> checkpoint_field(:join_parents) |> rehydrate_parents(),
        errors: Map.get(checkpoint, :errors) || Map.get(checkpoint, "errors") || [],
        telemetry_scope: %{
          node_id:
            Map.get(checkpoint, :current_state) ||
              Map.get(checkpoint, "current_state") ||
              Map.get(ir, :initial_state)
        }
      }

      with :ok <- validate_runtime_consistency(runtime) do
        {:ok, compute_projection(runtime)}
      end
    end
  end

  defp checkpoint_field(checkpoint, key),
    do: Map.get(checkpoint, key) || Map.get(checkpoint, Atom.to_string(key)) || %{}

  # A checkpoint read back from a JSON store has string keys and string
  # statuses. These put the engine-owned shapes back. They are no-ops on a
  # checkpoint that never left memory.
  defp rehydrate_tokens(tokens) when is_map(tokens) do
    Map.new(tokens, fn {token_id, token} -> {token_id, rehydrate_token(token)} end)
  end

  defp rehydrate_tokens(_tokens), do: %{}

  defp rehydrate_token(%{} = token) do
    token = known_keys(token, @token_keys)

    token
    |> Map.update(:status, :ready, &vocab(&1, @token_statuses, :ready))
    |> Map.update(:failure_policy, :fail_fast, &vocab(&1, @failure_policies, :fail_fast))
    |> Map.update(:wait, nil, &rehydrate_wait/1)
    |> Map.put_new(:context, %{})
    |> Map.put_new(:artifacts, %{})
  end

  defp rehydrate_waits(waits) when is_map(waits) do
    Map.new(waits, fn {token_id, wait} -> {token_id, rehydrate_wait(wait)} end)
  end

  defp rehydrate_waits(_waits), do: %{}

  defp rehydrate_wait(%{} = wait) do
    wait
    |> known_keys(@wait_keys)
    |> Map.update(:status, :waiting, &vocab(&1, @token_statuses, :waiting))
    |> Map.update(:resume_event, @default_wait_event, fn
      "resume" -> :resume
      other -> other
    end)
  end

  defp rehydrate_wait(wait), do: wait

  defp rehydrate_join_buffers(buffers) when is_map(buffers) do
    Map.new(buffers, fn {join_id, buffer} ->
      buffer = known_keys(buffer, [:expected, :branches])

      branches =
        buffer
        |> Map.get(:branches, %{})
        |> Map.new(fn {branch_id, entry} ->
          entry =
            entry
            |> known_keys(@join_entry_keys)
            |> Map.update(:status, :completed, &vocab(&1, @token_statuses, :completed))

          {branch_id, entry}
        end)

      {join_id, Map.put(buffer, :branches, branches)}
    end)
  end

  defp rehydrate_join_buffers(_buffers), do: %{}

  defp rehydrate_branch_results(results) when is_map(results) do
    Map.new(results, fn {join_id, result} ->
      {join_id, known_keys(result, [:branches, :artifacts])}
    end)
  end

  defp rehydrate_branch_results(_results), do: %{}

  defp rehydrate_for_each(states) when is_map(states) do
    Map.new(states, fn {join_id, state} ->
      state =
        state
        |> known_keys(@for_each_keys)
        |> Map.update(:failure_policy, :fail_fast, &vocab(&1, @failure_policies, :fail_fast))
        |> Map.update(:parent, %{}, &rehydrate_parent/1)

      {join_id, state}
    end)
  end

  defp rehydrate_for_each(_states), do: %{}

  defp rehydrate_parents(parents) when is_map(parents) do
    Map.new(parents, fn {join_id, parent} -> {join_id, rehydrate_parent(parent)} end)
  end

  defp rehydrate_parents(_parents), do: %{}

  defp rehydrate_parent(%{} = parent) do
    parent
    |> known_keys(@parent_keys)
    |> Map.update(:failure_policy, :fail_fast, &vocab(&1, @failure_policies, :fail_fast))
  end

  defp rehydrate_parent(_parent), do: %{}

  defp known_keys(%{} = map, keys) do
    Enum.reduce(keys, map, fn key, acc ->
      string_key = Atom.to_string(key)

      case Map.pop(acc, string_key, :__absent__) do
        {:__absent__, acc} -> acc
        {value, acc} -> Map.put_new(acc, key, value)
      end
    end)
  end

  defp known_keys(other, _keys), do: other

  defp vocab(value, _table, _default) when is_atom(value) and not is_nil(value), do: value
  defp vocab(value, table, default) when is_binary(value), do: Map.get(table, value, default)
  defp vocab(_value, _table, default), do: default

  defp validate_flow_spec(%{profile: :flow} = spec) do
    nodes = Map.get(spec, :nodes, %{})
    initial = Map.get(spec, :initial_state)
    edges = Map.get(spec, :edges, [])

    cond do
      map_size(nodes) == 0 -> {:error, :missing_nodes}
      is_nil(initial) -> {:error, :missing_initial_state}
      is_nil(node_for(spec, initial)) -> {:error, {:unknown_initial_state, initial}}
      Enum.any?(edges, &invalid_edge?(spec, &1)) -> {:error, :invalid_edges}
      true -> validate_graph(spec, nodes, edges)
    end
  end

  defp validate_flow_spec(_), do: {:error, :invalid_flow_spec}

  defp validate_graph(spec, nodes, edges) do
    with :ok <- validate_node_types(nodes),
         :ok <- validate_edge_conditions(edges),
         :ok <- validate_for_each_nodes(spec, nodes) do
      validate_no_structural_cycle(spec, nodes)
    end
  end

  defp validate_for_each_nodes(spec, nodes) do
    Enum.find_value(nodes, :ok, fn {node_id, node} ->
      if node_type(node) == :for_each do
        case for_each_body(spec, node_id) do
          {:ok, _body, _join} -> validate_for_each_config(node_id, node)
          {:error, reason} -> {:error, {:invalid_for_each, node_id, reason}}
        end
      end
    end)
  end

  defp validate_for_each_config(node_id, node) do
    over = field(node, :over)
    concurrency = field(node, :max_concurrency)

    cond do
      not (is_binary(over) or is_list(over)) ->
        {:error, {:invalid_for_each, node_id, :missing_over}}

      not (is_nil(concurrency) or (is_integer(concurrency) and concurrency > 0)) ->
        {:error, {:invalid_for_each, node_id, :invalid_max_concurrency}}

      true ->
        nil
    end
  end

  # A for_each node has exactly one way into its body, and the body must end
  # at a join: that join is where the items are counted back in.
  defp for_each_body(spec, node_id) do
    case Enum.filter(outgoing_edges(spec, node_id), &(&1.on == :next)) do
      [edge] ->
        case first_join_reachable(spec, edge.to) do
          nil -> {:error, :no_join}
          join_id -> {:ok, edge.to, join_id}
        end

      [] ->
        {:error, :no_body}

      _ ->
        {:error, :more_than_one_body}
    end
  end

  defp validate_node_types(nodes) do
    Enum.find_value(nodes, :ok, fn {node_id, node} ->
      case field(node, :type) do
        type when is_binary(type) and not is_map_key(@node_types, type) ->
          {:error, {:unknown_node_type, node_id, type}}

        _ ->
          nil
      end
    end)
  end

  # Fork, join and end nodes are walked through without waiting for an event.
  # A cycle made only of them would never yield, so it is refused up front.
  defp validate_no_structural_cycle(spec, nodes) do
    structural =
      nodes
      |> Enum.filter(fn {_id, node} -> node_type(node) in [:fork, :join, :end] end)
      |> Enum.map(fn {id, _node} -> id end)

    if Enum.any?(structural, &structural_cycle?(spec, &1, &1, MapSet.new())) do
      {:error, :structural_cycle}
    else
      :ok
    end
  end

  defp structural_cycle?(spec, origin, node_id, visited) do
    spec
    |> outgoing_edges(node_id)
    |> Enum.any?(fn edge ->
      target = edge.to

      cond do
        node_type(node_for(spec, target) || %{}) not in [:fork, :join, :end] -> false
        edge_matches?(target, origin) -> true
        MapSet.member?(visited, target) -> false
        true -> structural_cycle?(spec, origin, target, MapSet.put(visited, target))
      end
    end)
  end

  defp validate_edge_conditions(edges) do
    Enum.reduce_while(edges, :ok, fn edge, :ok ->
      normalized = normalize_edge(edge)

      case normalized.when do
        nil ->
          {:cont, :ok}

        condition ->
          case Condition.validate(condition) do
            :ok ->
              {:cont, :ok}

            {:error, reason} ->
              {:halt, {:error, {:invalid_edge_condition, normalized.from, normalized.to, reason}}}
          end
      end
    end)
  end

  defp validate_runtime_consistency(runtime) do
    tokens =
      runtime.active_tokens
      |> Map.values()
      |> Kernel.++(Map.values(runtime.completed_tokens))
      |> Kernel.++(Map.values(runtime.failed_tokens))
      |> Kernel.++(Map.values(runtime.cancelled_tokens))

    cond do
      Enum.any?(tokens, fn token -> is_nil(node_for(runtime.spec, token.node_id)) end) ->
        {:error, :invalid_checkpoint_tokens}

      Enum.any?(runtime.pending_waits, fn {token_id, _wait} ->
        case Map.get(runtime.active_tokens, token_id) do
          %{status: :waiting} -> false
          _ -> true
        end
      end) ->
        {:error, :invalid_checkpoint_waits}

      true ->
        :ok
    end
  end

  defp validate_checkpoint_version(checkpoint) do
    version =
      Map.get(checkpoint, :checkpoint_version) ||
        Map.get(checkpoint, "checkpoint_version") ||
        @checkpoint_version

    if version == @checkpoint_version do
      :ok
    else
      {:error, {:unsupported_checkpoint_version, version}}
    end
  end

  defp advance(runtime, event, payload) do
    token_ids = selectable_token_ids(runtime, payload)

    {runtime, waits, error} =
      Enum.reduce_while(token_ids, {runtime, [], nil}, fn token_id, {acc, waits, _err} ->
        case Map.get(acc.active_tokens, token_id) do
          nil ->
            {:cont, {acc, waits, nil}}

          token ->
            case process_token(acc, token, event, payload) do
              {:ok, updated, nil} ->
                {:cont, {updated, waits, nil}}

              {:ok, updated, wait_cfg} ->
                {:cont, {updated, waits ++ [wait_cfg], nil}}

              {:error, reason, updated} ->
                {:halt, {updated, waits, reason}}
            end
        end
      end)

    cond do
      not is_nil(error) ->
        {:error, error, compute_projection(runtime)}

      waits != [] ->
        {:wait, compute_projection(runtime), normalize_wait_result(waits)}

      true ->
        {:ok, compute_projection(runtime)}
    end
  end

  defp selectable_token_ids(runtime, payload) do
    requested =
      Map.get(payload, :token_id) ||
        Map.get(payload, "token_id")

    cond do
      is_binary(requested) ->
        [requested]

      true ->
        runtime.active_tokens
        |> Map.values()
        |> Enum.filter(&(&1.status in [:ready, :waiting]))
        |> Enum.sort_by(& &1.token_id)
        |> Enum.map(& &1.token_id)
    end
  end

  defp process_token(runtime, token, event, payload) do
    node = node_for(runtime.spec, token.node_id)

    cond do
      is_nil(node) ->
        {:error, {:unknown_node, token.node_id},
         fail_token(runtime, token, {:unknown_node, token.node_id})}

      token.status == :waiting and not resume_event?(event, token) ->
        {:ok, runtime, nil}

      # A ready token moves only on an event meant for it. Without this an
      # untargeted `:resume` for one waiting branch would walk every other
      # ready branch past its node with the action skipped.
      token.status == :ready and not advance_event?(event, node) ->
        {:ok, runtime, nil}

      true ->
        do_process_token(runtime, token, node, event, payload)
    end
  end

  defp do_process_token(runtime, token, node, event, payload) do
    resumed? = token.status == :waiting
    resume_context = if resumed?, do: resume_context(payload), else: %{}

    token =
      if resumed? do
        token
        |> Map.put(:status, :ready)
        |> Map.put(:wait, nil)
        |> Map.update(:context, resume_context, &Map.merge(&1 || %{}, resume_context))
      else
        token
      end

    runtime =
      runtime
      |> maybe_merge_root_context(token, resume_context)
      |> put_active_token(token)
      |> Map.update(:pending_waits, %{}, &Map.delete(&1, token.token_id))

    case node_type(node) do
      :fork -> process_fork(runtime, token, node)
      :for_each -> process_for_each(runtime, token, node)
      :join -> process_join(runtime, token, node)
      :end -> process_end(runtime, token)
      _ -> process_task(runtime, token, node, event, payload)
    end
  end

  defp process_task(runtime, token, node, event, payload) do
    case maybe_run_node_action(runtime, token, node, event, payload) do
      {:ok, runtime, _token, wait_cfg} when not is_nil(wait_cfg) ->
        {:ok, runtime, wait_cfg}

      {:ok, runtime, token, nil} ->
        case move_token_to_next(runtime, token, node, event) do
          {:ok, runtime, token} -> continue_token(runtime, token, payload)
          {:isolated, runtime} -> {:ok, runtime, nil}
          {:error, reason, runtime} -> {:error, reason, runtime}
        end

      {:isolated, runtime} ->
        {:ok, runtime, nil}

      {:error, reason, runtime} ->
        {:error, reason, runtime}
    end
  end

  defp process_fork(runtime, token, node) do
    outgoing = outgoing_edges(runtime.spec, token.node_id)
    region_id = token.region_id || token.token_id
    parent_branch_context = branch_context_for(runtime, token)
    failure_policy = fork_failure_policy(node)

    runtime =
      case outgoing do
        [edge | _] -> put_join_parent(runtime, first_join_reachable(runtime.spec, edge.to), token)
        [] -> runtime
      end

    runtime =
      runtime
      |> remove_active_token(token.token_id)
      |> append_history(%{
        kind: :fork,
        node_id: token.node_id,
        token_id: token.token_id,
        at: DateTime.utc_now()
      })
      |> append_trace(%{
        kind: :fork,
        node_id: token.node_id,
        token_id: token.token_id,
        branch_count: length(outgoing)
      })

    runtime =
      Enum.reduce(outgoing, runtime, fn edge, acc ->
        branch_id = edge_branch_id(edge, edge.to)
        child_id = child_token_id(token.token_id, branch_id, edge.to)

        child =
          new_token(child_id, edge.to, branch_id, parent_branch_context)
          |> Map.put(:region_id, region_id)
          |> Map.put(:parent_token_id, token.token_id)
          |> Map.put(:failure_policy, failure_policy)
          |> Map.put(:join_target, first_join_reachable(runtime.spec, edge.to))

        put_active_token(acc, child)
      end)

    focus =
      outgoing
      |> List.first()
      |> case do
        nil -> token.node_id
        edge -> edge.to
      end

    {:ok, with_focus(runtime, focus, %{token_id: token.token_id, node_id: token.node_id}), nil}
  end

  defp process_join(runtime, %{for_each_join: join_id} = token, _node) when not is_nil(join_id) do
    if edge_matches?(join_id, token.node_id) do
      runtime
      |> remove_active_token(token.token_id)
      |> for_each_arrival(join_id, token, :completed, nil)
    else
      # An inner join inside a for_each body: an ordinary fork/join.
      process_plain_join(runtime, token)
    end
  end

  defp process_join(runtime, token, _node), do: process_plain_join(runtime, token)

  defp process_plain_join(runtime, token) do
    join_id = token.node_id
    branch_id = token.branch_id || :default

    entry = %{
      token_id: token.token_id,
      branch_id: branch_id,
      status: :completed,
      context: token.context || %{},
      artifacts: token.artifacts || %{},
      arrived_at: DateTime.utc_now()
    }

    runtime =
      runtime
      |> remove_active_token(token.token_id)
      |> put_in_join_buffer(join_id, branch_id, entry)
      |> append_history(%{
        kind: :join_wait,
        node_id: join_id,
        token_id: token.token_id,
        branch_id: branch_id,
        at: DateTime.utc_now()
      })
      |> append_trace(%{
        kind: :join_buffered,
        node_id: join_id,
        token_id: token.token_id,
        branch_id: branch_id
      })

    maybe_complete_join(runtime, join_id)
  end

  defp process_for_each(runtime, token, node) do
    {:ok, body, join_id} = for_each_body(runtime.spec, token.node_id)
    context = branch_context_for(runtime, token)

    case for_each_items(field(node, :over), context) do
      {:ok, items} when length(items) > @max_for_each_items ->
        reason = {:for_each_too_many_items, token.node_id, length(items)}
        for_each_failed(runtime, token, reason)

      {:ok, items} ->
        state = %{
          node_id: token.node_id,
          body: body,
          items: items,
          next_index: 0,
          total: length(items),
          as: field(node, :as) || context_key(runtime.spec, :item),
          max_concurrency: field(node, :max_concurrency) || 1,
          failure_policy: for_each_failure_policy(node),
          collect: field(node, :collect),
          parent: Map.take(token, @parent_keys),
          completed: 0,
          failed: 0,
          results: [],
          errors: []
        }

        runtime =
          runtime
          |> remove_active_token(token.token_id)
          |> Map.update(:for_each, %{join_id => state}, &Map.put(&1, join_id, state))
          |> append_history(%{
            kind: :for_each,
            node_id: token.node_id,
            token_id: token.token_id,
            items: state.total,
            at: DateTime.utc_now()
          })
          |> append_trace(%{
            kind: :for_each,
            node_id: token.node_id,
            token_id: token.token_id,
            items: state.total
          })

        if state.total == 0 do
          finalize_for_each(runtime, join_id)
        else
          runtime = spawn_for_each_items(runtime, join_id, state.max_concurrency)

          {:ok, with_focus(runtime, body, %{token_id: token.token_id, node_id: token.node_id}),
           nil}
        end

      {:error, reason} ->
        for_each_failed(runtime, token, {reason, token.node_id})
    end
  end

  defp for_each_failed(runtime, token, reason) do
    failed = fail_token(runtime, token, reason)
    if isolated_failure?(token), do: {:ok, failed, nil}, else: {:error, reason, failed}
  end

  defp for_each_items(items, _context) when is_list(items), do: {:ok, items}

  defp for_each_items(path, context) when is_binary(path) do
    case Condition.resolve_path(path, context) do
      items when is_list(items) -> {:ok, items}
      nil -> {:error, :for_each_list_missing}
      _other -> {:error, :for_each_not_a_list}
    end
  end

  defp for_each_items(_over, _context), do: {:error, :for_each_not_a_list}

  # `on_error: "continue"` lets the other items carry on when one fails;
  # the default stops the run at the first failure.
  defp for_each_failure_policy(node) do
    case field(node, :on_error) do
      value when value in [:continue, "continue"] -> :isolate
      _ -> :fail_fast
    end
  end

  # Starts up to `count` of the items not yet started. Each item is its own
  # branch: a token at the head of the body carrying the item.
  defp spawn_for_each_items(runtime, join_id, count) do
    state = Map.fetch!(runtime.for_each, join_id)
    last = min(state.next_index + count, state.total) - 1

    if last < state.next_index do
      runtime
    else
      parent = state.parent
      index_key = index_key(state.as)

      runtime =
        state.items
        |> Enum.slice(state.next_index..last//1)
        |> Enum.with_index(state.next_index)
        |> Enum.reduce(runtime, fn {item, index}, acc ->
          branch_id = Integer.to_string(index)

          child =
            child_token_id(parent.token_id, branch_id, state.body)
            |> new_token(
              state.body,
              branch_id,
              Map.merge(parent[:context] || %{}, %{state.as => item, index_key => index})
            )
            |> Map.put(:region_id, parent[:region_id] || parent.token_id)
            |> Map.put(:parent_token_id, parent.token_id)
            |> Map.put(:failure_policy, state.failure_policy)
            |> Map.put(:join_target, join_id)
            |> Map.put(:for_each_join, join_id)

          put_active_token(acc, child)
        end)

      put_in(runtime, [:for_each, join_id, :next_index], last + 1)
    end
  end

  defp index_key(as) when is_atom(as), do: :"#{as}_index"
  defp index_key(as), do: "#{as}_index"

  # One item is back: count it, keep what the spec asked to collect, start
  # the next item waiting for a slot, and close the loop when all are in.
  defp for_each_arrival(runtime, join_id, token, outcome, reason) do
    state = Map.fetch!(runtime.for_each, join_id)
    index = item_index(token)

    state =
      case outcome do
        :completed ->
          state
          |> Map.update!(:completed, &(&1 + 1))
          |> collect_item(index, token)

        :failed ->
          state
          |> Map.update!(:failed, &(&1 + 1))
          |> Map.update!(:errors, fn errors ->
            if length(errors) < @max_for_each_errors,
              do: errors ++ [%{index: index, reason: json_safe(reason)}],
              else: errors
          end)
      end

    runtime =
      runtime
      |> put_in([:for_each, join_id], state)
      |> append_trace(%{kind: :for_each_item, join_id: join_id, index: index, status: outcome})

    if state.completed + state.failed >= state.total do
      finalize_for_each(runtime, join_id)
    else
      {:ok, spawn_for_each_items(runtime, join_id, 1), nil}
    end
  end

  defp item_index(token) do
    case token.branch_id do
      index when is_integer(index) -> index
      index when is_binary(index) -> String.to_integer(index)
    end
  end

  defp collect_item(%{collect: nil} = state, _index, _token), do: state

  defp collect_item(state, index, token) do
    value = Condition.resolve_path(to_string(state.collect), token.context || %{})
    Map.update!(state, :results, &[[index, value] | &1])
  end

  defp finalize_for_each(runtime, join_id) do
    {state, remaining} = Map.pop(runtime.for_each, join_id)
    spec = runtime.spec

    summary =
      %{
        context_key(spec, :total) => state.total,
        context_key(spec, :completed) => state.completed,
        context_key(spec, :failed) => state.failed,
        context_key(spec, :errors) => for_each_errors(spec, state.errors)
      }
      |> then(fn summary ->
        if is_nil(state.collect) do
          summary
        else
          items = state.results |> Enum.sort_by(&hd/1) |> Enum.map(&List.last/1)
          Map.put(summary, context_key(spec, :items), items)
        end
      end)

    results = %{context_key(spec, :flow_results) => %{join_id => summary}}
    parent = state.parent

    successor =
      join_successor_token_id(join_id)
      |> new_token(
        join_id,
        parent[:branch_id],
        deep_merge(parent[:context] || %{}, successor_results(parent, results))
      )
      |> restore_parent_identity(parent)

    runtime =
      runtime
      |> Map.put(:for_each, remaining)
      |> Map.update(:branch_results, %{join_id => summary}, &Map.put(&1, join_id, summary))
      |> maybe_merge_root_context(successor, results)
      |> put_active_token(successor)
      |> append_history(%{
        kind: :for_each_complete,
        node_id: join_id,
        total: state.total,
        failed: state.failed,
        at: DateTime.utc_now()
      })
      |> append_trace(%{
        kind: :for_each_complete,
        node_id: join_id,
        total: state.total,
        failed: state.failed
      })

    leave_join(runtime, successor, join_id)
  end

  defp for_each_errors(spec, errors) do
    Enum.map(errors, fn error ->
      %{
        context_key(spec, :index) => Map.get(error, :index) || Map.get(error, "index"),
        context_key(spec, :reason) => Map.get(error, :reason) || Map.get(error, "reason")
      }
    end)
  end

  # A successor inside a branch keeps the results in its own context; one at
  # the root reads them from the run's context, where they are merged.
  defp successor_results(%{branch_id: branch_id}, results) when not is_nil(branch_id), do: results
  defp successor_results(_parent, _results), do: %{}

  defp restore_parent_identity(token, parent) do
    token
    |> Map.put(:region_id, parent[:region_id])
    |> Map.put(:parent_token_id, parent[:parent_token_id])
    |> Map.put(:failure_policy, parent[:failure_policy] || :fail_fast)
    |> Map.put(:join_target, parent[:join_target])
    |> Map.put(:for_each_join, parent[:for_each_join])
  end

  # Moves a join's successor on from the join. Conditions on the join's
  # outgoing edges see the joined results.
  defp leave_join(runtime, successor, join_id) do
    node = node_for(runtime.spec, join_id)

    case move_token_to_next(runtime, successor, node, :next) do
      {:ok, runtime, token} -> continue_token(runtime, token, %{})
      {:isolated, runtime} -> {:ok, runtime, nil}
      {:error, reason, runtime} -> {:error, reason, runtime}
    end
  end

  defp put_join_parent(runtime, nil, _token), do: runtime

  defp put_join_parent(runtime, join_id, token) do
    parent = Map.take(token, @parent_keys)
    Map.update(runtime, :join_parents, %{join_id => parent}, &Map.put(&1, join_id, parent))
  end

  defp process_end(runtime, token) do
    runtime =
      runtime
      |> complete_token(token)
      |> append_history(%{
        kind: :end,
        node_id: token.node_id,
        token_id: token.token_id,
        at: DateTime.utc_now()
      })
      |> append_trace(%{kind: :end, node_id: token.node_id, token_id: token.token_id})

    {:ok, with_focus(runtime, token.node_id, %{node_id: token.node_id, token_id: token.token_id}),
     nil}
  end

  # What the outside world hands a waiting branch when it resumes it (the
  # guest's form, the link that was clicked) arrives under `context` in the
  # event payload and lands in that branch's context.
  defp resume_context(payload) do
    case Map.get(payload, :context) || Map.get(payload, "context") do
      %{} = context -> context
      _ -> %{}
    end
  end

  defp continue_token(runtime, token, payload) do
    node = node_for(runtime.spec, token.node_id)

    case node_type(node) do
      type when type in [:fork, :for_each, :join, :end] ->
        do_process_token(runtime, token, node, :next, payload)

      _ ->
        {:ok, runtime, nil}
    end
  end

  defp maybe_run_node_action(runtime, token, node, event, payload) do
    action = normalize_action(Map.get(node, :action) || Map.get(node, "action"))

    cond do
      is_nil(action) ->
        {:ok, runtime, token, nil}

      not trigger_match?(event, action.triggers) ->
        {:ok, runtime, token, nil}

      true ->
        input = %{
          profile: :flow,
          event: event,
          payload: payload,
          node_id: token.node_id,
          token_id: token.token_id,
          branch_id: token.branch_id,
          branch_context: token.context || %{},
          graph_context: Map.get(runtime, :context, %{}),
          # The same key the `:stepwise` profile hands a capability, so one
          # capability serves both profiles: graph context with the branch's
          # own context on top.
          context: branch_context_for(runtime, token),
          join_context: join_context_for(runtime, token),
          current_state: Map.get(runtime, :current_state),
          execution_id: runtime.execution_id,
          artifacts: Artifacts.merge(runtime.artifacts, token.artifacts || %{}),
          meta: runtime.meta,
          action_config: action.config || %{}
        }

        case Capabilities.execute(runtime.tenant_id, action.handle, input) do
          {:ok, %{context: %{} = updates} = out} ->
            token =
              token
              |> Map.put(:context, Map.merge(token.context || %{}, updates))
              |> Map.put(:artifacts, Artifacts.merge(token.artifacts || %{}, artifacts_from(out)))

            runtime =
              runtime
              |> maybe_merge_root_context(token, updates)
              |> put_active_token(token)
              |> append_trace(%{
                kind: :action,
                handle: action.handle,
                node_id: token.node_id,
                token_id: token.token_id,
                branch_id: token.branch_id
              })
              |> with_focus(token.node_id, telemetry_scope(token, token.node_id))

            {:ok, runtime, token, nil}

          {:ok, other} ->
            runtime =
              runtime
              |> append_trace(%{
                kind: :action,
                handle: action.handle,
                node_id: token.node_id,
                token_id: token.token_id,
                branch_id: token.branch_id
              })
              |> with_focus(token.node_id, telemetry_scope(token, token.node_id))

            {:ok, Map.put(runtime, :action_result, other), token, nil}

          {:wait, wait_return} ->
            {context_updates, wait_cfg} = extract_wait(wait_return)

            token =
              token
              |> Map.put(:status, :waiting)
              |> Map.put(:context, Map.merge(token.context || %{}, context_updates))
              |> Map.put(
                :artifacts,
                Artifacts.merge(token.artifacts || %{}, artifacts_from(wait_return))
              )
              |> Map.put(:wait, normalize_wait(wait_cfg, event))

            runtime =
              runtime
              |> maybe_merge_root_context(token, context_updates)
              |> put_active_token(token)
              |> Map.update(:pending_waits, %{}, &Map.put(&1, token.token_id, token.wait))
              |> append_trace(%{
                kind: :action,
                handle: action.handle,
                node_id: token.node_id,
                token_id: token.token_id,
                branch_id: token.branch_id,
                status: :wait
              })
              |> with_focus(token.node_id, telemetry_scope(token, token.node_id))

            {:ok, runtime, token, token.wait}

          {:error, reason} ->
            failed = fail_token(runtime, token, reason)

            if isolated_failure?(token) do
              {:isolated, failed}
            else
              {:error, reason, failed}
            end

          {:error, reason, extra} ->
            runtime =
              runtime
              |> merge_artifacts(artifacts_from(extra))
              |> append_error(%{type: :capability_error, reason: reason, node_id: token.node_id})

            failed = fail_token(runtime, token, reason)

            if isolated_failure?(token) do
              {:isolated, failed}
            else
              {:error, reason, failed}
            end
        end
    end
  end

  defp move_token_to_next(runtime, token, _node, event) do
    outgoing = outgoing_edges(runtime.spec, token.node_id)

    case select_edge(runtime, token, outgoing) do
      {:ok, nil} ->
        {:ok,
         runtime
         |> complete_token(token)
         |> append_history(%{
           event: event,
           from: token.node_id,
           to: nil,
           token_id: token.token_id,
           at: DateTime.utc_now()
         })
         |> append_trace(%{kind: :step, from: token.node_id, to: nil, token_id: token.token_id}),
         token}

      {:ok, edge} ->
        {runtime, updated_token} = move_token_along(runtime, token, edge, event)
        {:ok, runtime, updated_token}

      {:error, reason} ->
        failed = fail_token(runtime, token, reason)

        if isolated_failure?(token) do
          {:isolated, failed}
        else
          {:error, reason, failed}
        end
    end
  end

  defp move_token_along(runtime, token, edge, event) do
    next = edge.to

    updated_token =
      token
      |> Map.put(:node_id, next)
      |> Map.put(:status, :ready)

    runtime =
      runtime
      |> put_active_token(updated_token)
      |> append_history(%{
        event: event,
        from: token.node_id,
        to: next,
        token_id: token.token_id,
        branch_id: token.branch_id,
        at: DateTime.utc_now()
      })
      |> append_trace(%{
        kind: :step,
        from: token.node_id,
        to: next,
        token_id: token.token_id,
        branch_id: token.branch_id
      })
      |> with_focus(next, telemetry_scope(updated_token, next))

    {runtime, updated_token}
  end

  # Which edge a token leaves a node by. Edges carrying a `when` condition are
  # tried in the order the spec declares them; the first that holds wins. An
  # edge without a condition is the default ("otherwise"). A node whose edges
  # are all conditional and none of which holds is a dead end the author did
  # not draw, so it is an error rather than a silent stop.
  defp select_edge(runtime, token, outgoing) do
    normal = Enum.filter(outgoing, &(&1.on == :next))
    conditional = normal |> Enum.reject(&is_nil(&1.when)) |> Enum.sort_by(& &1.index)
    defaults = Enum.filter(normal, &is_nil(&1.when))

    if conditional == [] do
      {:ok, List.first(defaults)}
    else
      context = branch_context_for(runtime, token)

      case Enum.find(conditional, &edge_condition_holds?(&1, context)) do
        nil ->
          case defaults do
            [edge | _] -> {:ok, edge}
            [] -> {:error, {:no_matching_edge, token.node_id}}
          end

        edge ->
          {:ok, edge}
      end
    end
  end

  defp edge_condition_holds?(edge, context) do
    case Condition.evaluate(edge.when, context) do
      {:ok, result} -> result
      {:error, _reason} -> false
    end
  end

  defp compute_projection(runtime) do
    focus_node = focus_node(runtime)
    focus_token = focus_token(runtime, focus_node)
    focus_context = merged_focus_context(runtime, focus_token)
    join_statuses = build_join_statuses(runtime)

    projection = %Projection{
      execution_id: Map.fetch!(runtime, :execution_id),
      profile: :flow,
      current_state: focus_node,
      available_events: available_events(runtime),
      blocked_reasons: Map.get(runtime, :blocked_reasons, %{}),
      breakpoint_hits: [],
      subscriptions:
        ProjectionHelpers.subscriptions_for(runtime.spec, %{
          runtime
          | context: focus_context
        }),
      artifacts: Map.get(runtime, :artifacts, %{}),
      ui: flow_ui_for(runtime.spec, focus_node, focus_context),
      errors: Map.get(runtime, :errors, []),
      trace: Map.get(runtime, :trace, []),
      extensions:
        ProjectionHelpers.build_extensions(runtime.spec, runtime)
        |> Map.put(:flow, %{
          focus_node: focus_node,
          active_nodes: active_nodes(runtime),
          active_tokens: Map.values(runtime.active_tokens),
          branch_statuses: branch_statuses(runtime),
          join_statuses: join_statuses,
          pending_waits: Map.get(runtime, :pending_waits, %{}),
          graph_view: %{
            active_count: map_size(runtime.active_tokens),
            completed_count: map_size(runtime.completed_tokens),
            failed_count: map_size(runtime.failed_tokens),
            cancelled_count: map_size(runtime.cancelled_tokens)
          }
        })
    }

    Map.put(runtime, :projection, projection)
  end

  defp available_events(runtime) do
    cond do
      map_size(runtime.pending_waits) > 0 -> [:resume, :cancel, :timeout]
      map_size(runtime.active_tokens) > 0 -> [:next, :cancel]
      true -> []
    end
  end

  defp flow_ui_for(spec, node_id, context) do
    node = node_for(spec, node_id) || %{}
    ui = Map.get(node, :ui) || Map.get(node, "ui") || %{}
    key = Map.get(ui, :key) || Map.get(ui, "key") || node_id
    assigns = Map.get(ui, :assigns) || Map.get(ui, "assigns") || %{}
    %{key: key, assigns: Map.merge(assigns, %{context: context, state: node_id})}
  end

  defp active_nodes(runtime) do
    runtime.active_tokens
    |> Map.values()
    |> Enum.map(& &1.node_id)
    |> Enum.uniq()
  end

  defp branch_statuses(runtime) do
    active =
      runtime.active_tokens
      |> Map.values()
      |> Enum.group_by(&(&1.branch_id || :root), &Map.take(&1, [:token_id, :node_id, :status]))

    completed =
      runtime.completed_tokens
      |> Map.values()
      |> Enum.group_by(&(&1.branch_id || :root), &Map.take(&1, [:token_id, :node_id, :status]))

    failed =
      runtime.failed_tokens
      |> Map.values()
      |> Enum.group_by(
        &(&1.branch_id || :root),
        &Map.take(&1, [:token_id, :node_id, :status, :failure_reason])
      )

    %{active: active, completed: completed, failed: failed}
  end

  defp build_join_statuses(runtime) do
    Enum.into(runtime.join_buffers, %{}, fn {join_id, data} ->
      buffered = Map.get(data, :branches, %{})
      expected = Map.get(data, :expected, [])

      {join_id,
       %{
         join_policy: join_policy(runtime.spec, join_id),
         expected_branches: expected,
         received_branches: Map.keys(buffered),
         complete?: join_policy_satisfied?(runtime.spec, join_id, buffered),
         completed_branches:
           buffered
           |> Enum.filter(fn {_branch_id, entry} -> Map.get(entry, :status) == :completed end)
           |> Enum.map(fn {branch_id, _entry} -> branch_id end),
         failed_branches:
           buffered
           |> Enum.filter(fn {_branch_id, entry} -> Map.get(entry, :status) == :failed end)
           |> Enum.map(fn {branch_id, _entry} -> branch_id end)
       }}
    end)
  end

  defp focus_node(runtime) do
    Map.get(runtime, :current_state) ||
      runtime.active_tokens
      |> Map.values()
      |> Enum.sort_by(& &1.token_id)
      |> case do
        [%{node_id: node_id} | _] -> node_id
        _ -> Map.get(runtime.spec, :initial_state)
      end
  end

  defp focus_token(runtime, node_id) do
    runtime.active_tokens
    |> Map.values()
    |> Enum.find(fn token -> token.node_id == node_id end)
  end

  defp merged_focus_context(runtime, nil), do: Map.get(runtime, :context, %{})

  defp merged_focus_context(runtime, token) do
    Map.merge(Map.get(runtime, :context, %{}), token.context || %{})
  end

  # Node ids keep the type the spec gave them. A token restored from a JSON
  # checkpoint carries a string where a spec written in code used an atom, so
  # the lookup accepts either without creating atoms.
  defp node_for(spec, node_id) do
    nodes = Map.get(spec, :nodes) || %{}

    case Map.fetch(nodes, node_id) do
      {:ok, node} ->
        node

      :error when is_atom(node_id) ->
        Map.get(nodes, Atom.to_string(node_id))

      :error when is_binary(node_id) ->
        Enum.find_value(nodes, fn
          {key, node} when is_atom(key) -> if Atom.to_string(key) == node_id, do: node
          _ -> nil
        end)

      :error ->
        nil
    end
  end

  defp node_type(node) do
    case field(node, :type) do
      nil -> :task
      type when is_binary(type) -> Map.get(@node_types, type, :task)
      type -> type
    end
  end

  defp outgoing_edges(spec, from) do
    spec
    |> Map.get(:edges, [])
    |> Enum.with_index()
    |> Enum.flat_map(fn {edge, index} ->
      normalized = edge |> normalize_edge() |> Map.put(:index, index)

      if edge_matches?(normalized.from, from) do
        [normalized]
      else
        []
      end
    end)
    |> Enum.sort_by(fn edge -> {to_string(edge.branch_id || ""), to_string(edge.to)} end)
  end

  defp incoming_edges(spec, node_id) do
    spec
    |> Map.get(:edges, [])
    |> Enum.flat_map(fn edge ->
      normalized = normalize_edge(edge)

      if edge_matches?(normalized.to, node_id) do
        [normalized]
      else
        []
      end
    end)
    |> Enum.sort_by(fn edge -> {to_string(edge.branch_id || ""), to_string(edge.from)} end)
  end

  defp expected_join_branches(spec, join_id) do
    node = node_for(spec, join_id) || %{}
    configured = Map.get(node, :expected_branches) || Map.get(node, "expected_branches")

    cond do
      is_list(configured) and configured != [] ->
        configured

      true ->
        spec
        |> incoming_edges(join_id)
        |> Enum.map(&edge_branch_id(&1, &1.from))
        |> Enum.uniq()
    end
  end

  defp normalize_edge({from, to}), do: %{from: from, to: to, branch_id: nil, when: nil, on: :next}
  defp normalize_edge([from, to]), do: %{from: from, to: to, branch_id: nil, when: nil, on: :next}

  defp normalize_edge(%{} = edge) do
    %{
      from: field(edge, :from),
      to: field(edge, :to),
      branch_id: field(edge, :branch_id),
      when: field(edge, :when),
      on: edge_on(field(edge, :on))
    }
  end

  defp normalize_edge(_edge), do: %{from: nil, to: nil, branch_id: nil, when: nil, on: :next}

  defp edge_on(on) when on in [:timeout, "timeout"], do: :timeout
  defp edge_on(_on), do: :next

  # Reads a spec field whose key may be an atom (spec written in code) or a
  # string (spec loaded from data). `false` is a value, not an absence.
  defp field(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp edge_matches?(a, b) when is_atom(a) and is_binary(b), do: Atom.to_string(a) == b
  defp edge_matches?(a, b) when is_binary(a) and is_atom(b), do: a == Atom.to_string(b)
  defp edge_matches?(a, b), do: a == b

  defp normalize_action(nil), do: nil

  defp normalize_action(%{type: type, handle: handle} = action)
       when type in [:capability, "capability"] do
    %{handle: handle, triggers: normalize_triggers(action), config: Map.get(action, :config)}
  end

  defp normalize_action(%{"type" => type, "handle" => handle} = action)
       when type in [:capability, "capability"] do
    %{
      handle: handle,
      triggers: normalize_triggers(action),
      config: Map.get(action, "config") || Map.get(action, :config)
    }
  end

  defp normalize_action(_), do: nil

  defp normalize_triggers(action) do
    raw_triggers =
      Map.get(action, :triggers) ||
        Map.get(action, "triggers") ||
        Map.get(action, :events) ||
        Map.get(action, "events") ||
        Map.get(action, :on) ||
        Map.get(action, "on") ||
        Map.get(action, :trigger) ||
        Map.get(action, "trigger")

    triggers =
      case raw_triggers do
        nil -> [:next, "next"]
        list when is_list(list) -> Enum.flat_map(list, &normalize_trigger/1)
        value -> normalize_trigger(value)
      end

    Enum.uniq(triggers)
  end

  defp normalize_trigger(:next), do: [:next, "next"]
  defp normalize_trigger("next"), do: [:next, "next"]
  defp normalize_trigger(:resume), do: [:resume, "resume"]
  defp normalize_trigger("resume"), do: [:resume, "resume"]
  defp normalize_trigger(trigger) when is_atom(trigger), do: [trigger, Atom.to_string(trigger)]
  defp normalize_trigger(trigger) when is_binary(trigger), do: [trigger]
  defp normalize_trigger(_), do: []

  defp trigger_match?(event, triggers) do
    Enum.any?(triggers, fn trigger ->
      cond do
        is_atom(event) and is_binary(trigger) -> Atom.to_string(event) == trigger
        is_binary(event) and is_atom(trigger) -> event == Atom.to_string(trigger)
        true -> event == trigger
      end
    end)
  end

  defp extract_wait(%{} = wait_return) do
    context = Map.get(wait_return, :context) || Map.get(wait_return, "context") || %{}

    wait =
      Map.get(wait_return, :wait) ||
        Map.get(wait_return, "wait") ||
        Map.drop(wait_return, [:context, "context", :artifacts, "artifacts"])

    {context, wait}
  end

  defp extract_wait(other), do: {%{}, other}

  defp artifacts_from(%{artifacts: %{} = artifacts}), do: artifacts
  defp artifacts_from(%{"artifacts" => %{} = artifacts}), do: artifacts
  defp artifacts_from(_), do: %{}

  defp normalize_wait(wait_cfg, event) when is_map(wait_cfg) do
    wait_cfg
    |> normalize_wait_deadline()
    |> Map.put_new(:resume_event, resume_event_name(event))
    |> Map.put_new(:status, :waiting)
  end

  defp normalize_wait(wait_cfg, event) do
    %{reason: wait_cfg, resume_event: resume_event_name(event), status: :waiting}
  end

  defp resume_event_name(event) when event in [:next, "next"], do: @default_wait_event
  defp resume_event_name(event), do: event

  defp cancel_event?(event), do: event in [:cancel, "cancel"]
  defp timeout_event?(event), do: event in [:timeout, "timeout"]

  defp advance_event?(event, _node) when event in [:next, "next"], do: true

  defp advance_event?(event, node) do
    case normalize_action(Map.get(node, :action) || Map.get(node, "action")) do
      nil -> false
      action -> trigger_match?(event, action.triggers)
    end
  end

  defp resume_event?(event, token) do
    wait = Map.get(token, :wait) || %{}
    event in [:resume, "resume", Map.get(wait, :resume_event), Map.get(wait, "resume_event")]
  end

  defp cancel_tokens(runtime, payload) do
    token_ids = cancel_token_ids(runtime, payload)

    Enum.reduce(token_ids, runtime, fn token_id, acc ->
      case Map.get(acc.active_tokens, token_id) do
        nil ->
          acc

        token ->
          acc
          |> remove_active_token(token_id)
          |> Map.update(
            :cancelled_tokens,
            %{},
            &Map.put(&1, token_id, Map.put(token, :status, :cancelled))
          )
          |> Map.update(:pending_waits, %{}, &Map.delete(&1, token_id))
          |> append_trace(%{
            kind: :cancelled,
            token_id: token_id,
            node_id: token.node_id,
            branch_id: token.branch_id
          })
          |> with_focus(Map.get(acc, :current_state), telemetry_scope(token, token.node_id))
      end
    end)
  end

  defp cancel_token_ids(runtime, payload) do
    requested = Map.get(payload, :token_id) || Map.get(payload, "token_id")
    branch_id = Map.get(payload, :branch_id) || Map.get(payload, "branch_id")

    cond do
      is_binary(requested) ->
        [requested]

      not is_nil(branch_id) ->
        runtime.active_tokens
        |> Map.values()
        |> Enum.filter(&(&1.branch_id == branch_id))
        |> Enum.map(& &1.token_id)

      true ->
        Map.keys(runtime.active_tokens)
    end
  end

  defp timeout_tokens(runtime, payload) do
    token_ids =
      if Map.get(payload, :token_id) || Map.get(payload, "token_id") ||
           Map.get(payload, :branch_id) || Map.get(payload, "branch_id") do
        cancel_token_ids(runtime, payload)
      else
        expired_waiting_token_ids(runtime)
      end

    Enum.reduce(token_ids, runtime, fn token_id, acc ->
      case Map.get(acc.active_tokens, token_id) do
        nil -> acc
        token -> timeout_token(acc, token)
      end
    end)
  end

  # A wait that runs out follows the node's `on: timeout` edge when it has
  # one ("the guest never clicked → do this instead"). Without one the branch
  # fails, as before.
  defp timeout_token(runtime, token) do
    case Enum.find(outgoing_edges(runtime.spec, token.node_id), &(&1.on == :timeout)) do
      nil ->
        fail_token(runtime, token, :timeout)

      edge ->
        token = token |> Map.put(:status, :ready) |> Map.put(:wait, nil)

        runtime =
          runtime
          |> Map.update(:pending_waits, %{}, &Map.delete(&1, token.token_id))
          |> append_trace(%{
            kind: :wait_timeout,
            token_id: token.token_id,
            node_id: token.node_id,
            branch_id: token.branch_id
          })

        {runtime, token} = move_token_along(runtime, token, edge, :timeout)

        case continue_token(runtime, token, %{}) do
          {:ok, updated, _wait} -> updated
          {:error, _reason, updated} -> updated
        end
    end
  end

  defp fail_token(runtime, token, reason) do
    token = Map.put(token, :status, :failed) |> Map.put(:failure_reason, reason)

    runtime =
      runtime
      |> remove_active_token(token.token_id)
      |> Map.update(:pending_waits, %{}, &Map.delete(&1, token.token_id))
      |> record_failed_token(token, reason)
      |> append_error(%{
        type: :flow_failure,
        reason: reason,
        node_id: token.node_id,
        token_id: token.token_id
      })
      |> append_trace(%{
        kind: :failed,
        token_id: token.token_id,
        node_id: token.node_id,
        branch_id: token.branch_id,
        reason: reason
      })
      |> with_focus(token.node_id, telemetry_scope(token, token.node_id))

    cond do
      isolated_failure?(token) and not is_nil(Map.get(token, :for_each_join)) ->
        runtime
        |> for_each_arrival(token.for_each_join, token, :failed, reason)
        |> case do
          {:ok, updated, _wait} -> updated
          {:error, _reason, updated} -> updated
        end

      isolated_failure?(token) and not is_nil(token.join_target) and not is_nil(token.branch_id) ->
        runtime
        |> put_in_join_buffer(token.join_target, token.branch_id, %{
          token_id: token.token_id,
          branch_id: token.branch_id,
          status: :failed,
          reason: reason,
          context: token.context || %{},
          artifacts: token.artifacts || %{},
          arrived_at: DateTime.utc_now()
        })
        |> append_trace(%{
          kind: :join_buffered_failure,
          join_id: token.join_target,
          token_id: token.token_id,
          branch_id: token.branch_id,
          reason: reason
        })
        |> maybe_complete_join(token.join_target)
        |> case do
          {:ok, updated, _wait} -> updated
          {:error, _reason, updated} -> updated
        end

      true ->
        runtime
    end
  end

  # An item of a for_each that fails under `on_error: continue` is counted in
  # the loop's summary; keeping every such token and a blocked reason for it
  # would grow with the list.
  defp record_failed_token(runtime, token, reason) do
    if isolated_failure?(token) and not is_nil(Map.get(token, :for_each_join)) do
      runtime
    else
      runtime
      |> Map.update(:failed_tokens, %{}, &Map.put(&1, token.token_id, token))
      |> Map.update(:blocked_reasons, %{}, &Map.put(&1, token.token_id, reason))
    end
  end

  defp complete_token(runtime, token) do
    token = Map.put(token, :status, :completed)

    runtime
    |> remove_active_token(token.token_id)
    |> Map.update(:completed_tokens, %{}, &Map.put(&1, token.token_id, token))
  end

  defp put_active_token(runtime, token) do
    Map.update(
      runtime,
      :active_tokens,
      %{token.token_id => token},
      &Map.put(&1, token.token_id, token)
    )
  end

  defp remove_active_token(runtime, token_id) do
    Map.update(runtime, :active_tokens, %{}, &Map.delete(&1, token_id))
  end

  defp merge_artifacts(runtime, incoming) do
    Map.update(runtime, :artifacts, Artifacts.normalize(incoming), &Artifacts.merge(&1, incoming))
  end

  defp merge_graph_context(runtime, updates) when map_size(updates) == 0, do: runtime

  defp merge_graph_context(runtime, updates) do
    Map.update(runtime, :context, updates, &deep_merge(&1, updates))
  end

  defp maybe_merge_root_context(runtime, _token, updates) when updates == %{}, do: runtime

  defp maybe_merge_root_context(runtime, token, updates) do
    if is_nil(token.branch_id) do
      merge_graph_context(runtime, updates)
    else
      runtime
    end
  end

  defp branch_context_for(runtime, token) do
    Map.merge(Map.get(runtime, :context, %{}), token.context || %{})
  end

  defp join_context_for(runtime, token) do
    runtime.branch_results
    |> Enum.find_value(%{}, fn {_join_id, data} ->
      branches = Map.get(data, :branches, %{})
      if Map.has_key?(branches, token.branch_id), do: data, else: nil
    end)
  end

  defp aggregate_join_results(buffered) do
    context =
      Enum.into(buffered, %{}, fn {branch_id, entry} ->
        if Map.get(entry, :status) == :completed do
          {branch_id, Map.get(entry, :context, %{})}
        else
          {branch_id, %{error: Map.get(entry, :reason)}}
        end
      end)

    artifacts =
      buffered
      |> Enum.reduce(%{}, fn {branch_id, entry}, acc ->
        Map.put(acc, branch_id, Artifacts.normalize(Map.get(entry, :artifacts, %{})))
      end)

    {context, artifacts}
  end

  defp record_join_result(runtime, join_id, join_context, join_artifacts) do
    runtime
    |> Map.update(:branch_results, %{}, fn existing ->
      Map.put(existing, join_id, %{branches: join_context, artifacts: join_artifacts})
    end)
  end

  defp put_in_join_buffer(runtime, join_id, branch_id, entry) do
    expected = expected_join_branches(runtime.spec, join_id)

    Map.update(
      runtime,
      :join_buffers,
      %{join_id => %{expected: expected, branches: %{branch_id => entry}}},
      fn existing ->
        Map.update(
          existing,
          join_id,
          %{expected: expected, branches: %{branch_id => entry}},
          fn join ->
            Map.update(join, :branches, %{branch_id => entry}, &Map.put(&1, branch_id, entry))
          end
        )
      end
    )
  end

  defp clear_join_buffer(runtime, join_id) do
    Map.update(runtime, :join_buffers, %{}, &Map.delete(&1, join_id))
  end

  defp maybe_complete_join(runtime, join_id) do
    buffered = get_in(runtime, [:join_buffers, join_id, :branches]) || %{}

    cond do
      join_policy_satisfied?(runtime.spec, join_id, buffered) ->
        finalize_join(runtime, join_id, buffered)

      join_policy_failed?(runtime.spec, join_id, buffered) ->
        runtime =
          runtime
          |> clear_join_buffer(join_id)
          |> Map.update(
            :blocked_reasons,
            %{},
            &Map.put(&1, join_id, {:join_policy_failed, join_policy(runtime.spec, join_id)})
          )
          |> append_error(%{
            type: :join_policy_failed,
            join_id: join_id,
            policy: join_policy(runtime.spec, join_id)
          })
          |> append_trace(%{
            kind: :join_failed,
            join_id: join_id,
            policy: join_policy(runtime.spec, join_id)
          })
          |> with_focus(join_id, %{join_id: join_id, node_id: join_id})

        {:error, {:join_policy_failed, join_id}, runtime}

      true ->
        {:ok, with_focus(runtime, join_id, %{join_id: join_id, node_id: join_id}), nil}
    end
  end

  defp finalize_join(runtime, join_id, buffered) do
    {join_context, join_artifacts} = aggregate_join_results(buffered)
    branch_ids = Map.keys(buffered)
    {parent, join_parents} = Map.pop(Map.get(runtime, :join_parents, %{}), join_id)
    parent = parent || %{}

    runtime =
      runtime
      |> Map.put(:join_parents, join_parents)
      |> record_join_result(join_id, join_context, join_artifacts)
      |> clear_join_buffer(join_id)
      |> cancel_late_branches(join_id)
      |> append_history(%{
        kind: :join_complete,
        node_id: join_id,
        branch_ids: branch_ids,
        at: DateTime.utc_now()
      })
      |> append_trace(%{
        kind: :join_complete,
        node_id: join_id,
        branch_ids: branch_ids,
        policy: join_policy(runtime.spec, join_id)
      })

    results = %{context_key(runtime.spec, :flow_results) => %{join_id => join_context}}

    if Enum.filter(outgoing_edges(runtime.spec, join_id), &(&1.on == :next)) == [] do
      {:ok,
       runtime
       |> merge_graph_context(results)
       |> merge_artifacts(join_artifacts)
       |> with_focus(join_id, %{join_id: join_id, node_id: join_id}), nil}
    else
      # A fork taken at the root hands its successor the joined results, as it
      # always has. A fork taken inside a branch hands the branch back its own
      # context with the results beside it, so the branch stays a branch.
      successor =
        if is_nil(parent[:branch_id]) do
          new_token(join_successor_token_id(join_id), join_id, nil, join_context)
        else
          join_successor_token_id(join_id)
          |> new_token(join_id, parent[:branch_id], deep_merge(parent[:context] || %{}, results))
          |> restore_parent_identity(parent)
        end

      runtime =
        runtime
        |> maybe_merge_root_context(successor, results)
        |> merge_artifacts(join_artifacts)
        |> put_active_token(successor)

      leave_join(runtime, successor, join_id)
    end
  end

  # With an "any one is enough" or quorum join, branches still running when
  # the join completes are cancelled. Left alone, one arriving later would
  # fill the emptied buffer and complete the join a second time.
  defp cancel_late_branches(runtime, join_id) do
    runtime.active_tokens
    |> Map.values()
    |> Enum.filter(fn token ->
      is_nil(Map.get(token, :for_each_join)) and not is_nil(token.join_target) and
        edge_matches?(token.join_target, join_id) and not edge_matches?(token.node_id, join_id)
    end)
    |> Enum.reduce(runtime, fn token, acc ->
      acc
      |> remove_active_token(token.token_id)
      |> Map.update(
        :cancelled_tokens,
        %{},
        &Map.put(&1, token.token_id, Map.put(token, :status, :cancelled))
      )
      |> Map.update(:pending_waits, %{}, &Map.delete(&1, token.token_id))
      |> append_trace(%{
        kind: :cancelled,
        token_id: token.token_id,
        node_id: token.node_id,
        branch_id: token.branch_id,
        reason: :join_complete
      })
    end)
  end

  # Keys the engine writes into the run's context follow the spec's own style:
  # a spec loaded from data (string ids) gets string keys, so the context
  # reads the same before and after a JSON checkpoint.
  defp context_key(spec, key) do
    if is_binary(Map.get(spec, :initial_state)), do: Atom.to_string(key), else: key
  end

  # History, trace and errors keep the most recent entries. A loop over
  # thousands of items would otherwise grow each of them, and every
  # checkpoint, with the length of the run.
  defp append_history(runtime, entry), do: append_bounded(runtime, :history, entry)
  defp append_trace(runtime, entry), do: append_bounded(runtime, :trace, entry)
  defp append_error(runtime, entry), do: append_bounded(runtime, :errors, entry)

  defp append_bounded(runtime, key, entry) do
    Map.update(runtime, key, [entry], fn entries ->
      entries = entries ++ [entry]
      overflow = length(entries) - @log_limit
      if overflow > 0, do: Enum.drop(entries, overflow), else: entries
    end)
  end

  defp with_focus(runtime, node_id, telemetry_scope) do
    runtime
    |> Map.put(:current_state, node_id)
    |> Map.put(:telemetry_scope, telemetry_scope)
  end

  defp telemetry_scope(token, node_id) do
    %{
      token_id: token.token_id,
      node_id: node_id,
      branch_id: token.branch_id
    }
  end

  defp new_token(token_id, node_id, branch_id, context) do
    %{
      token_id: token_id,
      node_id: node_id,
      branch_id: branch_id,
      status: :ready,
      context: context,
      artifacts: %{},
      wait: nil,
      parent_token_id: nil,
      region_id: nil,
      failure_policy: :fail_fast,
      join_target: nil,
      for_each_join: nil
    }
  end

  defp root_token_id(initial_state), do: "tok:root:#{initial_state}"
  defp child_token_id(parent_id, branch_id, node_id), do: "#{parent_id}:#{branch_id}:#{node_id}"

  defp join_successor_token_id(join_id),
    do: "tok:join:#{join_id}:#{System.unique_integer([:positive, :monotonic])}"

  defp edge_branch_id(edge, fallback), do: edge.branch_id || fallback

  defp fetch_tenant_id(runtime_context) do
    case Map.get(runtime_context, :tenant_id) || Map.get(runtime_context, "tenant_id") do
      nil -> {:error, :missing_tenant_id}
      tid -> {:ok, tid}
    end
  end

  defp fetch_execution_id(runtime_context) do
    case Map.get(runtime_context, :execution_id) || Map.get(runtime_context, "execution_id") do
      nil -> {:ok, "stem-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))}
      id -> {:ok, id}
    end
  end

  defp fetch_initial_state(spec) do
    case Map.get(spec, :initial_state) do
      nil -> {:error, :missing_initial_state}
      state -> {:ok, state}
    end
  end

  defp deep_merge(left, right) when is_map(left) and is_map(right) do
    Map.merge(left, right, fn _key, left_value, right_value ->
      if is_map(left_value) and is_map(right_value) do
        deep_merge(left_value, right_value)
      else
        right_value
      end
    end)
  end

  defp ok_result(runtime), do: {:ok, compute_projection(runtime)}

  defp normalize_wait_result([single]), do: single
  defp normalize_wait_result(waits), do: %{tokens: waits}

  defp invalid_edge?(spec, edge) do
    normalized = normalize_edge(edge)
    is_nil(node_for(spec, normalized.from)) or is_nil(node_for(spec, normalized.to))
  end

  defp fork_failure_policy(node) do
    (field(node, :failure_policy) || field(node, :branch_failure_policy))
    |> vocab(@failure_policies, :fail_fast)
  end

  defp isolated_failure?(token), do: Map.get(token, :failure_policy, :fail_fast) == :isolate

  # The join that closes the region a branch starting at `node_id` belongs
  # to. A fork or for_each met on the way opens a region of its own: its
  # join is stepped over, and the search carries on after it.
  defp first_join_reachable(spec, node_id, visited \\ MapSet.new()) do
    type = node_type(node_for(spec, node_id) || %{})

    cond do
      MapSet.member?(visited, node_id) ->
        nil

      type == :join ->
        node_id

      type in [:fork, :for_each] ->
        visited = MapSet.put(visited, node_id)

        with inner when not is_nil(inner) <- join_via_edges(spec, node_id, visited) do
          join_via_edges(spec, inner, MapSet.put(visited, inner))
        end

      true ->
        join_via_edges(spec, node_id, MapSet.put(visited, node_id))
    end
  end

  defp join_via_edges(spec, node_id, visited) do
    spec
    |> outgoing_edges(node_id)
    |> Enum.find_value(fn edge -> first_join_reachable(spec, edge.to, visited) end)
  end

  defp join_policy(spec, join_id) do
    node = node_for(spec, join_id) || %{}

    case field(node, :join_policy) do
      nil -> :all
      policy when is_binary(policy) -> Map.get(@join_policies, policy, :all)
      # The data forms of `{:any_success, n}` and `{:quorum, n}`.
      %{"any_success" => count} when is_integer(count) -> {:any_success, count}
      %{"quorum" => count} when is_integer(count) -> {:quorum, count}
      policy -> policy
    end
  end

  defp join_policy_satisfied?(spec, join_id, buffered) do
    expected = expected_join_branches(spec, join_id)
    completed_count = count_join_status(buffered, :completed)

    case normalize_join_policy(join_policy(spec, join_id), length(expected)) do
      {:all, expected_count} ->
        map_size(buffered) == expected_count and completed_count == expected_count

      {:quorum, needed} ->
        completed_count >= needed

      {:any_success, needed} ->
        completed_count >= needed

      {:all_settled, expected_count} ->
        map_size(buffered) == expected_count
    end
  end

  defp join_policy_failed?(spec, join_id, buffered) do
    expected_count = length(expected_join_branches(spec, join_id))
    completed_count = count_join_status(buffered, :completed)
    failed_count = count_join_status(buffered, :failed)

    case normalize_join_policy(join_policy(spec, join_id), expected_count) do
      {:all, _expected} ->
        map_size(buffered) == expected_count and failed_count > 0

      {:quorum, needed} ->
        map_size(buffered) == expected_count and completed_count < needed

      {:any_success, needed} ->
        map_size(buffered) == expected_count and completed_count < needed

      {:all_settled, _expected} ->
        false
    end
  end

  defp normalize_join_policy(:all, expected_count), do: {:all, expected_count}
  defp normalize_join_policy({:all, count}, _expected_count), do: {:all, count}
  defp normalize_join_policy(:all_settled, expected_count), do: {:all_settled, expected_count}
  defp normalize_join_policy(:any_success, _expected_count), do: {:any_success, 1}
  defp normalize_join_policy({:any_success, count}, _expected_count), do: {:any_success, count}
  defp normalize_join_policy({:quorum, count}, _expected_count), do: {:quorum, count}
  defp normalize_join_policy(other, _expected_count) when is_integer(other), do: {:quorum, other}
  defp normalize_join_policy(_, expected_count), do: {:all, expected_count}

  defp count_join_status(buffered, status) do
    Enum.count(buffered, fn {_branch_id, entry} -> Map.get(entry, :status) == status end)
  end

  defp normalize_wait_deadline(wait_cfg) do
    case Map.get(wait_cfg, :deadline) || Map.get(wait_cfg, "deadline") do
      %DateTime{} = dt ->
        Map.put(wait_cfg, :deadline, DateTime.to_iso8601(dt))

      _ ->
        wait_cfg
    end
  end

  defp expired_waiting_token_ids(runtime) do
    now = DateTime.utc_now()

    runtime.pending_waits
    |> Enum.flat_map(fn {token_id, wait_cfg} ->
      if deadline_due?(wait_cfg, now), do: [token_id], else: []
    end)
  end

  defp deadline_due?(wait_cfg, now) do
    case Map.get(wait_cfg, :deadline) || Map.get(wait_cfg, "deadline") do
      nil ->
        false

      %DateTime{} = deadline ->
        DateTime.compare(deadline, now) != :gt

      deadline when is_binary(deadline) ->
        case DateTime.from_iso8601(deadline) do
          {:ok, parsed, _offset} -> DateTime.compare(parsed, now) != :gt
          _ -> false
        end

      _ ->
        false
    end
  end
end
