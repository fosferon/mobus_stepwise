defmodule Mobus.Stepwise.Profiles.Flow do
  @moduledoc false

  @behaviour Mobus.Stepwise.ProfileBehaviour

  alias Mobus.Stepwise.Artifacts
  alias Mobus.Stepwise.Capabilities
  alias Mobus.Stepwise.IR
  alias Mobus.Stepwise.Projection
  alias Mobus.Stepwise.ProjectionHelpers

  @checkpoint_version 1
  @default_wait_event :resume

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
          root_token_id(initial_state) => new_token(root_token_id(initial_state), initial_state, nil, %{})
        },
        completed_tokens: %{},
        failed_tokens: %{},
        cancelled_tokens: %{},
        join_buffers: %{},
        branch_results: %{},
        pending_waits: %{},
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
      :errors
    ])
  end

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
        active_tokens:
          Map.get(checkpoint, :active_tokens) || Map.get(checkpoint, "active_tokens") || %{},
        completed_tokens:
          Map.get(checkpoint, :completed_tokens) ||
            Map.get(checkpoint, "completed_tokens") || %{},
        failed_tokens:
          Map.get(checkpoint, :failed_tokens) || Map.get(checkpoint, "failed_tokens") || %{},
        cancelled_tokens:
          Map.get(checkpoint, :cancelled_tokens) ||
            Map.get(checkpoint, "cancelled_tokens") || %{},
        join_buffers:
          Map.get(checkpoint, :join_buffers) || Map.get(checkpoint, "join_buffers") || %{},
        branch_results:
          Map.get(checkpoint, :branch_results) || Map.get(checkpoint, "branch_results") || %{},
        pending_waits:
          Map.get(checkpoint, :pending_waits) || Map.get(checkpoint, "pending_waits") || %{},
        errors: Map.get(checkpoint, :errors) || Map.get(checkpoint, "errors") || [],
        telemetry_scope:
          %{
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

  defp validate_flow_spec(%{profile: :flow} = spec) do
    nodes = Map.get(spec, :nodes, %{})
    initial = Map.get(spec, :initial_state)
    edges = Map.get(spec, :edges, [])

    cond do
      map_size(nodes) == 0 -> {:error, :missing_nodes}
      is_nil(initial) -> {:error, :missing_initial_state}
      is_nil(node_for(spec, initial)) -> {:error, {:unknown_initial_state, initial}}
      Enum.any?(edges, &invalid_edge?(spec, &1)) -> {:error, :invalid_edges}
      true -> :ok
    end
  end

  defp validate_flow_spec(_), do: {:error, :invalid_flow_spec}

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
        {:error, {:unknown_node, token.node_id}, fail_token(runtime, token, {:unknown_node, token.node_id})}

      token.status == :waiting and not resume_event?(event, token) ->
        {:ok, runtime, nil}

      true ->
        do_process_token(runtime, token, node, event, payload)
    end
  end

  defp do_process_token(runtime, token, node, event, payload) do
    token =
      if token.status == :waiting do
        token
        |> Map.put(:status, :ready)
        |> Map.put(:wait, nil)
      else
        token
      end

    runtime =
      runtime
      |> put_active_token(token)
      |> Map.update(:pending_waits, %{}, &Map.delete(&1, token.token_id))

    case node_type(node) do
      :fork -> process_fork(runtime, token, node)
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
        with {:ok, runtime, token} <- move_token_to_next(runtime, token, node, event) do
          continue_token(runtime, token, payload)
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
      runtime
      |> remove_active_token(token.token_id)
      |> append_history(%{kind: :fork, node_id: token.node_id, token_id: token.token_id, at: DateTime.utc_now()})
      |> append_trace(%{kind: :fork, node_id: token.node_id, token_id: token.token_id, branch_count: length(outgoing)})

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

  defp process_join(runtime, token, _node) do
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
      |> append_history(%{kind: :join_wait, node_id: join_id, token_id: token.token_id, branch_id: branch_id, at: DateTime.utc_now()})
      |> append_trace(%{kind: :join_buffered, node_id: join_id, token_id: token.token_id, branch_id: branch_id})

    maybe_complete_join(runtime, join_id)
  end

  defp process_end(runtime, token) do
    runtime =
      runtime
      |> complete_token(token)
      |> append_history(%{kind: :end, node_id: token.node_id, token_id: token.token_id, at: DateTime.utc_now()})
      |> append_trace(%{kind: :end, node_id: token.node_id, token_id: token.token_id})

    {:ok, with_focus(runtime, token.node_id, %{node_id: token.node_id, token_id: token.token_id}), nil}
  end

  defp continue_token(runtime, token, payload) do
    node = node_for(runtime.spec, token.node_id)

    case node_type(node) do
      type when type in [:fork, :join, :end] ->
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
              |> Map.put(:artifacts, Artifacts.merge(token.artifacts || %{}, artifacts_from(wait_return)))
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

    case outgoing do
      [] ->
        {:ok,
         runtime
         |> complete_token(token)
         |> append_history(%{event: event, from: token.node_id, to: nil, token_id: token.token_id, at: DateTime.utc_now()})
         |> append_trace(%{kind: :step, from: token.node_id, to: nil, token_id: token.token_id}), token}

      [edge | _] ->
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

        {:ok, runtime, updated_token}
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
      subscriptions: ProjectionHelpers.subscriptions_for(runtime.spec, %{
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
      |> Enum.group_by(&(&1.branch_id || :root), &Map.take(&1, [:token_id, :node_id, :status, :failure_reason]))

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

  defp node_for(spec, node_id) do
    nodes = Map.get(spec, :nodes) || %{}
    Map.get(nodes, node_id) || Map.get(nodes, to_string(node_id))
  end

  defp node_type(node) do
    Map.get(node, :type) || Map.get(node, "type") || :task
  end

  defp outgoing_edges(spec, from) do
    spec
    |> Map.get(:edges, [])
    |> Enum.flat_map(fn edge ->
      normalized = normalize_edge(edge)

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

  defp normalize_edge(%{from: from, to: to} = edge) do
    %{from: from, to: to, branch_id: Map.get(edge, :branch_id) || Map.get(edge, "branch_id")}
  end

  defp normalize_edge(%{"from" => from, "to" => to} = edge) do
    %{from: from, to: to, branch_id: Map.get(edge, "branch_id") || Map.get(edge, :branch_id)}
  end

  defp normalize_edge({from, to}), do: %{from: from, to: to, branch_id: nil}

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
          |> Map.update(:cancelled_tokens, %{}, &Map.put(&1, token_id, Map.put(token, :status, :cancelled)))
          |> Map.update(:pending_waits, %{}, &Map.delete(&1, token_id))
          |> append_trace(%{kind: :cancelled, token_id: token_id, node_id: token.node_id, branch_id: token.branch_id})
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
        nil ->
          acc

        token ->
          acc
          |> fail_token(token, :timeout)
      end
    end)
  end

  defp fail_token(runtime, token, reason) do
    token = Map.put(token, :status, :failed) |> Map.put(:failure_reason, reason)

    runtime =
      runtime
      |> remove_active_token(token.token_id)
      |> Map.update(:pending_waits, %{}, &Map.delete(&1, token.token_id))
      |> Map.update(:failed_tokens, %{}, &Map.put(&1, token.token_id, token))
      |> Map.update(:blocked_reasons, %{}, &Map.put(&1, token.token_id, reason))
      |> append_error(%{type: :flow_failure, reason: reason, node_id: token.node_id, token_id: token.token_id})
      |> append_trace(%{
        kind: :failed,
        token_id: token.token_id,
        node_id: token.node_id,
        branch_id: token.branch_id,
        reason: reason
      })
      |> with_focus(token.node_id, telemetry_scope(token, token.node_id))

    if isolated_failure?(token) and not is_nil(token.join_target) and not is_nil(token.branch_id) do
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
    else
      runtime
    end
  end

  defp complete_token(runtime, token) do
    token = Map.put(token, :status, :completed)

    runtime
    |> remove_active_token(token.token_id)
    |> Map.update(:completed_tokens, %{}, &Map.put(&1, token.token_id, token))
  end

  defp put_active_token(runtime, token) do
    Map.update(runtime, :active_tokens, %{token.token_id => token}, &Map.put(&1, token.token_id, token))
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

    Map.update(runtime, :join_buffers, %{join_id => %{expected: expected, branches: %{branch_id => entry}}}, fn existing ->
      Map.update(existing, join_id, %{expected: expected, branches: %{branch_id => entry}}, fn join ->
        Map.update(join, :branches, %{branch_id => entry}, &Map.put(&1, branch_id, entry))
      end)
    end)
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
          |> Map.update(:blocked_reasons, %{}, &Map.put(&1, join_id, {:join_policy_failed, join_policy(runtime.spec, join_id)}))
          |> append_error(%{type: :join_policy_failed, join_id: join_id, policy: join_policy(runtime.spec, join_id)})
          |> append_trace(%{kind: :join_failed, join_id: join_id, policy: join_policy(runtime.spec, join_id)})
          |> with_focus(join_id, %{join_id: join_id, node_id: join_id})

        {:error, {:join_policy_failed, join_id}, runtime}

      true ->
        {:ok, with_focus(runtime, join_id, %{join_id: join_id, node_id: join_id}), nil}
    end
  end

  defp finalize_join(runtime, join_id, buffered) do
    {join_context, join_artifacts} = aggregate_join_results(buffered)
    branch_ids = Map.keys(buffered)

    runtime =
      runtime
      |> record_join_result(join_id, join_context, join_artifacts)
      |> clear_join_buffer(join_id)
      |> append_history(%{kind: :join_complete, node_id: join_id, branch_ids: branch_ids, at: DateTime.utc_now()})
      |> append_trace(%{kind: :join_complete, node_id: join_id, branch_ids: branch_ids, policy: join_policy(runtime.spec, join_id)})

    next_edges = outgoing_edges(runtime.spec, join_id)

    if next_edges == [] do
      {:ok, with_focus(runtime, join_id, %{join_id: join_id, node_id: join_id}), nil}
    else
      next_node = hd(next_edges).to
      next_token = new_token(join_successor_token_id(join_id), next_node, nil, join_context)

      runtime =
        runtime
        |> merge_graph_context(%{flow_results: %{join_id => join_context}})
        |> merge_artifacts(join_artifacts)
        |> put_active_token(next_token)
        |> with_focus(next_node, %{join_id: join_id, node_id: next_node, token_id: next_token.token_id})

      continue_token(runtime, next_token, %{})
    end
  end

  defp append_history(runtime, entry) do
    Map.update(runtime, :history, [entry], &(&1 ++ [entry]))
  end

  defp append_trace(runtime, entry) do
    Map.update(runtime, :trace, [entry], &(&1 ++ [entry]))
  end

  defp append_error(runtime, entry) do
    Map.update(runtime, :errors, [entry], &(&1 ++ [entry]))
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
      join_target: nil
    }
  end

  defp root_token_id(initial_state), do: "tok:root:#{initial_state}"
  defp child_token_id(parent_id, branch_id, node_id), do: "#{parent_id}:#{branch_id}:#{node_id}"
  defp join_successor_token_id(join_id), do: "tok:join:#{join_id}:#{System.unique_integer([:positive, :monotonic])}"
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
    Map.get(node, :failure_policy) ||
      Map.get(node, "failure_policy") ||
      Map.get(node, :branch_failure_policy) ||
      Map.get(node, "branch_failure_policy") ||
      :fail_fast
  end

  defp isolated_failure?(token), do: Map.get(token, :failure_policy, :fail_fast) == :isolate

  defp first_join_reachable(spec, node_id, visited \\ MapSet.new()) do
    cond do
      MapSet.member?(visited, node_id) ->
        nil

      node_type(node_for(spec, node_id) || %{}) == :join ->
        node_id

      true ->
        visited = MapSet.put(visited, node_id)

        spec
        |> outgoing_edges(node_id)
        |> Enum.find_value(fn edge -> first_join_reachable(spec, edge.to, visited) end)
    end
  end

  defp join_policy(spec, join_id) do
    node = node_for(spec, join_id) || %{}
    Map.get(node, :join_policy) || Map.get(node, "join_policy") || :all
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
    end
  end

  defp normalize_join_policy(:all, expected_count), do: {:all, expected_count}
  defp normalize_join_policy({:all, count}, _expected_count), do: {:all, count}
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
