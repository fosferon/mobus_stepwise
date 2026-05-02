defmodule Mobus.Stepwise.Components.StepwiseAction do
  @moduledoc """
  Executes declarative per-step actions for stepwise workflows.

  A step action is expected to live under the current state's definition:

      states: %{
        step_1: %{action: %{type: :capability, handle: "cap.handle"}},
        ...
      }

  For now, actions are executed only on `:next` (and `"next"`) events.
  When no capability runner adapter is configured (default), capability
  execution is a no-op -- suitable for form-only wizards.
  """

  alias Mobus.Stepwise.Artifacts
  alias Mobus.Stepwise.Capabilities

  @doc """
  Executes the declarative action defined for the current step, if any.

  This is an ALF pipeline stage. Looks up the current state's `:action` definition
  in `spec.states`, and if a `:capability` action is defined whose trigger matches
  the current event, executes it via `Mobus.Stepwise.Capabilities`. On success,
  merges context updates and artifacts into the runtime. On failure, sets error
  status and populates `blocked_reasons`.

  Skipped when the event has an error status or the event payload is not a map.

  ## Parameters

    * `event` — pipeline event map with `:spec`, `:runtime`, `:event`, `:payload`
    * `opts` — ALF stage options (unused)

  ## Returns

    * Updated event map, potentially with modified runtime context, artifacts,
      trace entries, or error status.

  """
  @spec call(map(), map()) :: map()
  def call(%{status: :error} = event, _opts), do: event

  def call(%{spec: spec, runtime: runtime, event: event_name, payload: payload} = event, _opts)
      when is_map(payload) do
    do_execute_action(event, spec, runtime, event_name, payload)
  end

  def call(event, _opts),
    do: Map.put(event, :status, :error) |> Map.put(:error, :invalid_action_shape)

  @doc false
  @spec run_entry_action(map()) :: map()
  def run_entry_action(%{spec: spec, runtime: runtime} = event) do
    do_execute_action(event, spec, runtime, :__enter__, %{})
  end

  defp do_execute_action(event, spec, runtime, event_name, payload) do
    event_key = normalize_event_key(event_name)

    case resolve_step_action(spec, Map.get(runtime, :current_state)) do
      nil ->
        event

      {:capability, handle, triggers, action_config} ->
        if trigger_match?(event_key, triggers) do
          run_capability(event, runtime, handle, event_name, payload, action_config)
        else
          event
        end
    end
  end

  defp run_capability(event, runtime, handle, event_name, payload, action_config) do
    tenant_id = Map.get(runtime, :tenant_id)

    input = %{
      event: event_name,
      payload: payload,
      current_state: Map.get(runtime, :current_state),
      context: Map.get(runtime, :context, %{}),
      meta: Map.get(runtime, :meta, %{}),
      artifacts: Map.get(runtime, :artifacts, %{}) || %{},
      execution_id: Map.get(runtime, :execution_id),
      action_config: action_config || %{}
    }

    case Capabilities.execute(tenant_id, handle, input) do
      {:ok, %{context: %{} = context_updates} = out} ->
        runtime =
          runtime
          |> merge_context(context_updates)
          |> Map.update(:artifacts, %{}, fn artifacts ->
            Artifacts.merge(artifacts, artifacts_from(out))
          end)
          |> Map.update(:trace, [], fn trace ->
            trace ++ [%{kind: :action, handle: handle, step_id: Map.get(runtime, :current_state)}]
          end)
          |> maybe_put_action_result(out)

        %{event | runtime: runtime}

      {:ok, other} ->
        runtime =
          runtime
          |> Map.update(:trace, [], fn trace ->
            trace ++ [%{kind: :action, handle: handle, step_id: Map.get(runtime, :current_state)}]
          end)

        %{event | runtime: runtime} |> Map.put(:action_result, other)

      # Capability yields a pause. The pipeline stamps `:wait` onto the event
      # so downstream stages (StepwiseAdvance, StepwiseEntryAction) short-circuit
      # and the engine's `handle_event/3` extractor sees `%{wait: cfg}` and
      # returns `{:wait, runtime, cfg}` to the caller. Optional `:context`
      # merges (so a wait step can still publish a `result` that becomes its
      # step output on resume).
      {:wait, wait_return} ->
        {context_updates, wait_cfg} = extract_wait(wait_return)

        runtime =
          runtime
          |> merge_context(context_updates)
          |> Map.update(:artifacts, %{}, fn artifacts ->
            Artifacts.merge(artifacts, artifacts_from(wait_return))
          end)
          |> Map.update(:trace, [], fn trace ->
            trace ++
              [
                %{
                  kind: :action,
                  handle: handle,
                  step_id: Map.get(runtime, :current_state),
                  status: :wait
                }
              ]
          end)

        event
        |> Map.put(:runtime, runtime)
        |> Map.put(:wait, wait_cfg)

      {:error, reason} ->
        runtime =
          runtime
          |> Map.update(:blocked_reasons, %{}, fn reasons ->
            Map.put(reasons, event_name, reason)
          end)

        event
        |> Map.put(:status, :error)
        |> Map.put(:error, reason)
        |> Map.put(:runtime, runtime)

      {:error, reason, extra} ->
        runtime =
          runtime
          |> merge_context(Map.get(extra, :context) || Map.get(extra, "context") || %{})
          |> Map.update(:artifacts, %{}, fn artifacts ->
            Artifacts.merge(artifacts, artifacts_from(extra))
          end)
          |> Map.update(:blocked_reasons, %{}, fn reasons ->
            Map.put(reasons, event_name, reason)
          end)

        event
        |> Map.put(:status, :error)
        |> Map.put(:error, reason)
        |> Map.put(:runtime, runtime)
    end
  end

  defp merge_context(runtime, %{} = updates) when map_size(updates) == 0, do: runtime

  defp merge_context(runtime, %{} = updates) do
    Map.update(runtime, :context, %{}, fn ctx -> Map.merge(ctx, updates) end)
  end

  defp merge_context(runtime, _), do: runtime

  defp resolve_step_action(spec, state) do
    states = Map.get(spec, :states) || %{}
    state_def = Map.get(states, state) || Map.get(states, to_string(state)) || %{}

    action = Map.get(state_def, :action) || Map.get(state_def, "action")
    normalize_action(action)
  end

  defp normalize_action(nil), do: nil

  defp normalize_action(%{type: type, handle: handle} = action)
       when type in [:capability, "capability"] do
    {:capability, handle, normalize_triggers(action), Map.get(action, :config)}
  end

  defp normalize_action(%{"type" => type, "handle" => handle} = action)
       when type in [:capability, "capability"] do
    {:capability, handle, normalize_triggers(action),
     Map.get(action, "config") || Map.get(action, :config)}
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
        nil -> default_triggers()
        list when is_list(list) -> Enum.flat_map(list, &normalize_trigger/1)
        value -> normalize_trigger(value)
      end

    triggers
    |> List.flatten()
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> default_triggers()
      list -> Enum.uniq(list)
    end
  end

  defp default_triggers, do: [:next, "next"]

  defp normalize_trigger(:enter), do: [:enter, :__enter__, "enter", "__enter__"]
  defp normalize_trigger("enter"), do: normalize_trigger(:enter)
  defp normalize_trigger(:__enter__), do: normalize_trigger(:enter)
  defp normalize_trigger(<<"__enter__"::binary>>), do: normalize_trigger(:enter)
  defp normalize_trigger(:next), do: [:next, "next"]
  defp normalize_trigger("next"), do: [:next, "next"]
  defp normalize_trigger(trigger) when is_atom(trigger), do: [trigger, Atom.to_string(trigger)]
  defp normalize_trigger(trigger) when is_binary(trigger), do: [trigger]
  defp normalize_trigger(_), do: []

  defp trigger_match?(nil, _triggers), do: false

  defp trigger_match?(event, triggers) do
    Enum.any?(triggers, fn trigger -> equivalent_event?(event, trigger) end)
  end

  defp equivalent_event?(a, b) when is_atom(a) and is_atom(b), do: a == b
  defp equivalent_event?(a, b) when is_binary(a) and is_binary(b), do: a == b
  defp equivalent_event?(a, b) when is_atom(a) and is_binary(b), do: Atom.to_string(a) == b
  defp equivalent_event?(a, b) when is_binary(a) and is_atom(b), do: a == Atom.to_string(b)
  defp equivalent_event?(_, _), do: false

  defp normalize_event_key(event) when is_atom(event), do: event
  defp normalize_event_key(event) when is_binary(event), do: event
  defp normalize_event_key(_), do: nil

  defp maybe_put_action_result(runtime, %{result: result}),
    do: Map.put(runtime, :action_result, result)

  defp maybe_put_action_result(runtime, _), do: runtime

  defp artifacts_from(%{artifacts: %{} = artifacts}), do: artifacts
  defp artifacts_from(%{"artifacts" => %{} = artifacts}), do: artifacts
  defp artifacts_from(_), do: %{}

  # Normalize the value accompanying `{:wait, ...}`. Capabilities may return:
  #   * `{:wait, wait_cfg}`          — bare wait config, no context merge
  #   * `{:wait, %{context: ctx, wait: cfg}}` — both at once
  #   * `{:wait, %{wait: cfg}}`      — explicit wait key, no context
  # Always returns `{context_updates_map, wait_cfg}` where wait_cfg may be a
  # map/keyword/binary describing the pause reason — the engine passes it
  # through opaquely to the caller.
  defp extract_wait(%{} = m) do
    context = Map.get(m, :context) || Map.get(m, "context") || %{}

    wait =
      Map.get(m, :wait) ||
        Map.get(m, "wait") ||
        Map.drop(m, [:context, "context", :artifacts, "artifacts"])

    {context, wait}
  end

  defp extract_wait(other), do: {%{}, other}
end
