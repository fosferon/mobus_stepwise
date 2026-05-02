defmodule Mobus.Stepwise.Components.StepwiseAdvance do
  @moduledoc """
  Advances (or reverses) the current step for stepwise workflows.

  Step ordering is derived from:
  - `spec.steps` (list of step IDs), otherwise
  - `states` with `step_number` (ascending)

  Supported events:
  - `:next` / `"next"`
  - `:back` / `"back"`
  """

  @doc """
  Advances or reverses the current step based on the event.

  This is an ALF pipeline stage. For `:next`/`"next"` events, moves to the next
  step in order. For `:back`/`"back"`, moves to the previous step. For custom
  events, looks up explicit transitions in `spec.transitions`. Updates history
  and trace on successful transitions.

  Skipped when `skip_transition: true` is set (projection-only mode) or on error status.

  ## Parameters

    * `event` — pipeline event map with `:spec`, `:runtime`, `:event`, `:payload`
    * `opts` — ALF stage options (unused)

  ## Returns

    * Updated event map with `:runtime` (potentially new `current_state`),
      `:previous_state`, and `:state_changed?` flag.

  """
  @spec call(map(), map()) :: map()
  def call(%{skip_transition: true} = event, _opts), do: event
  def call(%{status: :error} = event, _opts), do: event
  # Capability yielded wait — do not advance. The step that returned wait
  # stays current until `Engine.restore/3` + another `handle_event/3` moves
  # the runtime forward on resume.
  def call(%{wait: wait} = event, _opts) when not is_nil(wait), do: event

  def call(%{spec: spec, runtime: runtime, event: event_name, payload: payload} = event, _opts)
      when is_map(payload) do
    current = Map.get(runtime, :current_state)
    ordered = ordered_steps(spec)

    runtime =
      case normalize_event_key(event_name) do
        :next ->
          maybe_move(runtime, ordered, current, :next, spec)

        "next" ->
          maybe_move(runtime, ordered, current, :next, spec)

        :back ->
          maybe_move(runtime, ordered, current, :back, spec)

        "back" ->
          maybe_move(runtime, ordered, current, :back, spec)

        _ ->
          maybe_apply_explicit_transition(runtime, spec, event_name, current)
      end

    event
    |> Map.put(:runtime, runtime)
    |> Map.put(:previous_state, current)
    |> Map.put(:state_changed?, not equivalent_state?(current, Map.get(runtime, :current_state)))
  end

  def call(event, _opts),
    do: Map.put(event, :status, :error) |> Map.put(:error, :invalid_stepwise_shape)

  defp maybe_move(runtime, ordered, current, dir, spec) when is_list(ordered) do
    idx = Enum.find_index(ordered, &equivalent_state?(&1, current))

    next_state =
      cond do
        is_nil(idx) ->
          nil

        dir == :next and idx + 1 < length(ordered) ->
          Enum.at(ordered, idx + 1)

        dir == :back and idx - 1 >= 0 ->
          Enum.at(ordered, idx - 1)

        true ->
          nil
      end

    if is_nil(next_state) do
      runtime
    else
      case apply_transition_policy(spec, runtime, current, next_state, dir) do
        :allow ->
          commit_move(runtime, current, next_state, dir)

        {:redirect, target} ->
          commit_move(runtime, current, target, dir)

        {:deny, reason} ->
          Map.update(runtime, :blocked_reasons, %{}, &Map.put(&1, dir, reason))
      end
    end
  end

  defp ordered_steps(spec) do
    steps = Map.get(spec, :steps) || Map.get(spec, "steps")

    cond do
      is_list(steps) and steps != [] ->
        steps

      true ->
        states = Map.get(spec, :states) || %{}

        states
        |> Enum.map(fn {k, v} ->
          step_number =
            Map.get(v, :step_number) ||
              Map.get(v, "step_number") ||
              Map.get(v, :step) ||
              Map.get(v, "step") ||
              0

          {k, step_number}
        end)
        |> Enum.sort_by(fn {_k, n} -> n end)
        |> Enum.map(fn {k, _} -> k end)
    end
  end

  defp normalize_event_key(event) when is_atom(event), do: event
  defp normalize_event_key(event) when is_binary(event), do: event
  defp normalize_event_key(_), do: nil

  defp maybe_apply_explicit_transition(runtime, spec, event_name, current) do
    case resolve_transition_to(spec, event_name) do
      nil ->
        runtime

      to_state ->
        case apply_transition_policy(spec, runtime, current, to_state, event_name) do
          :allow ->
            commit_move(runtime, current, to_state, event_name)

          {:redirect, target} ->
            commit_move(runtime, current, target, event_name)

          {:deny, reason} ->
            Map.update(runtime, :blocked_reasons, %{}, &Map.put(&1, event_name, reason))
        end
    end
  end

  defp commit_move(runtime, from, to, event) do
    trace_entry = %{kind: :step, from: from, to: to}

    trace_entry =
      if event in [:next, :back, "next", "back"],
        do: Map.put(trace_entry, :direction, event),
        else: Map.put(trace_entry, :event, event)

    runtime
    |> Map.put(:current_state, to)
    |> Map.update(:history, [], fn hist ->
      hist ++ [%{event: event, from: from, to: to, at: DateTime.utc_now()}]
    end)
    |> Map.update(:trace, [], fn trace ->
      trace ++ [trace_entry]
    end)
  end

  # Optional transition policy hook. When `spec.transition_policy` names a
  # module that exports `allow_transition?/5`, it is called before every
  # state move. Fail-open: if the module is absent, misconfigured, or raises,
  # the transition proceeds as normal.
  #
  # Expected callback:
  #   allow_transition?(spec, runtime, from_state, to_state, event) ::
  #     :allow | {:redirect, state} | {:deny, reason}
  defp apply_transition_policy(spec, runtime, from, to, event) do
    case Map.get(spec, :transition_policy) do
      nil ->
        :allow

      module when is_atom(module) ->
        if function_exported?(module, :allow_transition?, 5) do
          try do
            module.allow_transition?(spec, runtime, from, to, event)
          rescue
            _ -> :allow
          end
        else
          :allow
        end

      _ ->
        :allow
    end
  end

  defp resolve_transition_to(spec, event_name) do
    transitions = Map.get(spec, :transitions) || Map.get(spec, "transitions") || %{}

    transition =
      Map.get(transitions, event_name) ||
        Map.get(transitions, to_string(event_name)) ||
        Map.get(transitions, normalize_event_key(event_name))

    case transition do
      %{to: to} -> to
      %{"to" => to} -> to
      _ -> nil
    end
  end

  defp equivalent_state?(a, b) when is_binary(a) and is_binary(b), do: a == b
  defp equivalent_state?(a, b) when is_atom(a) and is_atom(b), do: a == b
  defp equivalent_state?(a, b) when is_atom(a) and is_binary(b), do: Atom.to_string(a) == b
  defp equivalent_state?(a, b) when is_binary(a) and is_atom(b), do: a == Atom.to_string(b)
  defp equivalent_state?(_a, _b), do: false
end
