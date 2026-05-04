defmodule Mobus.Stepwise.ProjectionHelpers do
  @moduledoc """
  Shared projection helper functions used by all profile projections
  (stepwise, FSM, flow).

  Extracted to eliminate duplication across projection components.
  """

  @doc """
  Builds the subscriptions list from spec declarations and runtime context,
  prepending the mandatory workflow_execution topic and interpolating
  `{placeholder}` variables from runtime context.
  """
  @spec subscriptions_for(map(), map()) :: [String.t()]
  def subscriptions_for(spec, runtime) do
    base = ["workflow_execution:#{Map.fetch!(runtime, :execution_id)}"]

    declared =
      Map.get(spec, :subscriptions) ||
        Map.get(spec, "subscriptions") ||
        []

    declared
    |> Enum.flat_map(fn
      topic when is_binary(topic) -> [topic]
      %{topic: topic} when is_binary(topic) -> [topic]
      %{"topic" => topic} when is_binary(topic) -> [topic]
      _ -> []
    end)
    |> Enum.map(&interpolate_topic(&1, Map.get(runtime, :context, %{}) || %{}))
    |> Enum.reject(&is_nil/1)
    |> then(&Enum.uniq(base ++ &1))
  end

  @doc """
  Replaces `{key}` placeholders in a topic template with values from
  the runtime context. Returns `nil` if any placeholder is missing or empty.
  """
  @spec interpolate_topic(String.t(), map()) :: String.t() | nil
  def interpolate_topic(template, context) when is_binary(template) and is_map(context) do
    placeholders = Regex.scan(~r/\{([a-zA-Z0-9_]+)\}/, template, capture: :all_but_first)

    Enum.reduce_while(placeholders, template, fn [key], acc ->
      value = Map.get(context, key) || Map.get(context, existing_atom(key))

      if is_binary(value) and byte_size(value) > 0 do
        {:cont, String.replace(acc, "{#{key}}", value)}
      else
        {:halt, nil}
      end
    end)
  end

  @doc """
  Resolves a UI descriptor for a given state from the spec definition.

  Looks up `state_def.ui` with optional `key` and `assigns` and merges
  in context assigns (`context` and `state`). Returns `nil` if no UI key
  is configured for the state.
  """
  @spec ui_for(map(), atom() | String.t(), map()) :: map() | nil
  def ui_for(spec, current, runtime) do
    states = Map.get(spec, :states) || %{}
    state_def = Map.get(states, current) || Map.get(states, to_string(current)) || %{}

    ui =
      Map.get(state_def, :ui) ||
        Map.get(state_def, "ui") ||
        %{}

    key =
      Map.get(ui, :key) || Map.get(ui, "key") || Map.get(state_def, :ui_key) ||
        Map.get(state_def, "ui_key")

    assigns =
      Map.get(ui, :assigns) ||
        Map.get(ui, "assigns") ||
        %{}

    context_assigns = %{context: Map.get(runtime, :context, %{}), state: current}

    if key do
      %{key: key, assigns: Map.merge(assigns, context_assigns)}
    else
      nil
    end
  end

  @doc """
  Safe conversion of a binary key to an existing atom. Returns `nil`
  if the atom doesn't exist in the atom table, rather than raising.
  """
  @spec existing_atom(String.t()) :: atom() | nil
  def existing_atom(key) when is_binary(key) do
    try do
      :erlang.binary_to_existing_atom(key, :utf8)
    rescue
      ArgumentError -> nil
    end
  end

  @doc """
  Builds the projection extensions map.

  When `spec.projection_enricher` names a module exporting `enrich/2`,
  it is called with the base extensions and the runtime, and its return
  replaces the extensions map. When absent, extensions contain just the
  runtime meta (if any).

  This is shared so all profiles (stepwise, FSM, flow) can populate
  extensions consistently.
  """
  require Logger

  @spec build_extensions(map(), map()) :: map()
  def build_extensions(spec, runtime) do
    base = %{meta: Map.get(runtime, :meta, %{})}

    case Map.get(spec, :projection_enricher) do
      nil ->
        base

      module when is_atom(module) ->
        if function_exported?(module, :enrich, 2) do
          try do
            module.enrich(base, runtime)
          rescue
            e ->
              Logger.warning(
                "Projection enricher #{inspect(module)}.enrich/2 raised: #{inspect(e)}"
              )

              base
          end
        else
          base
        end

      _ ->
        base
    end
  end
end
