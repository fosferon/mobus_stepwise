defmodule Mobus.Stepwise.IR do
  @moduledoc """
  Normalization helpers for workflow specs compiled into IR.

  Normalizes spec shapes to reduce downstream conditionals in the
  stepwise engine and pipeline components.
  """

  @type t :: map()

  @doc """
  Normalizes a raw workflow spec into the internal representation (IR).

  Applies the following normalizations:
  - Coerces `"stepwise"` / `"flow"` string profiles to atoms
  - Extracts `:initial_state` from atom or string keys
  - Ensures `:states`, `:transitions`, and `:nodes` are maps (converts keyword lists)

  This reduces downstream conditionals in engine and pipeline components.

  ## Parameters

    * `spec` — a raw workflow specification map

  ## Returns

    * A normalized spec map (IR) with consistent atom keys.

  ## Examples

      IR.normalize(%{"profile" => "stepwise", "initial_state" => :step_one, "states" => %{}})
      #=> %{profile: :stepwise, initial_state: :step_one, states: %{}, transitions: %{}, ...}

  """
  @spec normalize(map()) :: t()
  def normalize(%{} = spec) do
    spec
    |> normalize_profile()
    |> normalize_initial_state()
    |> normalize_states()
    |> normalize_transitions()
    |> normalize_nodes()
    |> normalize_edges()
  end

  defp normalize_profile(spec) do
    profile = Map.get(spec, :profile) || Map.get(spec, "profile")

    profile =
      case profile do
        "stepwise" -> :stepwise
        "flow" -> :flow
        other -> other
      end

    Map.put(spec, :profile, profile)
  end

  defp normalize_initial_state(spec) do
    initial = Map.get(spec, :initial_state) || Map.get(spec, "initial_state")
    Map.put(spec, :initial_state, initial)
  end

  defp normalize_states(spec) do
    states = Map.get(spec, :states) || Map.get(spec, "states") || %{}
    Map.put(spec, :states, states)
  end

  defp normalize_transitions(spec) do
    transitions = Map.get(spec, :transitions) || Map.get(spec, "transitions") || %{}

    transitions =
      case transitions do
        %{} -> transitions
        list when is_list(list) -> Map.new(list, fn {k, v} -> {k, v} end)
        other -> other
      end

    Map.put(spec, :transitions, transitions)
  end

  defp normalize_nodes(spec) do
    nodes = Map.get(spec, :nodes) || Map.get(spec, "nodes") || %{}

    nodes =
      case nodes do
        %{} -> nodes
        list when is_list(list) -> Map.new(list, fn {k, v} -> {k, v} end)
        other -> other
      end

    Map.put(spec, :nodes, nodes)
  end

  defp normalize_edges(spec) do
    edges = Map.get(spec, :edges) || Map.get(spec, "edges") || []
    Map.put(spec, :edges, edges)
  end
end
