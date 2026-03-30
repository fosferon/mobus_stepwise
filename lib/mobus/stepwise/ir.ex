defmodule Mobus.Stepwise.IR do
  @moduledoc """
  Normalization helpers for workflow specs compiled into IR.

  Normalizes spec shapes to reduce downstream conditionals in the
  stepwise engine and pipeline components.
  """

  @type t :: map()

  @spec normalize(map()) :: t()
  def normalize(%{} = spec) do
    spec
    |> normalize_profile()
    |> normalize_initial_state()
    |> normalize_states()
    |> normalize_transitions()
  end

  defp normalize_profile(spec) do
    profile = Map.get(spec, :profile) || Map.get(spec, "profile")

    profile =
      case profile do
        "stepwise" -> :stepwise
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
end
