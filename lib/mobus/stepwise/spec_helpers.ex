defmodule Mobus.Stepwise.SpecHelpers do
  @moduledoc """
  Shared pure functions for inspecting stepwise workflow specs.

  Extracted from `StepwiseAdvance`, `StepwiseProjection`, and
  `FsmBreakpoint` to eliminate internal duplication. Used by pipeline
  components in the foundation; workflow_stem inherits these via
  defdelegate.

  All functions are pure (no side effects, no process dependency) so
  they compose safely in ALF pipeline stages.
  """

  @doc """
  Returns `true` when two state identifiers refer to the same state.

  Compares atoms directly, binaries directly, and cross-compares
  atom/binary via `Atom.to_string/1`.

  ## Examples

      iex> equivalent_state?(:greeting, :greeting)
      true

      iex> equivalent_state?(:greeting, "greeting")
      true

      iex> equivalent_state?(:greeting, :problem_solving)
      false

  """
  @spec equivalent_state?(term(), term()) :: boolean()
  def equivalent_state?(a, b) when is_binary(a) and is_binary(b), do: a == b
  def equivalent_state?(a, b) when is_atom(a) and is_atom(b), do: a == b
  def equivalent_state?(a, b) when is_atom(a) and is_binary(b), do: Atom.to_string(a) == b
  def equivalent_state?(a, b) when is_binary(a) and is_atom(b), do: a == Atom.to_string(b)
  def equivalent_state?(_a, _b), do: false

  @doc """
  Returns the ordered list of step identifiers for a workflow spec.

  Order is derived from:
  - `spec.steps` when present and non-empty (explicit order)
  - `spec.states` sorted by `step_number` (or `step`) ascending, otherwise
    insertion order

  Returns `[]` when the spec has no steps or states.

  ## Examples

      iex> ordered_steps(%{steps: [:a, :b, :c]})
      [:a, :b, :c]

  """
  @spec ordered_steps(map()) :: [atom() | String.t()]
  def ordered_steps(spec) do
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
end
