defmodule Mobus.Stepwise.Components.StepwiseProjection do
  @moduledoc """
  Projection component for stepwise workflows.

  Produces canonical `Mobus.Stepwise.Projection` from the current runtime state.
  """

  alias Mobus.Stepwise.Projection
  alias Mobus.Stepwise.ProjectionHelpers
  alias Mobus.Stepwise.SpecHelpers

  @doc """
  Builds the canonical `Mobus.Stepwise.Projection` from the current runtime.

  This is an ALF pipeline stage. Computes the projection struct containing
  all information needed for UI rendering: current state, available events,
  UI descriptor, artifacts, blocked reasons, subscriptions, errors, and trace.

  Available events are derived from the step's position in the ordered step list:
  - First step: `[:next]`
  - Last step: `[:back]`
  - Middle steps: `[:back, :next]`

  ## Parameters

    * `event` — pipeline event map with `:spec` and `:runtime`
    * `opts` — ALF stage options (unused)

  ## Returns

    * Updated event map with `%Mobus.Stepwise.Projection{}` in both
      `event.runtime.projection` and `event.projection`.

  """
  @spec call(map(), map()) :: map()
  def call(%{spec: spec, runtime: runtime} = event, _opts) do
    current = Map.get(runtime, :current_state)
    ordered = SpecHelpers.ordered_steps(spec)

    projection = %Projection{
      execution_id: Map.fetch!(runtime, :execution_id),
      profile: :stepwise,
      current_state: current,
      available_events: available_events(ordered, current),
      blocked_reasons: Map.get(runtime, :blocked_reasons, %{}),
      breakpoint_hits: Map.get(runtime, :breakpoint_hits, []),
      subscriptions: ProjectionHelpers.subscriptions_for(spec, runtime),
      artifacts: Map.get(runtime, :artifacts, %{}),
      ui: ProjectionHelpers.ui_for(spec, current, runtime),
      errors: Map.get(runtime, :errors, []),
      trace: Map.get(runtime, :trace, []),
      extensions: ProjectionHelpers.build_extensions(spec, runtime)
    }

    runtime = Map.put(runtime, :projection, projection)
    event |> Map.put(:runtime, runtime) |> Map.put(:projection, projection)
  end

  defp available_events(ordered, current) when is_list(ordered) do
    idx = Enum.find_index(ordered, &SpecHelpers.equivalent_state?(&1, current))

    cond do
      is_nil(idx) ->
        []

      idx == 0 and idx + 1 < length(ordered) ->
        [:next]

      idx + 1 >= length(ordered) and idx - 1 >= 0 ->
        [:back]

      idx + 1 < length(ordered) and idx - 1 >= 0 ->
        [:back, :next]

      true ->
        []
    end
  end

  defp available_events(_ordered, _current), do: []
end
