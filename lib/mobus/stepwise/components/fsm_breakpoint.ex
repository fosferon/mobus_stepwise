defmodule Mobus.Stepwise.Components.FsmBreakpoint do
  @moduledoc """
  Breakpoint component for stepwise workflows.

  Records breakpoint hits for debugging and inspection purposes.

  > **Naming note**: Despite the `FsmBreakpoint` module name, this component
  > operates exclusively within stepwise pipelines. The name is historical
  > (predating the profile extraction) and is retained to avoid breaking
  > internal pipeline references.
  """

  alias Mobus.Stepwise.SpecHelpers

  @doc """
  Records breakpoint hits for debugging and inspection.

  This is an ALF pipeline stage. Checks the spec's `:breakpoints` list against
  the current event and state. Matching breakpoints are appended to
  `runtime.breakpoint_hits` with a timestamp, and a trace entry is recorded.

  Supports breakpoint types:
  - `on: :event` — matches when the breakpoint value equals the event name
  - `on: :state_enter` — matches when the breakpoint value equals the current state

  Skipped when the event has an error status.

  ## Parameters

    * `event` — pipeline event map with `:spec`, `:runtime`, `:event`
    * `opts` — ALF stage options (unused)

  ## Returns

    * Updated event map with breakpoint hits and trace entries in runtime.

  """
  @spec call(map(), map()) :: map()
  def call(%{status: :error} = event, _opts), do: event

  def call(%{spec: spec, runtime: runtime, event: event_name} = event, _opts) do
    breakpoints = Map.get(spec, :breakpoints) || Map.get(spec, "breakpoints") || []
    current = Map.get(runtime, :current_state)

    hits =
      Enum.filter(breakpoints, fn bp ->
        on = Map.get(bp, :on) || Map.get(bp, "on")
        value = Map.get(bp, :value) || Map.get(bp, "value")

        case on do
          :event -> value == event_name or to_string(value) == to_string(event_name)
          "event" -> to_string(value) == to_string(event_name)
          :state_enter -> SpecHelpers.equivalent_state?(value, current)
          "state_enter" -> SpecHelpers.equivalent_state?(value, current)
          _ -> false
        end
      end)

    runtime =
      if hits == [] do
        runtime
      else
        runtime
        |> Map.update(:breakpoint_hits, [], fn existing ->
          existing ++ Enum.map(hits, &Map.put(&1, :hit_at, DateTime.utc_now()))
        end)
        |> Map.update(:trace, [], fn trace ->
          trace ++ [%{kind: :breakpoints, hits: length(hits), event: event_name, state: current}]
        end)
      end

    Map.put(event, :runtime, runtime)
  end

  def call(event, _opts), do: event
end
