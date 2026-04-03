defmodule Mobus.Stepwise.Components.StepwiseContextMerge do
  @moduledoc """
  Merges inbound payload into `runtime.context` for stepwise workflows.

  Stepwise workflows are wizard-like; user input is typically collected over
  multiple events. This component ensures that payload updates persist in the
  in-band runtime context, enabling resume and later step actions.
  """

  @doc """
  Merges the event payload into `runtime.context`.

  This is an ALF pipeline stage. It shallow-merges `event.payload` into
  `runtime.context`, accumulating user input across successive events.
  Skipped when the event has an error status.

  ## Parameters

    * `event` — pipeline event map with `:runtime` and `:payload` keys
    * `opts` — ALF stage options (unused)

  ## Returns

    * Updated event map with merged context in `event.runtime.context`.

  """
  @spec call(map(), map()) :: map()
  def call(%{status: :error} = event, _opts), do: event

  def call(%{runtime: runtime, payload: payload} = event, _opts) when is_map(payload) do
    runtime =
      runtime
      |> Map.update(:context, %{}, fn ctx -> Map.merge(ctx, payload) end)

    %{event | runtime: runtime}
  end

  def call(event, _opts), do: event
end
