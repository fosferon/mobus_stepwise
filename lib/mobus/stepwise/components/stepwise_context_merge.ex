defmodule Mobus.Stepwise.Components.StepwiseContextMerge do
  @moduledoc """
  Merges inbound payload into `runtime.context` for stepwise workflows.

  Stepwise workflows are wizard-like; user input is typically collected over
  multiple events. This component ensures that payload updates persist in the
  in-band runtime context, enabling resume and later step actions.
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
