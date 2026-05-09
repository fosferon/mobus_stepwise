defmodule Mobus.Stepwise.Telemetry do
  @moduledoc """
  Telemetry integration for the stepwise engine.

  All engine lifecycle phases emit `:telemetry` span events
  (`:start`, `:stop`, `:exception` suffixes).

  ## Engine spans

    * `[:mobus_stepwise, :engine, :init]`
    * `[:mobus_stepwise, :engine, :handle_event]`
    * `[:mobus_stepwise, :engine, :restore]`

  ## Pipeline events

    * `[:mobus_stepwise, :pipeline, :action, :start | :stop | :exception]`

  ## Metadata

  All events include:

    * `:execution_id`
    * `:tenant_id`
    * `:current_state`
    * `:meta` — the full orchestration metadata map

  Engine `handle_event` spans also include:

    * `:event` — the event name
    * `:status` — outcome (`:ok`, `:wait`, `:error`)

  ## Example

      :telemetry.attach(
        "my-handler",
        [:mobus_stepwise, :engine, :handle_event, :stop],
        fn _event, measurements, metadata, _config ->
          Logger.info("Event processed in \#{measurements.duration}ns")
        end,
        nil
      )
  """

  @engine_events [
    [:mobus_stepwise, :engine, :init],
    [:mobus_stepwise, :engine, :handle_event],
    [:mobus_stepwise, :engine, :restore]
  ]

  @pipeline_events [
    [:mobus_stepwise, :pipeline, :action]
  ]

  @doc "Returns all telemetry event prefixes emitted by the engine."
  @spec events() :: [list(atom())]
  def events, do: @engine_events ++ @pipeline_events

  @doc false
  def span(event_prefix, metadata, fun) do
    :telemetry.span(event_prefix, metadata, fun)
  end

  @doc false
  def runtime_metadata(runtime) when is_map(runtime) do
    base = %{
      execution_id: Map.get(runtime, :execution_id),
      tenant_id: Map.get(runtime, :tenant_id),
      current_state: Map.get(runtime, :current_state),
      profile:
        Map.get(runtime, :profile) ||
          get_in(runtime, [:spec, :profile]),
      meta: Map.get(runtime, :meta, %{})
    }

    case Map.get(runtime, :telemetry_scope) do
      %{} = extra -> Map.merge(base, extra)
      _ -> base
    end
  end
end
