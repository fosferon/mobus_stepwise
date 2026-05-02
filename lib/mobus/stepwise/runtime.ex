defmodule Mobus.Stepwise.Runtime do
  @moduledoc """
  Stable accessor helpers for stepwise runtime maps.

  Provides deterministic inspection and mutation of runtime state
  without depending on internal map structure. Orchestrators should
  prefer these helpers over direct map access.

  ## Meta

  The `:meta` field is an open map for orchestration metadata
  (session IDs, DAG coordinates, tags, etc.). It survives
  checkpoint/restore cycles and is passed read-only to capabilities.

      runtime = Runtime.put_meta(runtime, :session_id, "sess-123")
      Runtime.get_meta(runtime, :session_id)
      #=> "sess-123"
  """

  @doc "Returns the full `:meta` map from the runtime."
  @spec meta(map()) :: map()
  def meta(runtime), do: Map.get(runtime, :meta, %{})

  @doc "Gets a single key from `:meta`, with optional default."
  @spec get_meta(map(), term(), term()) :: term()
  def get_meta(runtime, key, default \\ nil) do
    runtime |> meta() |> Map.get(key, default)
  end

  @doc "Puts a single key into `:meta`."
  @spec put_meta(map(), term(), term()) :: map()
  def put_meta(runtime, key, value) do
    Map.update(runtime, :meta, %{key => value}, &Map.put(&1, key, value))
  end

  @doc "Shallow-merges a map into `:meta`."
  @spec merge_meta(map(), map()) :: map()
  def merge_meta(runtime, %{} = map) do
    Map.update(runtime, :meta, map, &Map.merge(&1, map))
  end

  @doc "Returns the `:context` map from the runtime."
  @spec context(map()) :: map()
  def context(runtime), do: Map.get(runtime, :context, %{})

  @doc "Puts a single key into `:context`."
  @spec overlay_context(map(), term(), term()) :: map()
  def overlay_context(runtime, key, value) do
    Map.update(runtime, :context, %{key => value}, &Map.put(&1, key, value))
  end

  @doc "Shallow-merges a map into `:context`."
  @spec merge_context(map(), map()) :: map()
  def merge_context(runtime, %{} = map) do
    Map.update(runtime, :context, map, &Map.merge(&1, map))
  end

  @doc "Returns the `:artifacts` map from the runtime."
  @spec artifacts(map()) :: map()
  def artifacts(runtime), do: Map.get(runtime, :artifacts, %{})

  @doc "Puts a single key into `:artifacts` (normalized via `Artifacts.merge/2`)."
  @spec overlay_artifacts(map(), term(), term()) :: map()
  def overlay_artifacts(runtime, key, value) do
    alias Mobus.Stepwise.Artifacts

    Map.update(runtime, :artifacts, %{}, fn arts ->
      Artifacts.merge(arts, %{key => value})
    end)
  end

  @doc "Returns the `:execution_id`."
  @spec execution_id(map()) :: String.t() | nil
  def execution_id(runtime), do: Map.get(runtime, :execution_id)

  @doc "Returns the `:tenant_id`."
  @spec tenant_id(map()) :: String.t() | nil
  def tenant_id(runtime), do: Map.get(runtime, :tenant_id)

  @doc "Returns the `:current_state`."
  @spec current_state(map()) :: atom() | String.t() | nil
  def current_state(runtime), do: Map.get(runtime, :current_state)
end
