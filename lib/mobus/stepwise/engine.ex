defmodule Mobus.Stepwise.Engine do
  @moduledoc """
  Public entrypoint for workflow engine execution across supported profiles.

  `:stepwise` preserves the existing linear wizard/runtime semantics.
  `:flow` exposes graph execution with explicit parallel branches and joins.
  """

  @behaviour Mobus.Stepwise.EngineBehaviour

  alias Mobus.Stepwise.IR
  alias Mobus.Stepwise.Telemetry

  @profile_modules %{
    stepwise: Mobus.Stepwise.Profiles.Stepwise,
    flow: Mobus.Stepwise.Profiles.Flow
  }

  @impl true
  def init(spec, runtime_context) when is_map(spec) and is_map(runtime_context) do
    telemetry_meta = %{
      tenant_id: Map.get(runtime_context, :tenant_id) || Map.get(runtime_context, "tenant_id"),
      execution_id:
        Map.get(runtime_context, :execution_id) || Map.get(runtime_context, "execution_id"),
      profile: profile_from_spec(spec)
    }

    Telemetry.span([:mobus_stepwise, :engine, :init], telemetry_meta, fn ->
      result = profile_module_for_spec(spec).init(spec, runtime_context)
      {result, stop_metadata(telemetry_meta, result)}
    end)
  end

  @impl true
  def handle_event(runtime, event, payload)
      when is_map(runtime) and (is_atom(event) or is_binary(event)) and is_map(payload) do
    telemetry_meta = Telemetry.runtime_metadata(runtime) |> Map.put(:event, event)

    Telemetry.span([:mobus_stepwise, :engine, :handle_event], telemetry_meta, fn ->
      result = profile_module_for_runtime(runtime).handle_event(runtime, event, payload)
      {result, stop_metadata(telemetry_meta, result)}
    end)
  end

  @impl true
  def get_state(runtime) when is_map(runtime) do
    profile_module_for_runtime(runtime).get_state(runtime)
  end

  @impl true
  def checkpoint(runtime) when is_map(runtime) do
    profile_module_for_runtime(runtime).checkpoint(runtime)
  end

  @impl true
  def restore(spec, checkpoint, runtime_context)
      when is_map(spec) and is_map(checkpoint) and is_map(runtime_context) do
    telemetry_meta = %{
      tenant_id: Map.get(runtime_context, :tenant_id) || Map.get(runtime_context, "tenant_id"),
      execution_id:
        Map.get(checkpoint, :execution_id) || Map.get(checkpoint, "execution_id"),
      profile: profile_from_spec(spec)
    }

    Telemetry.span([:mobus_stepwise, :engine, :restore], telemetry_meta, fn ->
      result = profile_module_for_spec(spec).restore(spec, checkpoint, runtime_context)
      {result, stop_metadata(telemetry_meta, result)}
    end)
  end

  defp stop_metadata(base, result) do
    case result do
      {:ok, rt} ->
        Map.merge(base, Telemetry.runtime_metadata(rt)) |> Map.put(:status, :ok)

      {:wait, rt, _cfg} ->
        Map.merge(base, Telemetry.runtime_metadata(rt)) |> Map.put(:status, :wait)

      {:error, {:initial_entry_action_failed, reason, rt}} ->
        Map.merge(base, Telemetry.runtime_metadata(rt))
        |> Map.put(:status, :error)
        |> Map.put(:reason, reason)

      {:error, reason, rt} ->
        Map.merge(base, Telemetry.runtime_metadata(rt))
        |> Map.put(:status, :error)
        |> Map.put(:reason, reason)

      {:error, reason} ->
        Map.merge(base, %{status: :error, reason: reason})
    end
  end

  defp profile_module_for_runtime(runtime) do
    runtime
    |> profile_from_runtime()
    |> module_for_profile()
  end

  defp profile_module_for_spec(spec) do
    spec
    |> profile_from_spec()
    |> module_for_profile()
  end

  defp module_for_profile(profile) do
    Map.fetch!(@profile_modules, profile)
  end

  defp profile_from_runtime(runtime) do
    Map.get(runtime, :profile) ||
      get_in(runtime, [:spec, :profile]) ||
      :stepwise
  end

  defp profile_from_spec(spec) do
    spec
    |> IR.normalize()
    |> Map.get(:profile, :stepwise)
  end
end
