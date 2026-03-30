defmodule Mobus.Stepwise.Engine do
  @moduledoc """
  ALF-backed engine for stepwise workflows.

  Stepwise workflows are intended for wizards and import pipelines:
  - linear(ish) progression
  - resumable via checkpoints
  - lighter semantics than full FSM (events are typically `:next` / `:back`)

  ## Usage

      spec = %{
        profile: :stepwise,
        initial_state: :step_one,
        steps: [:step_one, :step_two, :step_three],
        states: %{
          step_one: %{step_number: 1, ui: %{key: :step_one}},
          step_two: %{step_number: 2, ui: %{key: :step_two}},
          step_three: %{step_number: 3, ui: %{key: :step_three}}
        }
      }

      {:ok, runtime} = Engine.init(spec, %{tenant_id: "t1", execution_id: "e1", sync: true})
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{name: "Alice"})
      projection = Engine.get_state(runtime)
  """

  @behaviour Mobus.Stepwise.EngineBehaviour

  alias Mobus.Stepwise.IR
  alias Mobus.Stepwise.Pipeline.Stepwise, as: StepwisePipeline
  alias Mobus.Stepwise.Components.StepwiseProjection
  alias Mobus.Stepwise.Projection
  require Logger

  @type runtime :: %{
          required(:execution_id) => String.t(),
          required(:tenant_id) => String.t(),
          required(:spec) => map(),
          required(:current_state) => atom() | String.t(),
          optional(:context) => map(),
          optional(:history) => list(),
          optional(:trace) => list(),
          optional(:blocked_reasons) => map(),
          optional(:breakpoint_hits) => list(),
          optional(:errors) => [map()],
          optional(:projection) => Projection.t()
        }

  @impl true
  def init(spec, runtime_context) when is_map(spec) and is_map(runtime_context) do
    with {:ok, tenant_id} <- fetch_tenant_id(runtime_context),
         {:ok, execution_id} <- fetch_execution_id(runtime_context),
         :ok <- ensure_pipeline(runtime_context),
         ir <- IR.normalize(spec),
         {:ok, initial_state} <- fetch_initial_state(ir) do
      runtime = %{
        execution_id: execution_id,
        tenant_id: tenant_id,
        spec: ir,
        current_state: initial_state,
        context: Map.get(runtime_context, :initial_context, %{}) || %{},
        artifacts: %{},
        history: [],
        trace: [],
        blocked_reasons: %{},
        breakpoint_hits: []
      }

      # Fire entry action for the initial state so capabilities with :enter trigger run
      runtime = run_initial_entry_action(runtime)

      {:ok, runtime |> compute_projection()}
    end
  end

  @impl true
  def handle_event(runtime, event, payload)
      when is_map(runtime) and (is_atom(event) or is_binary(event)) and is_map(payload) do
    :ok = ensure_pipeline(%{})

    input = %{
      spec: runtime.spec,
      runtime: Map.delete(runtime, :projection),
      event: event,
      payload: payload,
      status: :ok
    }

    case call_pipeline(input) do
      {:ok, %{status: :error, error: reason, runtime: updated}} ->
        {:error, reason, compute_projection(updated)}

      {:ok, %{runtime: updated, wait: wait_cfg}} ->
        {:wait, compute_projection(updated), wait_cfg}

      {:ok, %{runtime: updated}} ->
        {:ok, compute_projection(updated)}

      {:error, reason} ->
        {:error, reason, compute_projection(runtime)}
    end
  end

  @impl true
  def get_state(%{projection: %Projection{} = projection}), do: projection

  def get_state(runtime) when is_map(runtime) do
    runtime = compute_projection(runtime)

    case runtime do
      %{projection: %Projection{} = projection} -> projection
      _ -> runtime |> build_projection() |> Map.fetch!(:projection)
    end
  end

  @impl true
  def checkpoint(runtime) when is_map(runtime) do
    runtime
    |> Map.drop([:projection])
    |> Map.take([
      :execution_id,
      :tenant_id,
      :spec,
      :current_state,
      :context,
      :artifacts,
      :history,
      :trace,
      :blocked_reasons,
      :breakpoint_hits
    ])
  end

  @impl true
  def restore(spec, checkpoint, runtime_context)
      when is_map(spec) and is_map(checkpoint) and is_map(runtime_context) do
    with {:ok, tenant_id} <- fetch_tenant_id(runtime_context),
         :ok <- ensure_pipeline(runtime_context) do
      ir = IR.normalize(spec)

      execution_id =
        Map.get(checkpoint, :execution_id) ||
          Map.get(checkpoint, "execution_id") ||
          Map.get(runtime_context, :execution_id) ||
          Map.get(runtime_context, "execution_id") ||
          "stem-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))

      runtime = %{
        execution_id: execution_id,
        tenant_id: tenant_id,
        spec: ir,
        current_state:
          Map.get(checkpoint, :current_state) || Map.get(checkpoint, "current_state"),
        context: Map.get(checkpoint, :context) || Map.get(checkpoint, "context") || %{},
        artifacts: Map.get(checkpoint, :artifacts) || Map.get(checkpoint, "artifacts") || %{},
        history: Map.get(checkpoint, :history) || Map.get(checkpoint, "history") || [],
        trace: Map.get(checkpoint, :trace) || Map.get(checkpoint, "trace") || [],
        blocked_reasons:
          Map.get(checkpoint, :blocked_reasons) || Map.get(checkpoint, "blocked_reasons") || %{},
        breakpoint_hits:
          Map.get(checkpoint, :breakpoint_hits) || Map.get(checkpoint, "breakpoint_hits") || []
      }

      {:ok, compute_projection(runtime)}
    end
  end

  defp ensure_pipeline(runtime_context) do
    opts =
      case Map.get(runtime_context, :sync) do
        true -> [sync: true]
        _ -> []
      end

    case StepwisePipeline.ensure_started(opts) do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  defp call_pipeline(input) do
    case StepwisePipeline.call(input) do
      %ALF.IP{event: out} -> {:ok, out}
      %ALF.ErrorIP{error: error} -> {:error, error}
      %{} = out -> {:ok, out}
      other -> {:error, {:unexpected_pipeline_result, other}}
    end
  end

  defp compute_projection(runtime) do
    input = %{
      spec: runtime.spec,
      runtime: Map.delete(runtime, :projection),
      event: "__projection__",
      payload: %{},
      status: :ok,
      skip_transition: true
    }

    case call_pipeline(input) do
      {:ok, %{runtime: updated}} ->
        if match?(%Projection{}, Map.get(updated, :projection)) do
          updated
        else
          error = projection_error(:missing_projection, updated, input.event)
          log_projection_error(error)
          build_projection(updated, [error])
        end

      {:error, reason} ->
        error = projection_error(reason, runtime, input.event)
        log_projection_error(error)
        build_projection(runtime, [error])
    end
  end

  defp build_projection(runtime, errors \\ []) do
    runtime = append_errors(runtime, errors)
    event = %{spec: runtime.spec, runtime: Map.delete(runtime, :projection)}

    case StepwiseProjection.call(event, %{}) do
      %{runtime: updated} -> updated
      _ -> runtime
    end
  end

  defp append_errors(runtime, errors) when is_list(errors) and errors != [] do
    Map.update(runtime, :errors, errors, fn existing -> existing ++ errors end)
  end

  defp append_errors(runtime, _errors), do: runtime

  defp projection_error(reason, runtime, event) do
    %{
      type: :pipeline_error,
      reason: reason,
      engine: __MODULE__,
      event: event,
      execution_id: Map.get(runtime, :execution_id),
      timestamp: DateTime.utc_now()
    }
  end

  defp log_projection_error(error) do
    Logger.warning("Stepwise projection pipeline failed: #{inspect(error)}")
  end

  defp fetch_tenant_id(runtime_context) do
    case Map.get(runtime_context, :tenant_id) || Map.get(runtime_context, "tenant_id") do
      nil -> {:error, :missing_tenant_id}
      tid -> {:ok, tid}
    end
  end

  defp fetch_execution_id(runtime_context) do
    case Map.get(runtime_context, :execution_id) || Map.get(runtime_context, "execution_id") do
      nil -> {:ok, "stem-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))}
      id -> {:ok, id}
    end
  end

  defp fetch_initial_state(spec) do
    case Map.get(spec, :initial_state) do
      nil -> {:error, :missing_initial_state}
      state -> {:ok, state}
    end
  end

  alias Mobus.Stepwise.Components.StepwiseAction

  defp run_initial_entry_action(runtime) do
    event = %{
      spec: runtime.spec,
      runtime: Map.delete(runtime, :projection),
      event: :__enter__,
      payload: %{},
      status: :ok,
      state_changed?: true
    }

    case StepwiseAction.run_entry_action(event) do
      %{runtime: updated_runtime} ->
        Map.merge(runtime, Map.take(updated_runtime, [:context, :artifacts, :trace]))

      _ ->
        runtime
    end
  end
end
