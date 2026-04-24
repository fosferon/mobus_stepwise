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

  @doc """
  Initializes a new stepwise workflow runtime from a spec and runtime context.

  Normalizes the spec into internal representation (IR), starts the ALF pipeline
  (if not already running), builds the initial runtime map, fires entry actions
  for the initial state, and computes the first projection.

  If the initial state defines a capability action whose trigger matches `:enter`,
  that action runs during `init/2`. When that action returns `{:error, reason}`,
  the error is propagated to the caller as
  `{:error, {:initial_entry_action_failed, reason, runtime}}` with `blocked_reasons`
  populated on the returned runtime. Previously such errors were silently discarded,
  causing workflows whose first step errored to advance as if the step had succeeded.

  ## Parameters

    * `spec` — workflow specification map containing `:profile`, `:initial_state`,
      `:steps`, `:states`, and optionally `:transitions`, `:breakpoints`, `:subscriptions`
    * `runtime_context` — context map requiring `:tenant_id` and optionally
      `:execution_id`, `:sync`, and `:initial_context`

  ## Returns

    * `{:ok, runtime}` — runtime map with computed projection
    * `{:error, reason}` — if tenant_id is missing or pipeline fails to start
    * `{:error, {:initial_entry_action_failed, reason, runtime}}` — if the initial
      state's entry capability returned `{:error, reason}`

  ## Examples

      spec = %{profile: :stepwise, initial_state: :step_one, steps: [:step_one, :step_two],
               states: %{step_one: %{step_number: 1, ui: %{key: :step_one}},
                         step_two: %{step_number: 2, ui: %{key: :step_two}}}}

      {:ok, runtime} = Engine.init(spec, %{tenant_id: "t1", execution_id: "e1", sync: true})

  """
  @spec init(map(), map()) ::
          {:ok, runtime()}
          | {:error, {:initial_entry_action_failed, term(), runtime()} | term()}
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

      # Fire entry action for the initial state so capabilities with :enter trigger run.
      # Errors from the entry capability propagate to the caller rather than being swallowed.
      case run_initial_entry_action(runtime) do
        {:ok, runtime} ->
          {:ok, compute_projection(runtime)}

        {:error, reason, failed_runtime} ->
          {:error, {:initial_entry_action_failed, reason, compute_projection(failed_runtime)}}
      end
    end
  end

  @doc """
  Processes an event against the current runtime, advancing the workflow.

  Sends the event through the ALF pipeline which, in order:
  1. Merges the payload into `runtime.context`
  2. Executes any step action (capability) defined for the current state
  3. Advances or reverses `current_state` based on the event
  4. Fires entry actions if the state changed
  5. Records breakpoint hits
  6. Computes the updated projection

  ## Parameters

    * `runtime` — current runtime map (as returned by `init/2` or a prior `handle_event/3`)
    * `event` — event atom or string (`:next`, `:back`, `"next"`, `"back"`, or custom)
    * `payload` — map of user input to merge into context

  ## Returns

    * `{:ok, runtime}` — successful state transition with updated projection
    * `{:wait, runtime, wait_cfg}` — transition requires async resolution
    * `{:error, reason, runtime}` — action or pipeline failure

  ## Examples

      {:ok, runtime} = Engine.handle_event(runtime, :next, %{name: "Alice"})
      {:ok, runtime} = Engine.handle_event(runtime, :back, %{})

  """
  @spec handle_event(runtime(), atom() | String.t(), map()) ::
          {:ok, runtime()} | {:wait, runtime(), map()} | {:error, term(), runtime()}
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

  @doc """
  Returns the current `Mobus.Stepwise.Projection` for the runtime.

  If the runtime already contains a computed projection, returns it directly.
  Otherwise, recomputes the projection by running the pipeline in
  projection-only mode (no state transition).

  ## Parameters

    * `runtime` — current runtime map

  ## Returns

    * `%Mobus.Stepwise.Projection{}` — the canonical UI projection struct

  ## Examples

      projection = Engine.get_state(runtime)
      projection.current_state  #=> :step_two
      projection.available_events  #=> [:back, :next]

  """
  @spec get_state(runtime()) :: Projection.t() | map()
  @impl true
  def get_state(%{projection: %Projection{} = projection}), do: projection

  def get_state(runtime) when is_map(runtime) do
    runtime = compute_projection(runtime)

    case runtime do
      %{projection: %Projection{} = projection} -> projection
      _ -> runtime |> build_projection() |> Map.fetch!(:projection)
    end
  end

  @doc """
  Extracts a serializable checkpoint from the runtime for persistence.

  Strips the projection (non-serializable) and retains only the core runtime
  fields needed to resume the workflow later via `restore/3`.

  ## Parameters

    * `runtime` — current runtime map

  ## Returns

    * A plain map suitable for JSON serialization or database storage.

  ## Examples

      checkpoint = Engine.checkpoint(runtime)
      # => %{execution_id: "e1", current_state: :step_two, context: %{name: "Alice"}, ...}

  """
  @spec checkpoint(runtime()) :: map()
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

  @doc """
  Restores a runtime from a previously saved checkpoint.

  Re-normalizes the spec into IR, reconstitutes the runtime from checkpoint
  data, starts the pipeline if needed, and recomputes the projection. The
  restored runtime is fully functional for subsequent `handle_event/3` calls.

  ## Parameters

    * `spec` — the original workflow specification map
    * `checkpoint` — map previously returned by `checkpoint/1`
    * `runtime_context` — context map requiring `:tenant_id` and optionally `:sync`

  ## Returns

    * `{:ok, runtime}` — restored runtime with recomputed projection
    * `{:error, reason}` — if tenant_id is missing or pipeline fails to start

  ## Examples

      checkpoint = Engine.checkpoint(runtime)
      {:ok, restored} = Engine.restore(spec, checkpoint, %{tenant_id: "t1", sync: true})

  """
  @spec restore(map(), map(), map()) :: {:ok, runtime()} | {:error, term()}
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

    # `:blocked_reasons` is merged back in both branches so success-then-wait paths
    # (e.g. the capability sets a reason but succeeds) surface it to the caller, and
    # so the error branch carries the capability-populated reason.
    case StepwiseAction.run_entry_action(event) do
      %{status: :error, error: reason, runtime: updated_runtime} ->
        {:error, reason, merge_entry_runtime(runtime, updated_runtime)}

      %{runtime: updated_runtime} ->
        {:ok, merge_entry_runtime(runtime, updated_runtime)}

      _ ->
        {:ok, runtime}
    end
  end

  defp merge_entry_runtime(runtime, updated_runtime) do
    Map.merge(
      runtime,
      Map.take(updated_runtime, [:context, :artifacts, :trace, :blocked_reasons])
    )
  end
end
