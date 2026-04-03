defmodule Mobus.Stepwise.Pipeline.Stepwise do
  @moduledoc """
  Static ALF pipeline for stepwise workflows.

  This pipeline interprets the compiled spec (IR) in-band; we do not generate
  dynamic pipeline modules for disk-loaded workflows.
  """

  use ALF.DSL

  alias Mobus.Stepwise.Components.FsmBreakpoint
  alias Mobus.Stepwise.Components.StepwiseAction
  alias Mobus.Stepwise.Components.StepwiseAdvance
  alias Mobus.Stepwise.Components.StepwiseEntryAction
  alias Mobus.Stepwise.Components.StepwiseContextMerge
  alias Mobus.Stepwise.Components.StepwiseProjection

  @components [
    stage(StepwiseContextMerge),
    stage(StepwiseAction),
    stage(StepwiseAdvance),
    stage(StepwiseEntryAction),
    stage(FsmBreakpoint),
    stage(StepwiseProjection)
  ]

  @doc """
  Ensures the ALF pipeline process is running, starting it if necessary.

  Checks if the pipeline process is registered. If not, starts it with the
  given options. Supports `sync: true` for deterministic test execution.

  ## Parameters

    * `opts` — keyword list passed to `ALF.DSL.start/1`. Common options:
      * `:sync` — when `true`, pipeline runs synchronously (useful for tests)

  ## Returns

    * `:ok` — pipeline is running
    * `{:error, reason}` — pipeline failed to start

  ## Examples

      :ok = Stepwise.ensure_started(sync: true)

  """
  @spec ensure_started(keyword()) :: :ok | {:error, term()}
  def ensure_started(opts \\ []) do
    case Process.whereis(__MODULE__) do
      nil -> __MODULE__.start(opts)
      _pid -> :ok
    end
  end
end
