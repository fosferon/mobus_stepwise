defmodule Mobus.Stepwise.Components.StepwiseEntryAction do
  @moduledoc """
  Executes entry-triggered actions for stepwise workflows.

  Runs immediately after a state transition when the new state defines an
  action with an `:enter` trigger (or equivalent definition).
  """

  alias Mobus.Stepwise.Components.StepwiseAction

  @doc """
  Executes entry-triggered actions after a state transition.

  This is an ALF pipeline stage. When `state_changed?` is `true`, delegates
  to `StepwiseAction.run_entry_action/1` to execute any action on the new
  state that has an `:enter` trigger. Skipped on error status, when
  `skip_transition` is set, or when the state did not change.

  ## Parameters

    * `event` — pipeline event map with `:state_changed?`, `:status`, `:skip_transition`
    * `opts` — ALF stage options (unused)

  ## Returns

    * Updated event map, potentially with modified runtime from entry action execution.

  """
  @spec call(map(), map()) :: map()
  def call(%{status: :error} = event, _opts), do: event
  def call(%{skip_transition: true} = event, _opts), do: event

  def call(%{state_changed?: true} = event, _opts) do
    StepwiseAction.run_entry_action(event)
  end

  def call(event, _opts), do: event
end
