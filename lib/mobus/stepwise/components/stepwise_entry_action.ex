defmodule Mobus.Stepwise.Components.StepwiseEntryAction do
  @moduledoc """
  Executes entry-triggered actions for stepwise workflows.

  Runs immediately after a state transition when the new state defines an
  action with an `:enter` trigger (or equivalent definition).
  """

  alias Mobus.Stepwise.Components.StepwiseAction

  @spec call(map(), map()) :: map()
  def call(%{status: :error} = event, _opts), do: event
  def call(%{skip_transition: true} = event, _opts), do: event

  def call(%{state_changed?: true} = event, _opts) do
    StepwiseAction.run_entry_action(event)
  end

  def call(event, _opts), do: event
end
