defmodule Mobus.Stepwise.UI.WizardStep do
  @moduledoc """
  Ecto embedded schema for an individual wizard step definition.

  Contains step metadata (title, description, state) and a `:component` field
  for step-type identification. Embedded within WizardConfig.

  Note: The original WizardStep embedded a FunctionCall schema.
  This has been replaced with a simple `:component` string field, since
  wizards use `projection.ui.key` for conditional rendering rather
  than FunctionCall dispatch.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:title, :string)
    field(:description, :string)
    field(:state, :string, default: "initialised")
    field(:component, :string)
  end

  def changeset(wizard_step, attrs) do
    wizard_step
    |> cast(attrs, [:title, :description, :state, :component])
    |> validate_required([:title, :state])
  end
end
