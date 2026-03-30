defmodule Mobus.Stepwise.UI.WizardConfig do
  @moduledoc """
  Ecto embedded schema for multi-step wizard UI configuration.

  Embeds wizard metadata including title, workflow reference, display options,
  and individual step definitions. Used for structuring complex multi-step processes
  in the UI layer.
  """

  use Ecto.Schema
  import Ecto.Changeset

  alias Mobus.Stepwise.UI.WizardStep

  @primary_key false
  embedded_schema do
    field(:title, :string, default: "Wizard")
    field(:workflow, :string)
    field(:state, :string, default: "initialised")
    field(:step_display, :string, default: "horizontal_detailed")
    field(:current_step, :integer, default: 1)
    embeds_many(:steps, WizardStep)
  end

  def changeset(wizard_config, attrs) do
    wizard_config
    |> cast(attrs, [:title, :state, :step_display, :current_step])
    |> validate_required([:workflow])
    |> cast_embed(:steps, with: &WizardStep.changeset/2, required: true)
  end
end
