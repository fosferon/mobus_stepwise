defmodule Mobus.Stepwise.Projection do
  @moduledoc """
  Canonical UI projection contract returned by the stepwise engine.

  The projection provides all information needed for a UI layer to render
  a wizard step and submit events without understanding workflow logic.
  """

  alias Mobus.Stepwise.Types

  @enforce_keys [:execution_id, :profile]
  defstruct [
    :execution_id,
    :profile,
    :current_state,
    available_events: [],
    blocked_reasons: %{},
    breakpoint_hits: [],
    subscriptions: [],
    artifacts: %{},
    ui: nil,
    errors: [],
    trace: []
  ]

  @type ui_descriptor :: %{
          required(:key) => atom() | String.t(),
          optional(:assigns) => map()
        }

  @type t :: %__MODULE__{
          execution_id: Types.execution_id(),
          profile: Types.profile(),
          current_state: atom() | String.t() | nil,
          available_events: [atom() | String.t()],
          blocked_reasons: map(),
          breakpoint_hits: list(),
          subscriptions: [String.t()],
          artifacts: map(),
          ui: ui_descriptor() | nil,
          errors: [map()],
          trace: list()
        }
end
