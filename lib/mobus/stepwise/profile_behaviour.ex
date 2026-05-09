defmodule Mobus.Stepwise.ProfileBehaviour do
  @moduledoc """
  Internal behaviour for profile-specific runtime implementations.
  """

  alias Mobus.Stepwise.Projection
  alias Mobus.Stepwise.Types

  @callback init(Types.spec(), Types.runtime_context()) ::
              {:ok, Types.runtime()}
              | {:wait, Types.runtime(), Types.wait_cfg()}
              | {:error, Types.reason()}
              | {:error, {:initial_entry_action_failed, Types.reason(), Types.runtime()}}

  @callback handle_event(Types.runtime(), Types.event(), Types.payload()) ::
              {:ok, Types.runtime()}
              | {:wait, Types.runtime(), Types.wait_cfg()}
              | {:error, Types.reason(), Types.runtime()}

  @callback get_state(Types.runtime()) :: Projection.t() | map()

  @callback checkpoint(Types.runtime()) :: map()

  @callback restore(Types.spec(), map(), Types.runtime_context()) ::
              {:ok, Types.runtime()} | {:error, Types.reason()}
end
