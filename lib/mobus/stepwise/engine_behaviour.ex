defmodule Mobus.Stepwise.EngineBehaviour do
  @moduledoc """
  Behaviour contract for stepwise engines.

  Engines must be deterministic under `sync: true` execution, enabling
  reliable unit tests and debugging.
  """

  alias Mobus.Stepwise.Types
  alias Mobus.Stepwise.Projection

  @callback init(Types.spec(), Types.runtime_context()) ::
              {:ok, Types.runtime()} | {:error, Types.reason()}

  @callback handle_event(Types.runtime(), Types.event(), Types.payload()) ::
              {:ok, Types.runtime()}
              | {:wait, Types.runtime(), Types.wait_cfg()}
              | {:error, Types.reason(), Types.runtime()}

  @callback get_state(Types.runtime()) :: Projection.t() | map()

  @callback checkpoint(Types.runtime()) :: map()

  @callback restore(Types.spec(), map(), Types.runtime_context()) ::
              {:ok, Types.runtime()} | {:error, Types.reason()}
end
