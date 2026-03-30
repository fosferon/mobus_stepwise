defmodule Mobus.Stepwise.Types do
  @moduledoc """
  Canonical types for the stepwise engine.

  Defines the shared type vocabulary used across engine, pipeline,
  and component modules.
  """

  @type tenant_id :: String.t()
  @type execution_id :: String.t()
  @type profile :: :stepwise

  @type spec :: map()
  @type ir :: map()
  @type runtime_context :: map()
  @type runtime :: map()

  @type event :: atom() | String.t()
  @type payload :: map()

  @type wait_cfg :: map()
  @type reason :: term()
end
