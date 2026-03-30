defmodule Mobus.Stepwise.Capabilities do
  @moduledoc """
  Configurable capability runner adapter for the stepwise engine.

  When adapter is nil (default), capability execution is a no-op returning
  `{:ok, %{context: %{}}}`. This allows the package to work for form-only
  wizards (e.g. Atrapos tenant creation) without requiring a capability runner.

  Configure via `:mobus_stepwise` application config:

      config :mobus_stepwise, :capability_runner_adapter, MyApp.CapabilityRunner
  """

  @spec adapter() :: module() | nil
  def adapter do
    Application.get_env(:mobus_stepwise, :capability_runner_adapter)
  end

  @spec enabled?() :: boolean()
  def enabled? do
    is_atom(adapter()) and not is_nil(adapter()) and function_exported?(adapter(), :execute, 3)
  end

  @spec execute(String.t(), String.t() | atom(), map()) :: {:ok, term()} | {:error, term()}
  def execute(tenant_id, capability_handle, input) when is_binary(tenant_id) and is_map(input) do
    case adapter() do
      nil -> {:ok, %{context: %{}}}
      mod when is_atom(mod) -> mod.execute(tenant_id, capability_handle, input)
    end
  end
end
