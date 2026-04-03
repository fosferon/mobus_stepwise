defmodule Mobus.Stepwise.Capabilities do
  @moduledoc """
  Configurable capability runner adapter for the stepwise engine.

  When adapter is nil (default), capability execution is a no-op returning
  `{:ok, %{context: %{}}}`. This allows the package to work for form-only
  wizards (e.g. Atrapos tenant creation) without requiring a capability runner.

  Configure via `:mobus_stepwise` application config:

      config :mobus_stepwise, :capability_runner_adapter, MyApp.CapabilityRunner
  """

  @doc """
  Returns the configured capability runner adapter module, or `nil` if none is set.

  Reads from application config key `:mobus_stepwise, :capability_runner_adapter`.

  ## Examples

      Capabilities.adapter()
      #=> MyApp.CapabilityRunner

      # When unconfigured:
      Capabilities.adapter()
      #=> nil

  """
  @spec adapter() :: module() | nil
  def adapter do
    Application.get_env(:mobus_stepwise, :capability_runner_adapter)
  end

  @doc """
  Returns `true` if a capability runner adapter is configured and exports `execute/3`.

  Checks that the adapter is a non-nil atom module with an `execute/3` function exported.

  ## Examples

      Capabilities.enabled?()
      #=> true

  """
  @spec enabled?() :: boolean()
  def enabled? do
    is_atom(adapter()) and not is_nil(adapter()) and function_exported?(adapter(), :execute, 3)
  end

  @doc """
  Executes a capability via the configured adapter, or returns a no-op result.

  When no adapter is configured, returns `{:ok, %{context: %{}}}` (no-op).
  Otherwise delegates to `adapter.execute(tenant_id, capability_handle, input)`.

  ## Parameters

    * `tenant_id` — tenant identifier string
    * `capability_handle` — capability name (string or atom), e.g. `"myapp.validate"`
    * `input` — execution input map with event context, payload, and state

  ## Returns

    * `{:ok, result}` — successful execution with context updates and/or artifacts
    * `{:error, reason}` — capability execution failed

  ## Examples

      Capabilities.execute("tenant-1", "myapp.validate", %{payload: %{email: "a@b.com"}})
      #=> {:ok, %{context: %{valid: true}}}

  """
  @spec execute(String.t(), String.t() | atom(), map()) :: {:ok, term()} | {:error, term()}
  def execute(tenant_id, capability_handle, input) when is_binary(tenant_id) and is_map(input) do
    case adapter() do
      nil -> {:ok, %{context: %{}}}
      mod when is_atom(mod) -> mod.execute(tenant_id, capability_handle, input)
    end
  end
end
