defmodule Mobus.Stepwise.Capabilities do
  @moduledoc """
  Configurable capability runner adapter for the stepwise engine.

  When adapter is nil (default), capability execution returns
  `{:ok, %{context: %{}}}` (no-op). This allows the package to work for
  form-only wizards without requiring a capability runner.

  Configure via `:mobus_stepwise` application config:

      config :mobus_stepwise, :capability_runner_adapter, MyApp.CapabilityRunner

  ### `:strict` mode

  Some consumers (e.g. workflow_stem with its own adapter plumbing) prefer
  strict semantics where a nil adapter signals a misconfiguration rather
  than silently succeeding. Enable via:

      config :mobus_stepwise, :capability_runner_strict, true

  When strict is `true`, calling `execute/3` with no adapter configured
  returns `{:error, :capability_runner_disabled}` instead of the default
  no-op success. Default: `false` (backwards compatible).
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
      nil ->
        if strict?() do
          {:error, :capability_runner_disabled}
        else
          {:ok, %{context: %{}}}
        end

      mod when is_atom(mod) ->
        mod.execute(tenant_id, capability_handle, input)
    end
  end

  @doc """
  Returns `true` when `:capability_runner_strict` mode is active.

  When true, `execute/3` returns `{:error, :capability_runner_disabled}` if
  no adapter is configured instead of silently succeeding. Consumers that
  always configure an adapter (e.g. workflow_stem) should set this to `true`
  so accidental nil-adapter calls are caught as errors.

  ## Examples

      Capabilities.strict?()
      #=> false

      # After config:
      #   config :mobus_stepwise, :capability_runner_strict, true
      Capabilities.strict?()
      #=> true

  """
  @spec strict?() :: boolean()
  def strict? do
    Application.get_env(:mobus_stepwise, :capability_runner_strict, false)
  end
end
