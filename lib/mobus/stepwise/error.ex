defmodule Mobus.Stepwise.Error do
  @moduledoc """
  Structured error metadata for capability and engine errors.

  Provides richer error classification than raw reason terms,
  enabling orchestrators to make policy decisions (retry, skip,
  escalate) without string-parsing error reasons.

  ## Usage

  Capabilities can return structured errors:

      {:error, %Mobus.Stepwise.Error{
        type: :validation,
        source: :capability,
        reason: "email already exists",
        recoverable?: true,
        retryable?: false
      }}

  The engine passes these through transparently. Orchestrators
  can pattern-match on the struct:

      case Result.error_reason(result) do
        %Error{retryable?: true} -> schedule_retry(...)
        %Error{recoverable?: false} -> escalate(...)
        raw_reason -> handle_legacy(raw_reason)
      end
  """

  @enforce_keys [:type, :reason]
  defstruct [
    :type,
    :reason,
    :source,
    :detail,
    recoverable?: true,
    retryable?: false
  ]

  @type t :: %__MODULE__{
          type: atom(),
          reason: term(),
          source: atom() | nil,
          detail: term() | nil,
          recoverable?: boolean(),
          retryable?: boolean()
        }

  @doc "Creates a new structured error."
  @spec new(atom(), term(), keyword()) :: t()
  def new(type, reason, opts \\ []) do
    %__MODULE__{
      type: type,
      reason: reason,
      source: Keyword.get(opts, :source),
      detail: Keyword.get(opts, :detail),
      recoverable?: Keyword.get(opts, :recoverable?, true),
      retryable?: Keyword.get(opts, :retryable?, false)
    }
  end

  @doc "Returns `true` if the value is a `%Mobus.Stepwise.Error{}` struct."
  @spec structured?(term()) :: boolean()
  def structured?(%__MODULE__{}), do: true
  def structured?(_), do: false
end
