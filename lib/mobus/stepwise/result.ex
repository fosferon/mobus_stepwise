defmodule Mobus.Stepwise.Result do
  @moduledoc """
  Normalized outcome helpers for engine return values.

  The engine returns one of three tuple shapes:

    * `{:ok, runtime}`
    * `{:wait, runtime, wait_cfg}`
    * `{:error, reason, runtime}`

  These helpers destructure any of those shapes uniformly,
  so orchestration code never hand-parses tuples.
  """

  @type engine_result ::
          {:ok, map()}
          | {:wait, map(), map()}
          | {:error, term(), map()}
          | {:error, term()}

  @doc "Returns the status atom: `:ok`, `:wait`, or `:error`."
  @spec status(engine_result()) :: :ok | :wait | :error
  def status({:ok, _}), do: :ok
  def status({:wait, _, _}), do: :wait
  def status({:error, _, _}), do: :error
  def status({:error, _}), do: :error

  @doc "Extracts the runtime from any engine result shape."
  @spec runtime(engine_result()) :: map() | nil
  def runtime({:ok, rt}), do: rt
  def runtime({:wait, rt, _}), do: rt
  def runtime({:error, _, rt}) when is_map(rt), do: rt
  def runtime({:error, _}), do: nil

  @doc "Returns `true` if the result is a wait."
  @spec wait?(engine_result()) :: boolean()
  def wait?({:wait, _, _}), do: true
  def wait?(_), do: false

  @doc "Returns the wait configuration, or `nil`."
  @spec wait_config(engine_result()) :: term() | nil
  def wait_config({:wait, _, cfg}), do: cfg
  def wait_config(_), do: nil

  @doc "Returns `true` if the result is an error."
  @spec error?(engine_result()) :: boolean()
  def error?({:error, _, _}), do: true
  def error?({:error, _}), do: true
  def error?(_), do: false

  @doc "Returns the error reason, or `nil`."
  @spec error_reason(engine_result()) :: term() | nil
  def error_reason({:error, reason, _}), do: reason
  def error_reason({:error, reason}), do: reason
  def error_reason(_), do: nil

  @doc "Returns `true` if the result is ok."
  @spec ok?(engine_result()) :: boolean()
  def ok?({:ok, _}), do: true
  def ok?(_), do: false
end
