defmodule Mobus.Stepwise.History do
  @moduledoc """
  Structured access to runtime history and trace data.

  The engine maintains two audit trails in the runtime:

    * `:history` — state transition log (`from`, `to`, `event`, `at`)
    * `:trace` — fine-grained execution trace (actions, steps, breakpoints)

  These helpers provide stable read access and controlled append
  without depending on internal list structure.
  """

  @doc "Returns the full history list from the runtime."
  @spec events(map()) :: [map()]
  def events(runtime), do: Map.get(runtime, :history, [])

  @doc "Returns the full trace list from the runtime."
  @spec trace(map()) :: [map()]
  def trace(runtime), do: Map.get(runtime, :trace, [])

  @doc "Appends a custom event to the history."
  @spec append(map(), map()) :: map()
  def append(runtime, %{} = entry) do
    entry = Map.put_new(entry, :at, DateTime.utc_now())
    Map.update(runtime, :history, [entry], &(&1 ++ [entry]))
  end

  @doc "Appends a custom entry to the trace."
  @spec append_trace(map(), map()) :: map()
  def append_trace(runtime, %{} = entry) do
    Map.update(runtime, :trace, [entry], &(&1 ++ [entry]))
  end

  @doc "Returns history entries filtered by event name."
  @spec events_by(map(), atom() | String.t()) :: [map()]
  def events_by(runtime, event_name) do
    runtime
    |> events()
    |> Enum.filter(fn entry ->
      Map.get(entry, :event) == event_name or
        to_string(Map.get(entry, :event)) == to_string(event_name)
    end)
  end

  @doc "Returns trace entries filtered by kind."
  @spec trace_by_kind(map(), atom() | String.t()) :: [map()]
  def trace_by_kind(runtime, kind) do
    runtime
    |> trace()
    |> Enum.filter(fn entry ->
      Map.get(entry, :kind) == kind or
        to_string(Map.get(entry, :kind)) == to_string(kind)
    end)
  end

  @doc "Returns the last history entry, or `nil`."
  @spec last_event(map()) :: map() | nil
  def last_event(runtime), do: runtime |> events() |> List.last()

  @doc "Returns the number of state transitions recorded."
  @spec transition_count(map()) :: non_neg_integer()
  def transition_count(runtime), do: runtime |> events() |> length()
end
