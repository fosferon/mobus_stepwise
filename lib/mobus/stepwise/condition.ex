defmodule Mobus.Stepwise.Condition do
  @moduledoc """
  Data conditions for `:flow` edges.

  A condition is plain data, so a workflow stored as JSON can carry it:

      %{"op" => "eq", "left" => "availability.found", "right" => true}
      %{"all" => [cond, cond]}
      %{"any" => [cond, cond]}
      %{"not" => cond}

  `left` and `right` are operands. An operand is `%{"path" => "a.b.0.c"}`
  (looked up in the context), `%{"value" => term}` (taken as is), or a bare
  term: a bare string on the `left` is a path, anything else is a literal.

  Operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `contains`,
  `icontains`, `starts_with`, `ends_with`, `exists`, `empty`, `truthy`,
  `falsy`. There is deliberately no regular-expression operator: conditions
  may be authored by a tenant, and a pattern is a way to stall the engine.

  Keys and operator names may be atoms or strings. No atom is ever created
  from condition data.

  Specs written in code may also use a boolean or a one-arity function.
  """

  @ops ~w(eq neq gt gte lt lte in contains icontains starts_with ends_with exists empty truthy falsy)
  @unary ~w(exists empty truthy falsy)

  @type t :: boolean() | (map() -> as_boolean(term())) | map()

  @doc "Returns `:ok` when the condition is well formed."
  @spec validate(term()) :: :ok | {:error, term()}
  def validate(condition) when is_boolean(condition), do: :ok
  def validate(condition) when is_function(condition, 1), do: :ok

  def validate(%{} = condition) do
    cond do
      has?(condition, :all) -> validate_list(get(condition, :all), :all)
      has?(condition, :any) -> validate_list(get(condition, :any), :any)
      has?(condition, :not) -> validate(get(condition, :not))
      has?(condition, :op) -> validate_op(condition)
      true -> {:error, {:invalid_condition, :missing_op}}
    end
  end

  def validate(_condition), do: {:error, {:invalid_condition, :unsupported_shape}}

  @doc "Evaluates a condition against a context map."
  @spec evaluate(t(), map()) :: {:ok, boolean()} | {:error, term()}
  def evaluate(condition, context) when is_map(context) do
    with :ok <- validate(condition) do
      {:ok, eval(condition, context)}
    end
  end

  @doc """
  Looks a dotted path up in a context whose keys may be strings or atoms.
  Integer segments index into lists. Returns `nil` when any segment is missing.
  """
  @spec resolve_path(String.t() | [String.t()], term()) :: term()
  def resolve_path(path, context) when is_binary(path),
    do: resolve_path(String.split(path, ".", trim: true), context)

  def resolve_path([], value), do: value

  def resolve_path([segment | rest], %{} = map) do
    case fetch_key(map, segment) do
      {:ok, value} -> resolve_path(rest, value)
      :error -> nil
    end
  end

  def resolve_path([segment | rest], list) when is_list(list) do
    case Integer.parse(segment) do
      {index, ""} when index >= 0 -> resolve_path(rest, Enum.at(list, index))
      _ -> nil
    end
  end

  def resolve_path(_path, _value), do: nil

  defp eval(condition, _context) when is_boolean(condition), do: condition
  defp eval(condition, context) when is_function(condition, 1), do: truthy?(condition.(context))

  defp eval(%{} = condition, context) do
    cond do
      has?(condition, :all) -> Enum.all?(get(condition, :all), &eval(&1, context))
      has?(condition, :any) -> Enum.any?(get(condition, :any), &eval(&1, context))
      has?(condition, :not) -> not eval(get(condition, :not), context)
      true -> eval_op(condition, context)
    end
  end

  defp eval_op(condition, context) do
    op = op_name(get(condition, :op))
    left = operand(get(condition, :left), context, :path)
    right = operand(get(condition, :right), context, :literal)
    compare(op, left, right)
  end

  defp operand(%{} = operand, context, mode) do
    cond do
      has?(operand, :path) -> resolve_path(to_string(get(operand, :path)), context)
      has?(operand, :value) -> get(operand, :value)
      mode == :path -> nil
      true -> operand
    end
  end

  defp operand(value, context, :path) when is_binary(value), do: resolve_path(value, context)
  defp operand(value, _context, _mode), do: value

  defp compare("eq", left, right), do: comparable(left) == comparable(right)
  defp compare("neq", left, right), do: comparable(left) != comparable(right)
  defp compare("gt", left, right), do: ordered(left, right, &(&1 == :gt))
  defp compare("gte", left, right), do: ordered(left, right, &(&1 in [:gt, :eq]))
  defp compare("lt", left, right), do: ordered(left, right, &(&1 == :lt))
  defp compare("lte", left, right), do: ordered(left, right, &(&1 in [:lt, :eq]))

  defp compare("in", left, right) when is_list(right),
    do: comparable(left) in Enum.map(right, &comparable/1)

  defp compare("in", _left, _right), do: false
  defp compare("contains", left, right), do: contains?(left, right, false)
  defp compare("icontains", left, right), do: contains?(left, right, true)
  defp compare("starts_with", left, right), do: string_op(left, right, &String.starts_with?/2)
  defp compare("ends_with", left, right), do: string_op(left, right, &String.ends_with?/2)
  defp compare("exists", left, _right), do: not is_nil(left) and left != ""
  defp compare("empty", left, _right), do: left in [nil, "", [], %{}]
  defp compare("truthy", left, _right), do: truthy?(left)
  defp compare("falsy", left, _right), do: not truthy?(left)

  defp truthy?(value), do: value not in [nil, false, 0, "", "false", [], %{}]

  defp comparable(value) when is_boolean(value) or is_nil(value), do: value
  defp comparable(value) when is_atom(value), do: Atom.to_string(value)
  defp comparable(value), do: value

  defp ordered(left, right, accept) do
    case order(left, right) do
      nil -> false
      result -> accept.(result)
    end
  end

  defp order(left, right) when is_number(left) and is_number(right), do: cmp(left, right)

  defp order(left, right) when is_binary(left) and is_binary(right) do
    with {:error, _} <- date_order(left, right),
         {:error, _} <- datetime_order(left, right) do
      cmp(left, right)
    else
      {:ok, result} -> result
    end
  end

  defp order(%Date{} = left, %Date{} = right), do: Date.compare(left, right)
  defp order(%DateTime{} = left, %DateTime{} = right), do: DateTime.compare(left, right)
  defp order(_left, _right), do: nil

  defp date_order(left, right) do
    with {:ok, l} <- Date.from_iso8601(left),
         {:ok, r} <- Date.from_iso8601(right) do
      {:ok, Date.compare(l, r)}
    else
      _ -> {:error, :not_dates}
    end
  end

  defp datetime_order(left, right) do
    with {:ok, l, _} <- DateTime.from_iso8601(left),
         {:ok, r, _} <- DateTime.from_iso8601(right) do
      {:ok, DateTime.compare(l, r)}
    else
      _ -> {:error, :not_datetimes}
    end
  end

  defp cmp(left, right) when left > right, do: :gt
  defp cmp(left, right) when left < right, do: :lt
  defp cmp(_left, _right), do: :eq

  defp contains?(left, right, _insensitive?) when is_list(left),
    do: comparable(right) in Enum.map(left, &comparable/1)

  defp contains?(left, right, insensitive?) when is_binary(left) do
    right = text(right)

    if insensitive? do
      String.contains?(String.downcase(left), String.downcase(right))
    else
      String.contains?(left, right)
    end
  end

  defp contains?(_left, _right, _insensitive?), do: false

  defp string_op(left, right, fun) when is_binary(left), do: fun.(left, text(right))
  defp string_op(_left, _right, _fun), do: false

  defp text(value) when is_binary(value), do: value
  defp text(value) when is_atom(value) and not is_nil(value), do: Atom.to_string(value)
  defp text(value) when is_integer(value), do: Integer.to_string(value)
  defp text(value) when is_float(value), do: Float.to_string(value)
  defp text(_value), do: ""

  defp validate_list(list, _kind) when is_list(list) and list != [] do
    Enum.reduce_while(list, :ok, fn item, :ok ->
      case validate(item) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_list(_list, kind), do: {:error, {:invalid_condition, {:empty, kind}}}

  defp validate_op(condition) do
    op = op_name(get(condition, :op))

    cond do
      op not in @ops ->
        {:error, {:invalid_condition, {:unknown_op, get(condition, :op)}}}

      not has?(condition, :left) ->
        {:error, {:invalid_condition, :missing_left}}

      op not in @unary and not has?(condition, :right) ->
        {:error, {:invalid_condition, :missing_right}}

      true ->
        :ok
    end
  end

  defp op_name(op) when is_atom(op) and not is_nil(op), do: Atom.to_string(op)
  defp op_name(op) when is_binary(op), do: op
  defp op_name(_op), do: nil

  defp has?(map, key), do: Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))

  defp get(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp fetch_key(map, segment) do
    case Map.fetch(map, segment) do
      {:ok, value} ->
        {:ok, value}

      :error ->
        try do
          Map.fetch(map, String.to_existing_atom(segment))
        rescue
          ArgumentError -> :error
        end
    end
  end
end
