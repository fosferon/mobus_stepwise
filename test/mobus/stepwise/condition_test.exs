defmodule Mobus.Stepwise.ConditionTest do
  use ExUnit.Case, async: true

  alias Mobus.Stepwise.Condition

  @context %{
    "availability" => %{"found" => true, "rooms" => 0},
    "guest" => %{"email" => "Ada@Example.com", "tags" => ["vip", "returning"]},
    "check_in" => "2026-10-02",
    "items" => [%{"id" => 7}, %{"id" => 9}],
    atom_key: %{nested: "yes"}
  }

  defp eval!(condition) do
    assert {:ok, result} = Condition.evaluate(condition, @context)
    result
  end

  test "a bare string on the left is a path; the right is a literal" do
    assert eval!(%{"op" => "eq", "left" => "availability.found", "right" => true})
    refute eval!(%{"op" => "eq", "left" => "availability.rooms", "right" => 1})
    # the right-hand string is NOT looked up
    refute eval!(%{"op" => "eq", "left" => "check_in", "right" => "check_in"})
  end

  test "explicit path and value operands work on either side" do
    assert eval!(%{
             "op" => "eq",
             "left" => %{"value" => 9},
             "right" => %{"path" => "items.1.id"}
           })
  end

  test "paths reach atom keys that already exist and never mint atoms" do
    assert eval!(%{op: :eq, left: "atom_key.nested", right: "yes"})
    refute eval!(%{op: :exists, left: "no_such_key_#{System.unique_integer([:positive])}.x"})
  end

  test "ordering compares numbers, ISO dates and plain strings" do
    assert eval!(%{"op" => "lt", "left" => "availability.rooms", "right" => 1})
    assert eval!(%{"op" => "gte", "left" => "check_in", "right" => "2026-10-02"})
    assert eval!(%{"op" => "gt", "left" => "check_in", "right" => "2026-9-30"}) == false
    assert eval!(%{"op" => "gt", "left" => "check_in", "right" => "2026-09-30"})
    # mismatched kinds never order
    refute eval!(%{"op" => "gt", "left" => "check_in", "right" => 3})
  end

  test "membership and text operators" do
    assert eval!(%{
             "op" => "in",
             "left" => %{"value" => "vip"},
             "right" => %{"path" => "guest.tags"}
           })

    assert eval!(%{"op" => "contains", "left" => "guest.tags", "right" => "returning"})
    assert eval!(%{"op" => "icontains", "left" => "guest.email", "right" => "ada@"})
    refute eval!(%{"op" => "contains", "left" => "guest.email", "right" => "ada@"})
    assert eval!(%{"op" => "ends_with", "left" => "guest.email", "right" => ".com"})
  end

  test "unary operators need no right-hand side" do
    assert eval!(%{"op" => "exists", "left" => "guest.email"})
    assert eval!(%{"op" => "empty", "left" => "guest.missing"})
    assert eval!(%{"op" => "falsy", "left" => "availability.rooms"})
    assert eval!(%{"op" => "truthy", "left" => "availability.found"})
  end

  test "all / any / not combine" do
    found = %{"op" => "truthy", "left" => "availability.found"}
    rooms = %{"op" => "gt", "left" => "availability.rooms", "right" => 0}

    refute eval!(%{"all" => [found, rooms]})
    assert eval!(%{"any" => [found, rooms]})
    assert eval!(%{"not" => rooms})
  end

  test "booleans and functions are accepted for specs written in code" do
    assert eval!(true)
    refute eval!(false)
    assert eval!(fn ctx -> ctx["availability"]["found"] end)
  end

  test "malformed conditions are rejected, not guessed at" do
    assert {:error, {:invalid_condition, {:unknown_op, "regex"}}} =
             Condition.evaluate(%{"op" => "regex", "left" => "a", "right" => ".*"}, @context)

    assert {:error, {:invalid_condition, :missing_right}} =
             Condition.validate(%{"op" => "eq", "left" => "a"})

    assert {:error, {:invalid_condition, :missing_op}} = Condition.validate(%{"left" => "a"})
    assert {:error, {:invalid_condition, {:empty, :all}}} = Condition.validate(%{"all" => []})
    assert {:error, {:invalid_condition, :unsupported_shape}} = Condition.validate("yes")
  end
end
