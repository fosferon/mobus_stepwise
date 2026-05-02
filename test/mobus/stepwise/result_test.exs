defmodule Mobus.Stepwise.ResultTest do
  use ExUnit.Case, async: true

  alias Mobus.Stepwise.Result

  @ok_result {:ok, %{current_state: :step_two}}
  @wait_result {:wait, %{current_state: :step_one}, %{type: :email, reason: "confirm"}}
  @error_result {:error, :boom, %{current_state: :step_one}}
  @bare_error {:error, :missing_tenant_id}

  describe "status/1" do
    test "ok" do
      assert Result.status(@ok_result) == :ok
    end

    test "wait" do
      assert Result.status(@wait_result) == :wait
    end

    test "error with runtime" do
      assert Result.status(@error_result) == :error
    end

    test "bare error" do
      assert Result.status(@bare_error) == :error
    end
  end

  describe "runtime/1" do
    test "extracts from ok" do
      assert Result.runtime(@ok_result) == %{current_state: :step_two}
    end

    test "extracts from wait" do
      assert Result.runtime(@wait_result) == %{current_state: :step_one}
    end

    test "extracts from error" do
      assert Result.runtime(@error_result) == %{current_state: :step_one}
    end

    test "returns nil for bare error" do
      assert Result.runtime(@bare_error) == nil
    end
  end

  describe "predicates" do
    test "ok?/1" do
      assert Result.ok?(@ok_result)
      refute Result.ok?(@wait_result)
      refute Result.ok?(@error_result)
    end

    test "wait?/1" do
      assert Result.wait?(@wait_result)
      refute Result.wait?(@ok_result)
      refute Result.wait?(@error_result)
    end

    test "error?/1" do
      assert Result.error?(@error_result)
      assert Result.error?(@bare_error)
      refute Result.error?(@ok_result)
    end
  end

  describe "accessors" do
    test "wait_config/1" do
      assert Result.wait_config(@wait_result) == %{type: :email, reason: "confirm"}
      assert Result.wait_config(@ok_result) == nil
    end

    test "error_reason/1" do
      assert Result.error_reason(@error_result) == :boom
      assert Result.error_reason(@bare_error) == :missing_tenant_id
      assert Result.error_reason(@ok_result) == nil
    end
  end
end
