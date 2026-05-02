defmodule Mobus.Stepwise.ErrorTest do
  use ExUnit.Case, async: true

  alias Mobus.Stepwise.Error

  describe "new/3" do
    test "creates with defaults" do
      err = Error.new(:validation, "bad input")
      assert err.type == :validation
      assert err.reason == "bad input"
      assert err.recoverable? == true
      assert err.retryable? == false
      assert err.source == nil
      assert err.detail == nil
    end

    test "accepts all options" do
      err =
        Error.new(:timeout, "connection lost",
          source: :capability,
          detail: %{attempt: 3},
          recoverable?: false,
          retryable?: true
        )

      assert err.source == :capability
      assert err.detail == %{attempt: 3}
      assert err.recoverable? == false
      assert err.retryable? == true
    end
  end

  describe "structured?/1" do
    test "true for Error struct" do
      assert Error.structured?(Error.new(:test, "x"))
    end

    test "false for other terms" do
      refute Error.structured?(:boom)
      refute Error.structured?("error")
      refute Error.structured?(%{type: :test})
    end
  end
end
