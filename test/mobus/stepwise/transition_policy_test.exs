defmodule Mobus.Stepwise.TransitionPolicyTest do
  use ExUnit.Case, async: false

  alias Mobus.Stepwise.Engine

  @runtime_context %{tenant_id: "t", execution_id: "e-policy", sync: true}

  describe "transition policy :deny" do
    test "blocks advancement when policy denies" do
      spec = %{
        profile: :stepwise,
        initial_state: :step_one,
        steps: [:step_one, :step_two, :step_three],
        states: %{
          step_one: %{step_number: 1, ui: %{key: :step_one}},
          step_two: %{step_number: 2, ui: %{key: :step_two}},
          step_three: %{step_number: 3, ui: %{key: :step_three}}
        },
        transition_policy: __MODULE__.DenyAtTwo
      }

      {:ok, runtime} = Engine.init(spec, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert runtime.current_state == :step_two

      # Policy denies moving from step_two to step_three
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert runtime.current_state == :step_two
      assert Map.has_key?(runtime.blocked_reasons, :next)
    end
  end

  describe "transition policy :redirect" do
    test "redirects to a different state" do
      spec = %{
        profile: :stepwise,
        initial_state: :step_one,
        steps: [:step_one, :step_two, :step_three],
        states: %{
          step_one: %{step_number: 1, ui: %{key: :step_one}},
          step_two: %{step_number: 2, ui: %{key: :step_two}},
          step_three: %{step_number: 3, ui: %{key: :step_three}}
        },
        transition_policy: __MODULE__.RedirectToThree
      }

      {:ok, runtime} = Engine.init(spec, @runtime_context)

      # Policy redirects step_one -> step_two into step_one -> step_three
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert runtime.current_state == :step_three
    end

    test "invalid redirect target is denied instead of committing impossible state" do
      spec = %{
        profile: :stepwise,
        initial_state: :step_one,
        steps: [:step_one, :step_two],
        states: %{
          step_one: %{step_number: 1, ui: %{key: :step_one}},
          step_two: %{step_number: 2, ui: %{key: :step_two}}
        },
        transition_policy: __MODULE__.RedirectToMissing
      }

      {:ok, runtime} = Engine.init(spec, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})

      assert runtime.current_state == :step_one
      assert runtime.blocked_reasons[:next] == {:invalid_redirect_target, :missing_step}
    end

    test "string-keyed transition_policy is honored" do
      spec = %{
        "profile" => "stepwise",
        "initial_state" => :step_one,
        "steps" => [:step_one, :step_two, :step_three],
        "states" => %{
          step_one: %{step_number: 1, ui: %{key: :step_one}},
          step_two: %{step_number: 2, ui: %{key: :step_two}},
          step_three: %{step_number: 3, ui: %{key: :step_three}}
        },
        "transition_policy" => __MODULE__.RedirectToThree
      }

      {:ok, runtime} = Engine.init(spec, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert runtime.current_state == :step_three
    end
  end

  describe "no policy configured" do
    test "normal advancement when no policy" do
      spec = %{
        profile: :stepwise,
        initial_state: :step_one,
        steps: [:step_one, :step_two],
        states: %{
          step_one: %{step_number: 1, ui: %{key: :step_one}},
          step_two: %{step_number: 2, ui: %{key: :step_two}}
        }
      }

      {:ok, runtime} = Engine.init(spec, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert runtime.current_state == :step_two
    end
  end

  describe "crashing policy" do
    test "fail-open when policy raises" do
      spec = %{
        profile: :stepwise,
        initial_state: :step_one,
        steps: [:step_one, :step_two],
        states: %{
          step_one: %{step_number: 1, ui: %{key: :step_one}},
          step_two: %{step_number: 2, ui: %{key: :step_two}}
        },
        transition_policy: __MODULE__.CrashingPolicy
      }

      {:ok, runtime} = Engine.init(spec, @runtime_context)
      {:ok, runtime} = Engine.handle_event(runtime, :next, %{})
      assert runtime.current_state == :step_two
    end
  end

  # Policy that denies transitions FROM step_two
  defmodule DenyAtTwo do
    def allow_transition?(_spec, _runtime, :step_two, _to, _event),
      do: {:deny, "step_two is locked"}

    def allow_transition?(_spec, _runtime, _from, _to, _event), do: :allow
  end

  # Policy that redirects step_one -> step_two into step_one -> step_three
  defmodule RedirectToThree do
    def allow_transition?(_spec, _runtime, :step_one, :step_two, _event),
      do: {:redirect, :step_three}

    def allow_transition?(_spec, _runtime, _from, _to, _event), do: :allow
  end

  defmodule RedirectToMissing do
    def allow_transition?(_spec, _runtime, :step_one, :step_two, _event),
      do: {:redirect, :missing_step}

    def allow_transition?(_spec, _runtime, _from, _to, _event), do: :allow
  end

  # Policy that crashes
  defmodule CrashingPolicy do
    def allow_transition?(_spec, _runtime, _from, _to, _event),
      do: raise("boom")
  end
end
