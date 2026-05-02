defmodule MobusStepwise.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/fosferon/mobus_stepwise"

  def project do
    [
      app: :mobus_stepwise,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      source_url: @source_url,
      description: "ALF-backed stepwise engine for multi-step wizards and workflows",
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:alf, "~> 0.12"},
      {:ecto, "~> 3.11"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 0.4 or ~> 1.0"}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "Mobus.Stepwise",
      source_url: @source_url
    ]
  end
end
