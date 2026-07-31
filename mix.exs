defmodule MobusStepwise.MixProject do
  use Mix.Project

  @version "0.3.1"
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
      description: "ALF-backed stepwise and flow workflow engine",
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
      {:jason, "~> 1.4"},
      {:telemetry, "~> 0.4 or ~> 1.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      maintainers: ["Leonidas"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md CHANGELOG.md ARCHITECTURE.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "Mobus.Stepwise.Engine",
      source_url: @source_url
    ]
  end
end
