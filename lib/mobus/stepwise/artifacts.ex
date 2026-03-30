defmodule Mobus.Stepwise.Artifacts do
  @moduledoc """
  Canonical helpers for workflow-owned artifacts in the stepwise runtime.

  Artifacts are durable, workflow-scoped data that should survive refreshes,
  retries, and external callbacks (e.g. email confirmation links).
  """

  @type artifact_entry :: %{
          required(String.t()) => term()
        }

  @type artifact_map :: %{optional(String.t()) => artifact_entry()}

  @spec normalize(map() | nil) :: artifact_map()
  def normalize(nil), do: %{}

  def normalize(artifacts) when is_map(artifacts) do
    now = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    artifacts
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      key = to_string(key)

      if is_binary(key) do
        Map.put(acc, key, normalize_entry(value, now))
      else
        acc
      end
    end)
  end

  def normalize(_), do: %{}

  @spec merge(map() | nil, map() | nil) :: artifact_map()
  def merge(existing, incoming) do
    existing = normalize(existing)
    incoming = normalize(incoming)
    Map.merge(existing, incoming)
  end

  defp normalize_entry(%{} = value, now) do
    kind =
      Map.get(value, "kind") ||
        Map.get(value, :kind) ||
        "opaque"

    version =
      Map.get(value, "version") ||
        Map.get(value, :version) ||
        1

    inserted_at =
      Map.get(value, "inserted_at") ||
        Map.get(value, :inserted_at) ||
        now

    data =
      Map.get(value, "data") ||
        Map.get(value, :data) ||
        value

    entry = %{
      "kind" => to_string(kind),
      "version" => version,
      "inserted_at" => inserted_at,
      "data" => data
    }

    meta = Map.get(value, "meta") || Map.get(value, :meta)
    if is_nil(meta), do: entry, else: Map.put(entry, "meta", meta)
  end

  defp normalize_entry(value, now) do
    %{
      "kind" => "opaque",
      "version" => 1,
      "inserted_at" => now,
      "data" => value
    }
  end
end
