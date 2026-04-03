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

  @doc """
  Normalizes an artifact map into canonical format.

  Each artifact entry is coerced to include `"kind"`, `"version"`, `"inserted_at"`,
  and `"data"` keys with string keys throughout. Keys are stringified, and entries
  without explicit metadata default to kind `"opaque"` and version `1`.

  Returns an empty map for `nil` or non-map inputs.

  ## Parameters

    * `artifacts` — a map of artifact entries, or `nil`

  ## Returns

    * A normalized `artifact_map()` with string keys and canonical entry structure.

  ## Examples

      Artifacts.normalize(%{report: %{kind: "pdf", data: "..."}})
      #=> %{"report" => %{"kind" => "pdf", "version" => 1, "inserted_at" => "...", "data" => "..."}}

      Artifacts.normalize(nil)
      #=> %{}

  """
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

  @doc """
  Merges two artifact maps, with incoming entries overriding existing ones.

  Both maps are normalized before merging. `nil` inputs are treated as empty maps.

  ## Parameters

    * `existing` — the current artifact map (or `nil`)
    * `incoming` — new artifacts to merge in (or `nil`)

  ## Returns

    * A merged and normalized `artifact_map()`.

  ## Examples

      Artifacts.merge(%{"a" => %{"data" => 1}}, %{"b" => %{"data" => 2}})
      #=> %{"a" => %{...}, "b" => %{...}}

  """
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
