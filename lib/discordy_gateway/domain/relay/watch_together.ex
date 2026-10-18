defmodule DiscordyGateway.Domain.Relay.WatchTogether do
  @moduledoc """
  Watch Together (op 24): commands are stamped with the server clock so every participant can
  compensate for latency, and the latest state per channel is kept for late joiners.
  """

  @spec stamp(map(), String.t(), integer()) :: map()
  def stamp(command, user_id, now_ms) do
    %{
      "channel_id" => command.channel_id,
      "action" => command.action,
      "position_ms" => command.position_ms,
      "at_server_ms" => now_ms,
      "user_id" => user_id
    }
    |> maybe_put("url", command.url)
  end

  @doc "Remembers the latest stamped command per channel, keeping the loaded url."
  @spec track(%{String.t() => map()}, map()) :: %{String.t() => map()}
  def track(state, %{"channel_id" => channel_id} = stamped) do
    url = stamped["url"] || get_in(state, [channel_id, "url"])
    Map.put(state, channel_id, maybe_put(stamped, "url", url))
  end

  @spec forget(%{String.t() => map()}, String.t()) :: %{String.t() => map()}
  def forget(state, channel_id), do: Map.delete(state, channel_id)

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
