defmodule DiscordyGateway.Domain.Voice.Discovery do
  @moduledoc "Projects a consistent P8 discovery snapshot into registered nodes and allocation health."

  alias DiscordyGateway.Domain.Voice.VoiceNode

  @nodes "/discordy/voice/nodes/"
  @health "/discordy/voice/health/"
  @blacklist "/discordy/voice/blacklist/"

  @spec nodes(map(), boolean()) :: [VoiceNode.t()]
  def nodes(records, fresh?) do
    Enum.flat_map(records, fn {key, kv} ->
      with true <- String.starts_with?(key, @nodes),
           {:ok, raw} <- Jason.decode(kv.value),
           {:ok, node} <- VoiceNode.parse(raw),
           true <- key == @nodes <> node.region <> "/" <> node.node_id do
        [decorate(node, kv, records, fresh?)]
      else
        _ -> []
      end
    end)
  end

  defp decorate(node, kv, records, fresh?) do
    health = health_status(records[@health <> node.node_id], node.node_id, kv[:lease])

    %{
      node
      | health: if(fresh? or health == :unhealthy, do: health, else: :unavailable),
        blacklisted:
          Map.has_key?(records, @blacklist <> "nodes/" <> node.node_id) or
            Map.has_key?(records, @blacklist <> "regions/" <> node.region)
    }
  end

  defp health_status(%{value: value, lease: observer_lease}, node_id, registration_lease)
       when is_integer(observer_lease) and observer_lease > 0 and
              is_integer(registration_lease) and registration_lease > 0 do
    with {:ok, %{"node_id" => ^node_id, "registration_lease" => lease} = raw} <-
           Jason.decode(value),
         true <- lease == Integer.to_string(registration_lease),
         true <- valid_health?(raw) do
      case raw["status"] do
        "healthy" -> :healthy
        "unhealthy" -> :unhealthy
        "unknown" -> :unknown
      end
    else
      _ -> :unknown
    end
  end

  defp health_status(_record, _node_id, _lease), do: :unknown

  defp valid_health?(raw) do
    is_binary(raw["prober_id"]) and raw["prober_id"] != "" and
      raw["status"] in ~w(healthy unhealthy unknown) and
      Enum.all?(~w(consecutive_failures consecutive_passes checked_at_ms), fn field ->
        is_integer(raw[field]) and raw[field] >= 0
      end)
  end
end
