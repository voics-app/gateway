defmodule DiscordyGateway.Infrastructure.Valkey.VoiceAllocationStore do
  @moduledoc "Voice allocations `voice:alloc:<server_id>` (P8) and the gateway-side drain set `voice:drained`."

  @behaviour DiscordyGateway.Application.Ports.VoiceAllocationStore

  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Infrastructure.Valkey.Pool

  @drained_key "voice:drained"
  @release_script "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"

  @impl true
  def get(server_id) do
    case Pool.command(["GET", Contracts.voice_alloc_key(server_id)]) do
      {:ok, node_id} when is_binary(node_id) -> {:ok, node_id}
      _none_or_error -> :none
    end
  end

  @impl true
  def claim(server_id, node_id) do
    key = Contracts.voice_alloc_key(server_id)

    with {:ok, _} <- Pool.command(["SET", key, node_id, "NX"]),
         {:ok, winner} when is_binary(winner) <- Pool.command(["GET", key]) do
      {:ok, winner}
    else
      {:ok, nil} -> {:error, :allocation_vanished}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def release(server_id, node_id) do
    Pool.command(["EVAL", @release_script, 1, Contracts.voice_alloc_key(server_id), node_id])
    :ok
  end

  @impl true
  def count, do: scan("0", 0)

  @impl true
  def drained do
    case Pool.command(["SMEMBERS", @drained_key]) do
      {:ok, members} -> MapSet.new(members)
      {:error, _reason} -> MapSet.new()
    end
  end

  @impl true
  def drain(node_id), do: ok(Pool.command(["SADD", @drained_key, node_id]))

  @impl true
  def undrain(node_id), do: ok(Pool.command(["SREM", @drained_key, node_id]))

  defp scan(cursor, total) do
    case Pool.command(["SCAN", cursor, "MATCH", Contracts.voice_alloc_key("*"), "COUNT", 500]) do
      {:ok, ["0", keys]} -> total + length(keys)
      {:ok, [next, keys]} -> scan(next, total + length(keys))
      {:error, _reason} -> total
    end
  end

  defp ok({:ok, _}), do: :ok
  defp ok({:error, reason}), do: {:error, reason}
end
