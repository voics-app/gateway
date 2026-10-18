defmodule DiscordyGateway.Infrastructure.Valkey.InboxStore do
  @moduledoc """
  Ephemeral E2EE inbox (P7): one Valkey stream per device, `e2ee:inbox:<device_id>`, trimmed with
  `MAXLEN ~ 10000`, and a sorted set `e2ee:ttl` indexing entries by expiry. A per-device hash
  `e2ee:idx:<device_id>` maps message ids to stream entry ids so an ACK can delete exactly one
  entry. Nothing here is written to a database.
  """

  @behaviour DiscordyGateway.Application.Ports.InboxStore

  alias DiscordyGateway.Contracts
  alias DiscordyGateway.Infrastructure.Valkey.Pool

  @separator <<31>>
  @index_ttl_seconds 8 * 24 * 60 * 60

  @sweep_script """
  local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
  for _, member in ipairs(members) do
    local sep = string.find(member, string.char(31), 1, true)
    local device = string.sub(member, 1, sep - 1)
    local message = string.sub(member, sep + 1)
    local index = 'e2ee:idx:' .. device
    local entry = redis.call('HGET', index, message)
    if entry then
      redis.call('XDEL', 'e2ee:inbox:' .. device, entry)
      redis.call('HDEL', index, message)
    end
    redis.call('ZREM', KEYS[1], member)
  end
  return #members
  """

  @impl true
  def store(device_id, message_id, json, expires_at_ms) do
    stream = Contracts.e2ee_inbox_key(device_id)
    index = index_key(device_id)

    with {:ok, entry_id} <-
           Pool.command([
             "XADD",
             stream,
             "MAXLEN",
             "~",
             Contracts.e2ee_inbox_maxlen(),
             "*",
             "message_id",
             message_id,
             "json",
             json
           ]),
         {:ok, _results} <-
           Pool.pipeline([
             ["HSET", index, message_id, entry_id],
             ["EXPIRE", index, @index_ttl_seconds],
             ["ZADD", Contracts.e2ee_ttl_key(), expires_at_ms, member(device_id, message_id)]
           ]) do
      :ok
    end
  end

  @impl true
  def list(device_id) do
    with {:ok, entries} <- Pool.command(["XRANGE", Contracts.e2ee_inbox_key(device_id), "-", "+"]) do
      {:ok, Enum.flat_map(entries, &entry/1)}
    end
  end

  @impl true
  def ack(_device_id, []), do: :ok

  def ack(device_id, message_ids) do
    index = index_key(device_id)

    with {:ok, entry_ids} <- Pool.command(["HMGET", index | message_ids]),
         present = Enum.reject(entry_ids, &is_nil/1),
         {:ok, _results} <-
           Pool.pipeline(
             delete_entries(device_id, present) ++
               [
                 ["HDEL", index | message_ids],
                 [
                   "ZREM",
                   Contracts.e2ee_ttl_key() | Enum.map(message_ids, &member(device_id, &1))
                 ]
               ]
           ) do
      :ok
    end
  end

  @impl true
  def sweep_expired(now_ms, limit) do
    Pool.command(["EVAL", @sweep_script, 1, Contracts.e2ee_ttl_key(), now_ms, limit])
  end

  defp delete_entries(_device_id, []), do: []

  defp delete_entries(device_id, entry_ids),
    do: [["XDEL", Contracts.e2ee_inbox_key(device_id) | entry_ids]]

  defp entry([_entry_id, fields]) do
    map = fields |> Enum.chunk_every(2) |> Map.new(fn [key, value] -> {key, value} end)

    case map do
      %{"message_id" => message_id, "json" => json} -> [{message_id, json}]
      _ -> []
    end
  end

  defp index_key(device_id), do: "e2ee:idx:" <> device_id
  defp member(device_id, message_id), do: device_id <> @separator <> message_id
end
