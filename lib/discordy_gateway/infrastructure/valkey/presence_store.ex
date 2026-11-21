defmodule DiscordyGateway.Infrastructure.Valkey.PresenceStore do
  @moduledoc """
  Presence in Valkey: independently expiring entries in `presence:sessions:<user_id>` and the
  last broadcast aggregate in `presence:agg:<user_id>`. One device cannot refresh another's lease.
  Expiry metadata is additive: older gateway replicas can still read the flat presence fields.
  """

  @behaviour DiscordyGateway.Application.Ports.PresenceStore

  alias DiscordyGateway.Domain.Presence
  alias DiscordyGateway.Infrastructure.Valkey.Pool

  @sessions_ttl_seconds 120
  @aggregate_ttl_seconds 86_400

  # Expiry, mutation and reading are atomic: concurrent device updates cannot revive expired entries.
  @sessions_script """
  local time = redis.call('TIME')
  local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
  local ttl = tonumber(ARGV[1])
  -- Preserve Jason's JSON verbatim: cjson turns empty activity arrays into objects.
  -- This server-owned trailing field is ignored by older gateway readers.
  local function stamp(encoded)
    local plain = string.gsub(encoded, ',"expires_at":%d+}%s*$', '}')
    local stamped = string.gsub(plain, '}%s*$', ',"expires_at":' .. (now + ttl) .. '}')
    return stamped
  end
  local entries = redis.call('HGETALL', KEYS[1])
  for i = 1, #entries, 2 do
    local value = cjson.decode(entries[i + 1])
    if value.expires_at and value.expires_at <= now then
      redis.call('HDEL', KEYS[1], entries[i])
    elseif not value.expires_at then
      redis.call('HSET', KEYS[1], entries[i], stamp(entries[i + 1]))
    end
  end
  if ARGV[2] == 'put' then
    redis.call('HSET', KEYS[1], ARGV[3], stamp(ARGV[4]))
  elseif ARGV[2] == 'remove' then
    redis.call('HDEL', KEYS[1], ARGV[3])
  elseif ARGV[2] == 'refresh' then
    local previous = redis.call('HGET', KEYS[1], ARGV[3])
    if previous then
      redis.call('HSET', KEYS[1], ARGV[3], stamp(previous))
    end
  end
  if ARGV[2] == 'put' or ARGV[2] == 'refresh' then
    redis.call('PEXPIRE', KEYS[1], ttl)
  end
  return redis.call('HVALS', KEYS[1])
  """

  @impl true
  def put(user_id, session_id, presence),
    do: sessions(user_id, ["put", session_id, Jason.encode!(presence)])

  @impl true
  def remove(user_id, session_id), do: sessions(user_id, ["remove", session_id])

  @impl true
  def swap_aggregate(user_id, aggregate) do
    encoded = Jason.encode!(aggregate)

    case Pool.command([
           "SET",
           aggregate_key(user_id),
           encoded,
           "EX",
           @aggregate_ttl_seconds,
           "GET"
         ]) do
      {:ok, previous} when is_binary(previous) ->
        if decode(previous) == decode(encoded), do: :unchanged, else: :changed

      _first_or_error ->
        :changed
    end
  end

  @impl true
  def get_aggregates([]), do: %{}

  def get_aggregates(user_ids) do
    case Pool.pipeline(Enum.map(user_ids, &session_command(&1, ["list"]))) do
      {:ok, results} ->
        for {user_id, values} <- Enum.zip(user_ids, results),
            is_list(values),
            into: %{},
            do: {user_id, Presence.aggregate(decode_sessions(values))}

      {:error, _reason} ->
        %{}
    end
  end

  @impl true
  def refresh(user_id, session_id), do: sessions(user_id, ["refresh", session_id])

  defp sessions(user_id, arguments) do
    with {:ok, values} <- Pool.command(session_command(user_id, arguments)) do
      {:ok, decode_sessions(values)}
    end
  end

  defp session_command(user_id, arguments),
    do:
      ["EVAL", @sessions_script, 1, sessions_key(user_id), @sessions_ttl_seconds * 1000] ++
        arguments

  defp decode_sessions(values),
    do: Enum.map(values, fn value -> session_presence(decode(value)) end)

  defp decode(json), do: Jason.decode!(json)

  defp session_presence(map) do
    %{
      status: map["status"],
      activities: map["activities"] || [],
      afk: map["afk"] == true,
      platform: map["platform"] || "desktop"
    }
  end

  defp sessions_key(user_id), do: "presence:sessions:" <> user_id
  defp aggregate_key(user_id), do: "presence:agg:" <> user_id
end
