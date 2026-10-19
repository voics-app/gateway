defmodule DiscordyGateway.Infrastructure.Valkey.PresenceStore do
  @moduledoc """
  Presence in Valkey: a hash `presence:sessions:<user_id>` of per-session entries (expiring unless
  a live session refreshes it) and the last broadcast aggregate in `presence:agg:<user_id>`.
  """

  @behaviour DiscordyGateway.Application.Ports.PresenceStore

  alias DiscordyGateway.Infrastructure.Valkey.Pool

  @sessions_ttl_seconds 120
  @aggregate_ttl_seconds 86_400

  @impl true
  def put(user_id, session_id, presence) do
    key = sessions_key(user_id)

    with {:ok, _} <-
           Pool.pipeline([
             ["HSET", key, session_id, Jason.encode!(presence)],
             ["EXPIRE", key, @sessions_ttl_seconds]
           ]) do
      list(user_id)
    end
  end

  @impl true
  def remove(user_id, session_id) do
    with {:ok, _} <- Pool.command(["HDEL", sessions_key(user_id), session_id]), do: list(user_id)
  end

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
    case Pool.command(["MGET" | Enum.map(user_ids, &aggregate_key/1)]) do
      {:ok, values} ->
        for {user_id, value} <- Enum.zip(user_ids, values), is_binary(value), into: %{} do
          {user_id, aggregate(decode(value))}
        end

      {:error, _reason} ->
        %{}
    end
  end

  @impl true
  def refresh(user_id) do
    Pool.command(["EXPIRE", sessions_key(user_id), @sessions_ttl_seconds])
    :ok
  end

  defp list(user_id) do
    with {:ok, values} <- Pool.command(["HVALS", sessions_key(user_id)]) do
      {:ok, Enum.map(values, &session_presence(decode(&1)))}
    end
  end

  defp decode(json), do: Jason.decode!(json)

  defp session_presence(map) do
    %{
      status: map["status"],
      activities: map["activities"] || [],
      afk: map["afk"] == true,
      platform: map["platform"] || "desktop"
    }
  end

  defp aggregate(map) do
    %{
      status: map["status"],
      activities: map["activities"] || [],
      client_status: map["client_status"] || %{}
    }
  end

  defp sessions_key(user_id), do: "presence:sessions:" <> user_id
  defp aggregate_key(user_id), do: "presence:agg:" <> user_id
end
