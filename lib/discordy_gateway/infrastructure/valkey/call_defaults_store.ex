defmodule DiscordyGateway.Infrastructure.Valkey.CallDefaultsStore do
  @moduledoc """
  The per-user `e2ee_calls_default` flag in one Valkey hash, `voice:e2ee_default` (field = user id,
  value `1` or `0`). Written by READY and USER_SETTINGS_UPDATE, read by voice joins.
  """

  @behaviour DiscordyGateway.Application.Ports.CallDefaults

  require Logger

  alias DiscordyGateway.Infrastructure.Valkey.Pool

  @key "voice:e2ee_default"

  @impl true
  def put(user_id, enabled) do
    case Pool.command(["HSET", @key, user_id, if(enabled, do: "1", else: "0")]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def enabled_user_ids([]), do: []

  def enabled_user_ids(user_ids) do
    case Pool.command(["HMGET", @key | user_ids]) do
      {:ok, values} ->
        for {user_id, "1"} <- Enum.zip(user_ids, values), do: user_id

      {:error, reason} ->
        Logger.warning(
          "call defaults unavailable, treating participants as not opted in: #{inspect(reason)}"
        )

        []
    end
  end
end
