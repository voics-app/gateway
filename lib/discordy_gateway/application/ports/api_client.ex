defmodule DiscordyGateway.Application.Ports.ApiClient do
  @moduledoc "Internal HTTP API of the `api` service (P4)."

  @type result :: {:ok, map()} | {:error, :forbidden | :not_found | :unauthorized | term()}

  @callback introspect(token :: String.t()) :: result()
  @callback fetch_ready(user_id :: String.t()) :: result()
  @callback fetch_guild_state(guild_id :: String.t()) :: result()
  @callback fetch_channel_access(channel_id :: String.t(), user_id :: String.t()) :: result()
end
