defmodule DiscordyGateway.Application.Settings do
  @moduledoc "Runtime settings the application layer needs, bound by the composition root."

  @spec default_voice_region() :: String.t()
  def default_voice_region, do: fetch(:default_voice_region)

  @spec public_gateway_url() :: String.t()
  def public_gateway_url, do: fetch(:public_gateway_url)

  defp fetch(key),
    do: :discordy_gateway |> Application.fetch_env!(:settings) |> Keyword.fetch!(key)
end
