defmodule DiscordyGateway.Application.Settings do
  @moduledoc "Runtime settings the application layer needs, bound by the composition root."

  alias DiscordyGateway.Contracts

  @spec default_voice_region() :: String.t()
  def default_voice_region, do: fetch(:default_voice_region)

  @spec public_gateway_url() :: String.t()
  def public_gateway_url, do: fetch(:public_gateway_url)

  @doc "Interval announced in HELLO."
  @spec heartbeat_interval_ms() :: pos_integer()
  def heartbeat_interval_ms, do: get(:heartbeat_interval_ms, Contracts.heartbeat_interval_ms())

  @doc "How long a detached session stays resumable."
  @spec resume_window_ms() :: pos_integer()
  def resume_window_ms, do: get(:resume_window_ms, Contracts.resume_window_ms())

  @doc "How long a connection may stay unauthenticated."
  @spec identify_timeout_ms() :: pos_integer()
  def identify_timeout_ms, do: get(:identify_timeout_ms, 10_000)

  @doc "How long a guild process lingers without subscribers."
  @spec guild_idle_ms() :: pos_integer()
  def guild_idle_ms, do: get(:guild_idle_ms, 300_000)

  defp fetch(key),
    do: :discordy_gateway |> Application.fetch_env!(:settings) |> Keyword.fetch!(key)

  defp get(key, default),
    do: :discordy_gateway |> Application.get_env(:settings, []) |> Keyword.get(key, default)
end
