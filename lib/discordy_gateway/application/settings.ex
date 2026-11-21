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

  @doc "Bounded availability indication after a mobile app reports background suspension."
  @spec mobile_background_presence_ms() :: pos_integer()
  def mobile_background_presence_ms, do: get(:mobile_background_presence_ms, 86_400_000)

  @doc """
  How long a dropped connection keeps its voice state before the session leaves voice. Shorter than
  the resume window, so a closed or crashed client does not hold a call open for minutes.
  """
  @spec voice_detach_grace_ms() :: non_neg_integer()
  def voice_detach_grace_ms, do: get(:voice_detach_grace_ms, 15_000)

  @doc "How long a connection may stay unauthenticated."
  @spec identify_timeout_ms() :: pos_integer()
  def identify_timeout_ms, do: get(:identify_timeout_ms, 10_000)

  @doc "How long a guild process lingers without subscribers."
  @spec guild_idle_ms() :: pos_integer()
  def guild_idle_ms, do: get(:guild_idle_ms, 300_000)

  @doc "What a voice join does when the participants' `e2ee_calls_default` cannot be read."
  @spec e2ee_settings_unavailable_policy() :: :open | :closed | :wait
  def e2ee_settings_unavailable_policy, do: get(:e2ee_settings_unavailable_policy, :open)

  @doc "Reads attempted before giving up under the `wait` policy."
  @spec e2ee_settings_wait_attempts() :: pos_integer()
  def e2ee_settings_wait_attempts, do: get(:e2ee_settings_wait_attempts, 3)

  @doc "Pause between two reads under the `wait` policy."
  @spec e2ee_settings_wait_interval_ms() :: non_neg_integer()
  def e2ee_settings_wait_interval_ms, do: get(:e2ee_settings_wait_interval_ms, 1_000)

  @doc "Share of the memory limit at which new connections are refused (`4015`)."
  @spec admission_memory_fraction() :: float()
  def admission_memory_fraction, do: get(:admission_memory_fraction, 0.75)

  @doc "How often the memory probe is sampled for admission control."
  @spec admission_interval_ms() :: pos_integer()
  def admission_interval_ms, do: get(:admission_interval_ms, 1_000)

  @doc """
  Frames a socket process may have queued before the session cuts the connection as a slow
  consumer (the client resumes).
  """
  @spec transport_backlog_limit() :: pos_integer()
  def transport_backlog_limit, do: get(:transport_backlog_limit, 2_000)

  defp fetch(key),
    do: :discordy_gateway |> Application.fetch_env!(:settings) |> Keyword.fetch!(key)

  defp get(key, default),
    do: :discordy_gateway |> Application.get_env(:settings, []) |> Keyword.get(key, default)
end
