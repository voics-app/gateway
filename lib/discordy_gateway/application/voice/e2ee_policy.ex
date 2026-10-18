defmodule DiscordyGateway.Application.Voice.E2eePolicy do
  @moduledoc """
  Reads the `e2ee_calls_default` account setting of the users taking part in a voice join, and
  applies `E2EE_SETTINGS_UNAVAILABLE_POLICY` when the store cannot answer:

    * `open` (default): the users count as not opted in, the join proceeds unencrypted and a
      warning is logged
    * `closed`: the users count as opted in, so the call is encrypted
    * `wait`: the read is retried up to `e2ee_settings_wait_attempts` times, pausing
      `e2ee_settings_wait_interval_ms` between reads; if it still fails the join is refused
  """

  require Logger

  alias DiscordyGateway.Application.{Ports, Settings}

  @doc "One flag per user, in the order given: whether that user has the setting on."
  @spec setting_flags([String.t()]) :: {:ok, [boolean()]} | {:error, :settings_unavailable}
  def setting_flags(user_ids) do
    policy = Settings.e2ee_settings_unavailable_policy()
    attempts = if policy == :wait, do: Settings.e2ee_settings_wait_attempts(), else: 1

    case read(user_ids, attempts) do
      {:ok, enabled} ->
        enabled = MapSet.new(enabled)
        {:ok, Enum.map(user_ids, &MapSet.member?(enabled, &1))}

      {:error, reason} ->
        unavailable(policy, user_ids, reason)
    end
  end

  defp read(user_ids, attempts) do
    case Ports.call_defaults().enabled_user_ids(user_ids) do
      {:ok, enabled} ->
        {:ok, enabled}

      {:error, _reason} when attempts > 1 ->
        Process.sleep(Settings.e2ee_settings_wait_interval_ms())
        read(user_ids, attempts - 1)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp unavailable(:wait, _user_ids, reason) do
    Logger.warning("call settings unavailable, voice join refused: #{inspect(reason)}")
    {:error, :settings_unavailable}
  end

  defp unavailable(:closed, user_ids, reason) do
    Logger.warning("call settings unavailable, call encrypted: #{inspect(reason)}")
    {:ok, List.duplicate(true, length(user_ids))}
  end

  defp unavailable(:open, user_ids, reason) do
    Logger.warning("call settings unavailable, call not encrypted: #{inspect(reason)}")
    {:ok, List.duplicate(false, length(user_ids))}
  end
end
