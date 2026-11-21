defmodule DiscordyGateway.Application.Ports.PresenceStore do
  @moduledoc "Per-session presence entries and the last broadcast aggregate per user (`presence:*`)."

  alias DiscordyGateway.Domain.Presence

  @callback put(user_id :: String.t(), session_id :: String.t(), Presence.session_presence()) ::
              {:ok, [Presence.session_presence()]} | {:error, term()}
  @callback remove(user_id :: String.t(), session_id :: String.t()) ::
              {:ok, [Presence.session_presence()]} | {:error, term()}
  @callback swap_aggregate(user_id :: String.t(), Presence.aggregate()) :: :changed | :unchanged
  @callback get_aggregates([String.t()]) :: %{String.t() => Presence.aggregate()}
  @callback refresh(user_id :: String.t(), session_id :: String.t()) ::
              {:ok, [Presence.session_presence()]} | {:error, term()}
end
