defmodule DiscordyGateway.Application.Ports.CallDefaults do
  @moduledoc """
  The `e2ee_calls_default` account setting of each user, as last seen in READY or
  USER_SETTINGS_UPDATE. Voice joins read it for the joiner and every current participant.
  """

  @callback put(user_id :: String.t(), enabled :: boolean()) :: :ok | {:error, term()}
  @callback enabled_user_ids([String.t()]) :: [String.t()]
end
