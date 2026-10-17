defmodule DiscordyGateway.Application.Ports.SessionDirectory do
  @moduledoc "Cluster-wide lookup of live session processes by session, user and device."

  @callback register(
              session_id :: String.t(),
              user_id :: String.t(),
              device_id :: String.t() | nil
            ) ::
              :ok
  @callback whereis(session_id :: String.t()) :: pid() | nil
  @callback sessions_for_user(user_id :: String.t()) :: [pid()]
  @callback sessions_for_device(device_id :: String.t()) :: [pid()]
  @callback count_by_node() :: %{String.t() => non_neg_integer()}
end
