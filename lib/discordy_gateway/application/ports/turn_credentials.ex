defmodule DiscordyGateway.Application.Ports.TurnCredentials do
  @moduledoc "Mints short-lived TURN credentials for P2P fallback (coturn static-auth-secret)."

  @type credentials :: %{
          urls: [String.t()],
          username: String.t(),
          credential: String.t(),
          ttl: pos_integer()
        }

  @callback issue(user_id :: String.t(), now_s :: integer()) :: credentials()
end
