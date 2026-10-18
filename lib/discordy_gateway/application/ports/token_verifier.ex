defmodule DiscordyGateway.Application.Ports.TokenVerifier do
  @moduledoc "Verifies user access tokens (P3, EdDSA JWT)."

  @type identity :: %{
          user_id: String.t(),
          auth_session_id: String.t() | nil,
          device_id: String.t() | nil
        }

  @callback verify_access_token(String.t()) :: {:ok, identity()} | {:error, term()}
end
