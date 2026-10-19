defmodule DiscordyGateway.Infrastructure.Auth.CoturnCredentials do
  @moduledoc """
  Short-lived TURN credentials for coturn's `use-auth-secret` (REST API) mode: the username is
  `<expiry>:<user_id>` and the credential is the base64 HMAC-SHA1 of it under the shared secret.
  """

  @behaviour DiscordyGateway.Application.Ports.TurnCredentials

  @ttl_s 3_600

  @impl true
  def issue(user_id, now_s) do
    turn = Application.fetch_env!(:discordy_gateway, :turn)
    issue(user_id, now_s, Keyword.fetch!(turn, :secret), Keyword.fetch!(turn, :urls))
  end

  @doc "Issues credentials with an explicit secret and url list."
  @spec issue(String.t(), integer(), binary(), [String.t()]) :: map()
  def issue(user_id, now_s, secret, urls) do
    username = Integer.to_string(now_s + @ttl_s) <> ":" <> user_id

    %{
      urls: urls,
      username: username,
      credential: :hmac |> :crypto.mac(:sha, secret, username) |> Base.encode64(),
      ttl: @ttl_s
    }
  end
end
