defmodule DiscordyGateway.Infrastructure.Auth.Hs256VoiceTokenSigner do
  @moduledoc "Signs voice tokens (P3) as HS256 JWTs with `VOICE_TOKEN_SECRET`."

  @behaviour DiscordyGateway.Application.Ports.VoiceTokenSigner

  @header %{"alg" => "HS256", "typ" => "JWT"}

  @impl true
  def sign(claims) do
    secret =
      :discordy_gateway |> Application.fetch_env!(:auth) |> Keyword.fetch!(:voice_token_secret)

    sign(claims, secret)
  end

  @doc "Signs `claims` with an explicit secret."
  @spec sign(map(), binary()) :: String.t()
  def sign(claims, secret) do
    signing_input = encode(@header) <> "." <> encode(claims)
    signature = :crypto.mac(:hmac, :sha256, secret, signing_input)
    signing_input <> "." <> Base.url_encode64(signature, padding: false)
  end

  defp encode(map), do: map |> Jason.encode!() |> Base.url_encode64(padding: false)
end
